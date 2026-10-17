"""Tests for the Rucio input rules of DX-ADR-010.

Run from this directory, outside the repository's pytest configuration::

    python3 -m pytest -c /dev/null -p no:cacheprovider test_rucio_inputs.py
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from adr006 import BadInput, ComputeParcel, DelayInput, PackerInput, Transformation
from crab_plugins import CRABSplitterArgs, crab_splitter_packer
from local_core import (
    LocalCore,
    MockRucio,
    SimClock,
    SubmissionError,
    compile_workgraph,
    main,
)
from pydantic import ValidationError
from rucio_common import InMemoryRucioTables, LiveInput, site_of
from rucio_hooks import RucioRuleKeeperArgs, rucio_rule_keeper_hook
from rucio_inputs import (
    ReleaseRucioInputRulesArgs,
    RucioInputRule,
    release_rucio_input_rules,
    rucio_input_rules_packer,
)

HERE = Path(__file__).parent
STORE = "/store/data/Run2024A/Muon/MINIAOD/PromptReco-v1/000/"
BLOCK = "/Muon/Run2024A-PromptReco-v1/MINIAOD#block"
T = Transformation(7, "wg-analysis", "wg")


def inner(*, inputs, flush, feeder_active, input_overview, replicas):
    """The step's own packer, as the core binds it."""
    return crab_splitter_packer(
        packer_args=CRABSplitterArgs(algorithm="LumiBased", units_per_job=3),
        inputs=inputs,
        flush=flush,
        feeder_active=feeder_active,
        input_overview=input_overview,
        replicas=replicas,
    )


class World:
    def __init__(self, tmp_path: Path, **rses: dict) -> None:
        data = json.loads((HERE / "example_rucio.json").read_text())
        data["rses"].update(rses)
        data["replicas"][STORE + "orphan.root"] = {"bytes": 1, "rses": ["T2_DE_DESY"]}
        path = tmp_path / "rucio.json"
        path.write_text(json.dumps(data))
        self.clock = SimClock()
        self.rucio = MockRucio(path, self.clock)
        self.tables = InMemoryRucioTables()

    def pack(self, inputs, feeder_active=False, transformation=T, **kwargs):
        hint = {
            "destinations": ["T2_IT_Legnaro", "T2_IT_Pisa", "T1_IT_CNAF_Disk"],
            "clone_scope": "user.test",
            **kwargs,
        }
        return list(
            rucio_input_rules_packer(
                packer_args=RucioInputRule(**hint),
                inputs=iter(inputs),
                feeder_active=feeder_active,
                input_overview={},
                rucio=self.rucio,
                tables=self.tables,
                clock=self.clock,
                transformation=transformation,
                inner=inner,
            )
        )

    def dataset(self, block: str, t: Transformation = T):
        return self.tables.dataset(t.id, "cms", BLOCK + block)


def inp(name: str, lumis: dict[str, list[list[int]]], n: int = 3) -> PackerInput:
    return PackerInput(
        uuid4(),
        STORE + name,
        1000,
        {"lumi_mask": lumis, "lumis": n, "events": 100 * n},
    )


def test_site_of():
    assert site_of("T1_IT_CNAF_Disk") == "T1_IT_CNAF"
    assert site_of("T2_IT_Pisa") == "T2_IT_Pisa"


def test_block_in_place_is_pinned_on_a_clone_and_packed_at_once(tmp_path):
    world = World(tmp_path)
    out = world.pack([inp("f1.root", {"1": [[1, 3]]})])
    assert [type(o) for o in out] == [ComputeParcel]
    assert out[0].requirements == {"sites": ["T2_IT_Legnaro"]}
    assert out[0].metadata["input_rses"] == {STORE + "f1.root": "T2_IT_Legnaro"}
    (rule,) = world.rucio.rules.values()
    clone = BLOCK + "A__dx-wg-analysis"
    assert (rule["scope"], rule["name"]) == ("user.test", clone)
    assert (rule["rse_expression"], rule["state"]) == ("T2_IT_Legnaro", "OK")
    assert rule["comment"] == "DiracX transformation 7"
    # the clone holds the files the transformation was fed, nothing more
    assert world.rucio.collections[clone]["children"] == [STORE + "f1.root"]
    row = world.dataset("A")
    assert (row.rse, row.files, row.bytes) == ("T2_IT_Legnaro", 2, 1_800_000)
    [ruled] = world.tables.rules(T.id)
    assert (ruled.kind, ruled.name) == ("InputDataset", clone)


def test_files_are_monitored_one_by_one(tmp_path):
    world = World(tmp_path)
    f3 = inp("f3.root", {"3": [[1, 3]]})  # tape only: recall + transfer
    f4 = inp("f4.root", {"4": [[1, 1]]}, n=1)  # at DESY: transfer only
    (delay,) = world.pack([f3, f4], destinations=["T1_IT_CNAF_Disk"])
    assert isinstance(delay, DelayInput)
    assert delay.input_ids == [f3.id, f4.id]
    assert delay.delay_until == world.clock.now() + timedelta(minutes=15)
    world.clock.advance(timedelta(minutes=11))
    out = world.pack([f3, f4], destinations=["T1_IT_CNAF_Disk"])
    # f4 is there but held back, since f3 is still on its way to the same site
    assert out == [DelayInput([f3.id], out[0].delay_until)]
    world.clock.advance(timedelta(minutes=120))
    parcels = world.pack([f3, f4], destinations=["T1_IT_CNAF_Disk"])
    assert all(isinstance(p, ComputeParcel) for p in parcels)
    assert {i for p in parcels for i in p.inputs} == {f3.id, f4.id}
    assert all(p.requirements == {"sites": ["T1_IT_CNAF"]} for p in parcels)
    assert len(world.rucio.rules) == 1  # one rule for the block, all along


def test_thresholds_move_a_block_to_the_next_destination(tmp_path):
    world = World(tmp_path)
    world.pack([inp("f1.root", {"1": [[1, 3]]})])
    world.pack([inp("f4.root", {"4": [[1, 1]]}, n=1)], max_bytes_per_rse=2_000_000)
    assert world.dataset("B").rse == "T2_IT_Pisa"

    world2 = World(tmp_path)
    world2.pack([inp("f1.root", {"1": [[1, 3]]})])
    world2.pack([inp("f4.root", {"4": [[1, 1]]}, n=1)], max_files_per_rse=3)
    assert world2.dataset("B").rse == "T2_IT_Pisa"


def test_stuck_rule_is_rerouted_then_given_up(tmp_path):
    world = World(tmp_path)
    f3 = inp("f3.root", {"3": [[1, 3]]})  # tape only; Pisa is stuck
    order = ["T2_IT_Pisa", "T2_IT_Legnaro"]
    world.pack([f3], destinations=order)
    assert world.dataset("B").rse == "T2_IT_Pisa"
    world.clock.advance(timedelta(hours=3))  # recall + transfer, then STUCK
    out = world.pack([f3], destinations=order)
    assert isinstance(out[0], DelayInput)
    row = world.dataset("B")
    assert (row.rse, row.reroutes, row.tried) == (
        "T2_IT_Legnaro",
        1,
        ("T2_IT_Pisa", "T2_IT_Legnaro"),
    )
    old, new = sorted(world.tables.rules(T.id), key=lambda r: r.created_at)
    assert old.rse_expression == "T2_IT_Pisa" and old.released_at is not None
    assert new.rse_expression == "T2_IT_Legnaro" and new.released_at is None
    assert old.name == new.name  # a second rule on the same clone
    expires = world.rucio.get_replication_rule(old.rule_id)["expires_at"]
    assert expires == world.clock.now() + timedelta(hours=24)

    world2 = World(tmp_path)
    world2.pack([f3], destinations=["T2_IT_Pisa"])
    world2.clock.advance(timedelta(hours=3))
    (bad,) = world2.pack([f3], destinations=["T2_IT_Pisa"], max_reroutes=0)
    assert isinstance(bad, BadInput)
    assert "STUCK" in bad.reason


def test_slow_rule_times_out(tmp_path):
    world = World(tmp_path)
    f3 = inp("f3.root", {"3": [[1, 3]]})
    world.pack([f3], destinations=["T1_IT_CNAF_Disk"])
    world.clock.advance(timedelta(hours=1))
    (bad,) = world.pack(
        [f3], destinations=["T1_IT_CNAF_Disk"], max_wait_hours=0.5, max_reroutes=0
    )
    assert isinstance(bad, BadInput)
    assert "REPLICATING" in bad.reason


def test_rule_and_clone_are_found_again_when_the_tables_are_lost(tmp_path):
    world = World(tmp_path)
    f4 = inp("f4.root", {"4": [[1, 1]]}, n=1)
    world.pack([f4])
    world.tables = InMemoryRucioTables()  # e.g. the packer task died after add_rule
    world.pack([f4])
    assert len(world.rucio.rules) == 1
    assert len(world.tables.rules(T.id)) == 1


def test_two_transformations_never_share_a_rule(tmp_path):
    world = World(tmp_path)
    other = Transformation(8, "wg2-analysis", "wg2")
    world.pack([inp("f1.root", {"1": [[1, 3]]})])
    world.pack([inp("f1.root", {"1": [[1, 3]]})], transformation=other)
    names = sorted(r["name"] for r in world.rucio.rules.values())
    assert names == [BLOCK + "A__dx-wg-analysis", BLOCK + "A__dx-wg2-analysis"]


def test_file_in_no_dataset_is_bad(tmp_path):
    world = World(tmp_path)
    (bad,) = world.pack([inp("orphan.root", {"1": [[1, 1]]})])
    assert isinstance(bad, BadInput)
    assert "no Rucio dataset" in bad.reason


def test_hint_is_checked():
    with pytest.raises(ValidationError):
        RucioInputRule(destinations=[])
    with pytest.raises(ValidationError, match="distinct"):
        RucioInputRule(destinations=["T2_IT_Pisa", "T2_IT_Pisa"])


def test_release_shortens_lifetimes(tmp_path):
    world = World(tmp_path)
    world.pack([inp("f1.root", {"1": [[1, 3]]}), inp("f4.root", {"4": [[1, 1]]}, 1)])
    result = release_rucio_input_rules(
        args=ReleaseRucioInputRulesArgs(grace_hours=1),
        rucio=world.rucio,
        tables=world.tables,
        clock=world.clock,
        transformation=T,
    )
    assert result.result == "Done"
    assert len(world.rucio.rules) == 2
    for rule in world.rucio.rules.values():
        assert rule["expires_at"] == world.clock.now() + timedelta(hours=1)
    assert all(r.released_at for r in world.tables.rules(T.id))


class Live:
    def __init__(self, inputs: list[LiveInput]) -> None:
        self.inputs = inputs

    def live(self) -> list[LiveInput]:
        return self.inputs


def test_keeper_renews_only_rules_still_needed(tmp_path):
    world = World(tmp_path)
    world.pack([inp("f1.root", {"1": [[1, 3]]}), inp("f4.root", {"4": [[1, 1]]}, 1)])
    world.clock.advance(timedelta(days=12))  # two days left on 14-day rules
    live = Live([LiveInput(STORE + "f1.root", "Assigned", ("T2_IT_Legnaro",))])
    rucio_rule_keeper_hook(
        transformation=T,
        input_overview={"Assigned": 1, "Processed": 1},
        args=RucioRuleKeeperArgs(),
        rucio=world.rucio,
        tables=world.tables,
        live=live,
        clock=world.clock,
    )
    a, b = world.dataset("A"), world.dataset("B")
    assert world.rucio.get_replication_rule(a.rule_id)["expires_at"] == (
        world.clock.now() + timedelta(days=14)
    )
    assert world.rucio.get_replication_rule(b.rule_id)["expires_at"] == (
        world.clock.now() + timedelta(days=2)
    )


def test_compile_wraps_the_packer(tmp_path):
    (spec,) = compile_workgraph(HERE / "rucio-input-rules.cwl")
    assert spec.packer == "CRABSplitter"
    assert spec.input_rule["destinations"][0] == "T2_IT_Legnaro"
    core = LocalCore(spec, {})
    assert core.packer_fn is rucio_input_rules_packer
    assert isinstance(core.packer_args, RucioInputRule)

    doc = yaml.safe_load((HERE / "rucio-input-rules.cwl").read_text())
    doc["inputs"]["input-data"]["dirac:RucioInputRule"] = {"rse": "T2_IT_Pisa"}
    path = tmp_path / "bad.cwl"
    path.write_text(yaml.safe_dump(doc))
    shutil.copytree(HERE / "transformations", tmp_path / "transformations")
    with pytest.raises(SubmissionError, match="RucioInputRule"):
        compile_workgraph(path)


def test_end_to_end_waits_reroutes_and_renews(tmp_path, capsys):
    rc = main(
        [
            str(HERE / "rucio-input-rules.cwl"),
            "--dbs",
            str(HERE / "example_dbs.json"),
            "--rucio",
            str(HERE / "example_rucio.json"),
            "--outdir",
            str(tmp_path),
        ]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    result = out["analysis"]
    assert result["inputs"] == {"Assigned": 5}
    assert result["summary"]["total_lumis"] == 12
    assert result["input_datasets"][BLOCK + "A"]["rse"] == "T2_IT_Legnaro"
    assert result["input_datasets"][BLOCK + "B"] == {
        "clone": BLOCK + "B__dx-rucio_input_rules-analysis",
        "rse": "T1_IT_CNAF_Disk",
        "reroutes": 1,
    }
    kinds = [(r["rse_expression"], r["released"]) for r in out["rucio_rules"]]
    assert sorted(kinds) == [
        ("T1_IT_CNAF_Disk", False),
        ("T2_IT_Legnaro", False),
        ("T2_IT_Pisa", True),
    ]
    assert result["cycles"] > 1
