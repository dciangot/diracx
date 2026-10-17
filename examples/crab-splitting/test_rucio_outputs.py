"""Tests for output registration in Rucio, the merge packer, the rules and the actions.

Run from this directory, outside the repository's pytest configuration::

    python3 -m pytest -c /dev/null -p no:cacheprovider test_rucio_outputs.py
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
import yaml
from adr006 import (
    BadInput,
    ComputeParcel,
    FileInput,
    NoMoreInputs,
    PackerInput,
    SetFeederState,
    Transformation,
)
from local_core import (
    EdgeFeederArgs,
    MockRucio,
    ParcelOutputs,
    SimClock,
    SubmissionError,
    compile_workgraph,
    edge_feeder,
    main,
)
from pydantic import ValidationError
from rucio_common import InMemoryRucioTables
from rucio_hooks import RucioRuleKeeperArgs, rucio_rule_keeper_hook
from rucio_outputs import (
    ProducedFile,
    RemoveRucioRulesArgs,
    RucioMergeBySizeArgs,
    RucioOutputToDestination,
    WaitForRucioTransfersArgs,
    remove_rucio_rules,
    rucio_merge_by_size_packer,
    wait_for_rucio_transfers,
)

HERE = Path(__file__).parent
CONTAINER = "/Muon/cmsuser-Run2024A-test/USER"
T = Transformation(2, "wg-merge", "wg")


class World:
    def __init__(self, tmp_path: Path, **rses: dict) -> None:
        data = json.loads((HERE / "example_rucio.json").read_text())
        data["rses"].update(rses)
        path = tmp_path / "rucio.json"
        path.write_text(json.dumps(data))
        self.clock = SimClock()
        self.rucio = MockRucio(path, self.clock)
        self.tables = InMemoryRucioTables()
        self.catalogue = self.registrar()

    def registrar(self) -> RucioOutputToDestination:
        return RucioOutputToDestination(
            self.rucio,
            self.tables,
            self.clock,
            scope="user.cmsuser",
            lfn_base="/store/user/cmsuser",
            temp_lfn_base="/store/temp/user/cmsuser",
        )

    def produced(self, n, *, site="T2_IT_Legnaro", intermediate=True, rule=None):
        return ProducedFile(
            transformation_id=T.id if not intermediate else 1,
            workgraph="wg",
            step="merge" if not intermediate else "analysis",
            output="out",
            parcel_id=n,
            location=site,
            basename=f"out_{n}.root",
            bytes=100,
            adler32="0000beef",
            intermediate=intermediate,
            hints={"RucioRule": rule} if rule else {},
        )

    def wait(self, **kwargs):
        return wait_for_rucio_transfers(
            args=WaitForRucioTransfersArgs(**kwargs),
            rucio=self.rucio,
            tables=self.tables,
            clock=self.clock,
            transformation=T,
        )

    def keeper(self, overview, feeder_enabled=False):
        class NoLive:
            def live(self):
                return []

        return rucio_rule_keeper_hook(
            transformation=Transformation(
                T.id, T.name, T.workgraph, feeder_enabled=feeder_enabled
            ),
            input_overview=overview,
            args=RucioRuleKeeperArgs(),
            rucio=self.rucio,
            tables=self.tables,
            live=NoLive(),
            clock=self.clock,
        )


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def _rule(rse="T2_DE_DESY", **kwargs):
    return {"rse_expression": rse, "container": CONTAINER, **kwargs}


# -- registration ------------------------------------------------------------


def test_intermediate_is_registered_at_temp_without_a_rule(world):
    [lfn] = world.catalogue.register([world.produced(1)])
    assert lfn == "/store/temp/user/cmsuser/wg/analysis/out/out_1.root"
    assert world.rucio.replicas[lfn]["rses"] == ["T2_IT_Legnaro_Temp"]
    assert world.rucio.replicas[lfn]["adler32"] == "0000beef"
    dataset = "/wg/analysis-out/UNMERGED#T2_IT_Legnaro_Temp"
    assert world.rucio.collections[dataset]["children"] == [lfn]
    assert world.rucio.collections["/wg/analysis-out/UNMERGED"]["children"] == [dataset]
    assert world.rucio.rules == {}
    assert world.tables.all_rules() == []


def test_registration_is_idempotent(world):
    files = [world.produced(1), world.produced(2, intermediate=False, rule=_rule())]
    world.catalogue.register(files)
    # The core retries Completing, possibly from a fresh process
    world.registrar().register(files)
    assert len(world.rucio.rules) == 1
    assert len(world.tables.rules(T.id)) == 1
    dataset = "/wg/analysis-out/UNMERGED#T2_IT_Legnaro_Temp"
    assert len(world.rucio.collections[dataset]["children"]) == 1


def test_output_rule_moves_the_container_to_the_destination(world):
    first = world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule())]
    )
    assert first == ["/store/user/cmsuser/wg/merge/out/out_1.root"]
    assert world.wait().result == "Pending"
    world.clock.advance(timedelta(minutes=20))
    # a file registered later, at another site, is covered by the same rule
    second = world.catalogue.register(
        [world.produced(2, site="T1_IT_CNAF", intermediate=False, rule=_rule())]
    )
    [rule] = world.tables.rules(T.id)
    assert (rule.kind, rule.name) == ("Output", CONTAINER)
    world.clock.advance(timedelta(minutes=15))
    assert world.wait().result == "Pending"  # the second file is still on its way
    world.clock.advance(timedelta(minutes=15))
    result = world.wait()
    assert result.result == "Passed"
    assert result.message == "Output transfer: 1 OK"
    for lfn in first + second:
        assert "T2_DE_DESY" in world.rucio.replicas[lfn]["rses"]


def test_output_rule_waits_for_min_completion(world):
    world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule(min_completion=0.5))]
    )
    [row] = world.tables.outputs(T.id)
    assert row.rule_id is None and world.rucio.rules == {}
    world.keeper({"Processed": 3, "Assigned": 1}, feeder_enabled=True)
    assert world.rucio.rules == {}  # not every input is known yet
    world.keeper({"Processed": 1, "Assigned": 3})
    assert world.rucio.rules == {}  # 25% < 50%
    world.keeper({"Processed": 2, "Assigned": 2})
    [row] = world.tables.outputs(T.id)
    assert row.rule_id in world.rucio.rules


def test_wait_creates_a_rule_still_pending(world):
    world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule(min_completion=1))]
    )
    assert world.wait().result == "Pending"
    [row] = world.tables.outputs(T.id)
    assert row.rule_id in world.rucio.rules


def test_stuck_output_rule_fails_finalizing(world):
    world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule("T2_IT_Pisa"))]
    )
    world.clock.advance(timedelta(hours=1))
    assert world.wait().result == "Failed"
    assert world.wait(fail_on_stuck=False).result == "Passed"


def test_remove_rules_deletes_every_rule(world):
    world.catalogue.register([world.produced(1, intermediate=False, rule=_rule())])
    result = remove_rucio_rules(
        args=RemoveRucioRulesArgs(),
        rucio=world.rucio,
        tables=world.tables,
        transformation=T,
    )
    assert (result.result, result.message) == ("Done", "1 rules deleted")
    assert world.rucio.rules == {}
    assert world.tables.rules(T.id) == []


# -- merge packer ------------------------------------------------------------


def _merge(world, inputs, feeder_active=False, **kwargs):
    args = {"min_merge_size": 250, "max_merge_size": 300, **kwargs}
    return list(
        rucio_merge_by_size_packer(
            packer_args=RucioMergeBySizeArgs(**args),
            inputs=iter(inputs),
            feeder_active=feeder_active,
            input_overview={},
            rucio=world.rucio,
        )
    )


def _unmerged(world, n, site):
    [lfn] = world.catalogue.register([world.produced(n, site=site)])
    return PackerInput(uuid4(), lfn, 100, {})


def test_merge_groups_by_rse_up_to_size(world):
    legnaro = [_unmerged(world, n, "T2_IT_Legnaro") for n in range(4)]
    cnaf = [_unmerged(world, 9, "T1_IT_CNAF")]
    out = _merge(world, [*legnaro, *cnaf], feeder_active=True)
    # 3 x 100 bytes reach min_merge_size; the rest waits for the feeder
    [parcel] = out
    assert isinstance(parcel, ComputeParcel)
    assert parcel.inputs == [i.id for i in legnaro[:3]]
    assert parcel.requirements == {"sites": ["T2_IT_Legnaro"]}
    assert parcel.metadata["rse"] == "T2_IT_Legnaro_Temp"
    assert parcel.parameters["input-data"] == [f"lfn:{i.lfn}" for i in legnaro[:3]]

    rest = _merge(world, [legnaro[3], *cnaf])
    assert sorted(tuple(p.requirements["sites"]) for p in rest) == [
        ("T1_IT_CNAF",),
        ("T2_IT_Legnaro",),
    ]


def test_merge_never_exceeds_max_size_or_files(world):
    inputs = [_unmerged(world, n, "T2_IT_Legnaro") for n in range(4)]
    out = _merge(world, inputs, min_merge_size=1000, max_merge_size=1000)
    assert [len(p.inputs) for p in out] == [4]
    out = _merge(
        world, inputs, min_merge_size=1000, max_merge_size=1000, max_merge_files=2
    )
    assert [len(p.inputs) for p in out] == [2, 2]
    big = PackerInput(uuid4(), inputs[0].lfn, 5000, {})
    out = _merge(world, [big, inputs[1]], min_merge_size=200, max_merge_size=300)
    assert [len(p.inputs) for p in out] == [1, 1]


def test_merge_input_without_replica_is_bad(world):
    [out] = _merge(world, [PackerInput(uuid4(), "/store/temp/nowhere.root", 1, {})])
    assert isinstance(out, BadInput)


def test_merge_args_are_checked():
    with pytest.raises(ValidationError):
        RucioMergeBySizeArgs(min_merge_size=10, max_merge_size=5)


# -- core side: edge feeder and workgraph compilation --------------------------


def test_edge_feeder_bookmarks_and_waits_for_the_producer():
    table = ParcelOutputs()
    table.add("analysis", "output-files", 1, "/a.root", 10)
    table.add("analysis", "log", 1, "/a.log", 1)

    def feed(state):
        return list(
            edge_feeder(
                feeder_args=EdgeFeederArgs(source="analysis/output-files"),
                feeder_state=state,
                outputs=table,
            )
        )

    first = feed({})
    assert first == [
        FileInput(descriptor={"producer_parcel": 1}, lfn="/a.root", lfn_size=10),
        SetFeederState({"last_output_id": 1}),
    ]
    table.add("analysis", "output-files", 2, "/b.root", 20)
    table.drained["analysis"] = True
    second = feed({"last_output_id": 1})
    assert [type(i) for i in second] == [FileInput, SetFeederState, NoMoreInputs]
    assert second[0].lfn == "/b.root"


def test_compile_merge_workgraph():
    analysis, merge = compile_workgraph(HERE / "rucio-merge-transfer.cwl")
    assert analysis.feeder == "CRABDBS"
    assert analysis.outputs == {"output-files": (True, {})}
    assert analysis.input_rule is not None
    assert merge.feeder == "Edge"
    assert merge.feeder_args == {"source": "analysis/output-files"}
    assert merge.packer == "RucioMergeBySize"
    assert merge.input_rule is None
    intermediate, hints = merge.outputs["merged"]
    assert not intermediate
    assert hints["RucioRule"]["rse_expression"] == "T2_DE_DESY"


def test_invalid_rucio_rule_is_refused(tmp_path):
    doc = yaml.safe_load((HERE / "rucio-merge-transfer.cwl").read_text())
    doc["outputs"]["merged"]["dirac:RucioRule"] = {"rse": "T2_DE_DESY"}
    path = tmp_path / "bad.cwl"
    path.write_text(yaml.safe_dump(doc))
    shutil.copytree(HERE / "transformations", tmp_path / "transformations")
    with pytest.raises(SubmissionError, match="RucioRule"):
        compile_workgraph(path)


# -- end to end ----------------------------------------------------------------

needs_cwltool = pytest.mark.skipif(
    shutil.which("cwltool") is None, reason="cwltool is not installed"
)


def _run(tmp_path, capsys, workgraph):
    rc = main(
        [
            str(workgraph),
            "--dbs",
            str(HERE / "example_dbs.json"),
            "--rucio",
            str(HERE / "example_rucio.json"),
            "--outdir",
            str(tmp_path / "out"),
            "--run",
        ]
    )
    assert rc == 0
    return json.loads(capsys.readouterr().out)


@needs_cwltool
def test_end_to_end_merge_and_transfer(tmp_path, capsys):
    result = _run(tmp_path, capsys, HERE / "rucio-merge-transfer.cwl")
    analysis, merge = result["analysis"], result["merge"]
    assert analysis["parcels"] == {"Done": 5}
    assert all(lfn.startswith("/store/temp/user/") for lfn in analysis["outputs"])
    assert [a["result"] for a in analysis["finalizing"]] == ["Skipped", "Done"]
    assert merge["inputs"] == {"Processed": 5}
    assert set(merge["parcels"]) == {"Done"}
    assert all(lfn.startswith("/store/user/") for lfn in merge["outputs"])
    assert [a["result"] for a in merge["finalizing"]] == ["Passed"]
    rules = result["rucio_rules"]
    [output] = [r for r in rules if r["kind"] == "Output"]
    assert output["name"] == "/Muon/cmsuser-Run2024A-rucio_merge_transfer/USER"
    assert (output["rse_expression"], output["state"]) == ("T2_DE_DESY", "OK")
    # no rule on intermediates; every input rule released at Finalizing
    inputs = [r for r in rules if r["kind"] == "Input"]
    assert len(inputs) == len(rules) - 1 == 3
    assert all(r["released"] for r in inputs)


@needs_cwltool
def test_end_to_end_transfer_without_merge(tmp_path, capsys):
    doc = yaml.safe_load((HERE / "rucio-merge-transfer.cwl").read_text())
    rule = doc["outputs"].pop("merged")["dirac:RucioRule"]
    del rule["min_completion"]
    del doc["steps"]["merge"]
    doc["steps"]["analysis"]["hints"]["dirac:Transformation"]["actions"][
        "Finalizing"
    ].insert(0, {"action": "WaitForRucioTransfers"})
    doc["outputs"]["output-files"] = {
        "type": "File[]",
        "outputSource": "analysis/output-files",
        "dirac:RucioRule": rule,
    }
    path = tmp_path / "no-merge.cwl"
    path.write_text(yaml.safe_dump(doc))
    shutil.copytree(HERE / "transformations", tmp_path / "transformations")
    result = _run(tmp_path, capsys, path)
    analysis = result["analysis"]
    assert all(lfn.startswith("/store/user/") for lfn in analysis["outputs"])
    assert analysis["finalizing"][0]["result"] == "Passed"
    [output] = [r for r in result["rucio_rules"] if r["kind"] == "Output"]
    assert output["state"] == "OK"
