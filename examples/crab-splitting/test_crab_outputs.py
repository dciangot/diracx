"""Tests for output registration in Rucio, the merge packer and the transfer actions.

Run from this directory, outside the repository's pytest configuration::

    python3 -m pytest -c /dev/null -p no:cacheprovider test_crab_outputs.py
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
)
from crab_outputs import (
    CRABMergeArgs,
    CRABReleaseIntermediatesArgs,
    CRABWaitForTransfersArgs,
    ProducedFile,
    RucioOutputCatalogue,
    crab_merge_packer,
    crab_release_intermediates,
    crab_wait_for_transfers,
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

HERE = Path(__file__).parent
CONTAINER = "/Muon/crabuser-Run2024A-test/USER"


class World:
    def __init__(self, tmp_path: Path, **rses: dict) -> None:
        data = json.loads((HERE / "example_rucio.json").read_text())
        data["rses"].update(rses)
        path = tmp_path / "rucio.json"
        path.write_text(json.dumps(data))
        self.clock = SimClock()
        self.rucio = MockRucio(path, self.clock)
        self.catalogue = RucioOutputCatalogue(self.rucio, account="crabuser")

    def produced(self, n, *, site="T2_IT_Legnaro", intermediate=True, rule=None):
        return ProducedFile(
            workgraph="wg",
            step="merge" if not intermediate else "analysis",
            output="out",
            parcel_id=n,
            site=site,
            basename=f"out_{n}.root",
            bytes=100,
            adler32="0000beef",
            intermediate=intermediate,
            rule=rule,
        )

    def wait(self, **kwargs):
        return crab_wait_for_transfers(
            args=CRABWaitForTransfersArgs(**kwargs),
            rucio=self.rucio,
            catalogue=self.catalogue,
        )


@pytest.fixture
def world(tmp_path):
    return World(tmp_path)


def _rule(rse="T2_DE_DESY"):
    return {"rse_expression": rse, "container": CONTAINER}


# -- registration ------------------------------------------------------------


def test_intermediate_is_registered_at_temp_and_pinned(world):
    [lfn] = world.catalogue.register([world.produced(1)])
    assert lfn == "/store/temp/user/crabuser/wg/analysis/out/out_1.root"
    assert world.rucio.replicas[lfn]["rses"] == ["T2_IT_Legnaro_Temp"]
    assert world.rucio.replicas[lfn]["adler32"] == "0000beef"
    dataset = "/wg/crabuser-analysis-out/UNMERGED#T2_IT_Legnaro_Temp"
    assert world.rucio.collections[dataset]["children"] == [lfn]
    assert world.rucio.collections["/wg/crabuser-analysis-out/UNMERGED"][
        "children"
    ] == [dataset]
    [(rule_id, (kind, name))] = world.catalogue.rules.items()
    assert (kind, name) == ("intermediate", dataset)
    rule = world.rucio.get_replication_rule(rule_id)
    assert rule["rse_expression"] == "T2_IT_Legnaro_Temp"
    assert rule["state"] == "OK"
    assert rule["expires_at"] == world.clock.now() + timedelta(days=7)


def test_registration_is_idempotent(world):
    files = [world.produced(1), world.produced(2)]
    world.catalogue.register(files)
    # The core retries Completing, possibly from a fresh process
    again = RucioOutputCatalogue(world.rucio, account="crabuser")
    again.register(files)
    assert len(world.rucio.rules) == 1
    assert again.rules.keys() == world.catalogue.rules.keys()
    dataset = "/wg/crabuser-analysis-out/UNMERGED#T2_IT_Legnaro_Temp"
    assert len(world.rucio.collections[dataset]["children"]) == 2


def test_output_rule_moves_the_container_to_the_destination(world):
    first = world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule())]
    )
    assert first == ["/store/user/crabuser/wg/merge/out/out_1.root"]
    assert world.wait().result == "Pending"
    world.clock.advance(timedelta(minutes=20))
    # a file registered later, at another site, is covered by the same rule
    second = world.catalogue.register(
        [world.produced(2, site="T1_IT_CNAF", intermediate=False, rule=_rule())]
    )
    assert [k for k, _ in world.catalogue.rules.values()] == ["final"]
    world.clock.advance(timedelta(minutes=15))
    assert world.wait().result == "Pending"  # the second file is still on its way
    world.clock.advance(timedelta(minutes=15))
    result = world.wait()
    assert result.result == "Passed"
    assert result.message == "Output transfer: 1 OK"
    for lfn in first + second:
        assert "T2_DE_DESY" in world.rucio.replicas[lfn]["rses"]


def test_stuck_output_rule_fails_finalizing(world):
    world.catalogue.register(
        [world.produced(1, intermediate=False, rule=_rule("T2_IT_Pisa"))]
    )
    world.clock.advance(timedelta(hours=1))
    assert world.wait().result == "Failed"
    assert world.wait(fail_on_stuck=False).result == "Passed"


def test_release_intermediates_shortens_only_the_pins(world):
    world.catalogue.register(
        [world.produced(1), world.produced(2, intermediate=False, rule=_rule())]
    )
    result = crab_release_intermediates(
        args=CRABReleaseIntermediatesArgs(grace_hours=1),
        rucio=world.rucio,
        catalogue=world.catalogue,
    )
    assert result.result == "Done"
    for rule_id, (kind, _) in world.catalogue.rules.items():
        expires = world.rucio.rules[rule_id]["expires_at"]
        if kind == "intermediate":
            assert expires == world.clock.now() + timedelta(hours=1)
        else:
            assert expires is None


# -- merge packer ------------------------------------------------------------


def _merge(world, inputs, feeder_active=False, **kwargs):
    args = {"min_merge_size": 250, "max_merge_size": 300, **kwargs}
    return list(
        crab_merge_packer(
            packer_args=CRABMergeArgs(**args),
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
        CRABMergeArgs(min_merge_size=10, max_merge_size=5)


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
    analysis, merge = compile_workgraph(HERE / "crab-analysis-merge.cwl")
    assert analysis.feeder == "CRABDBS"
    assert analysis.outputs == {"output-files": (True, None)}
    assert merge.feeder == "Edge"
    assert merge.feeder_args == {"source": "analysis/output-files"}
    assert merge.packer == "CRABMerge"
    intermediate, rule = merge.outputs["merged"]
    assert not intermediate
    assert rule["rse_expression"] == "T2_DE_DESY"


def test_invalid_rucio_rule_is_refused(tmp_path):
    doc = yaml.safe_load((HERE / "crab-analysis-merge.cwl").read_text())
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
    result = _run(tmp_path, capsys, HERE / "crab-analysis-merge.cwl")
    analysis, merge = result["analysis"], result["merge"]
    assert analysis["parcels"] == {"Done": 5}
    assert all(lfn.startswith("/store/temp/user/") for lfn in analysis["outputs"])
    assert merge["inputs"] == {"Processed": 5}
    assert merge["parcels"] == {"Done": 3}
    assert len(merge["outputs"]) == 3
    assert [a["result"] for a in merge["finalizing"]] == ["Passed", "Done"]
    rules = result["rucio_rules"]
    final = rules["/Muon/crabuser-Run2024A-crab_analysis_merge/USER"]
    assert (final["kind"], final["state"]) == ("final", "OK")
    assert sorted(k for k, r in rules.items() if r["kind"] == "intermediate") == [
        "/crab_analysis_merge/crabuser-analysis-output-files/UNMERGED#T1_IT_CNAF_Temp",
        "/crab_analysis_merge/crabuser-analysis-output-files/UNMERGED#T2_IT_Legnaro_Temp",
    ]


@needs_cwltool
def test_end_to_end_transfer_without_merge(tmp_path, capsys):
    doc = yaml.safe_load((HERE / "crab-analysis-merge.cwl").read_text())
    rule = doc["outputs"].pop("merged")["dirac:RucioRule"]
    del doc["steps"]["merge"]
    doc["steps"]["analysis"]["hints"]["dirac:Transformation"]["actions"][
        "Finalizing"
    ].insert(0, {"action": "CRABWaitForTransfers"})
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
    [(name, rule)] = result["rucio_rules"].items()
    assert (rule["kind"], rule["state"]) == ("final", "OK")
