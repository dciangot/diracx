"""Tests for the CRAB feeder/packer plugins and the local core.

Run from this directory, outside the repository's pytest configuration::

    python3 -m pytest -c /dev/null -p no:cacheprovider test_crab_plugins.py
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from adr006 import (
    BadInput,
    ComputeParcel,
    FileInput,
    Input,
    NoMoreInputs,
    PackerInput,
    SetFeederState,
    SplitInto,
    ToProblematic,
    ToUnassigned,
)
from crab_plugins import (
    CRABDBSFeederArgs,
    CRABHandleFailedInputArgs,
    CRABPrivateMCFeederArgs,
    CRABSplitterArgs,
    crab_dbs_feeder,
    crab_handle_failed_input,
    crab_private_mc_feeder,
    crab_splitter_packer,
    expand,
)
from local_core import (
    JSONDBS,
    JSONReplicas,
    LocalCore,
    SubmissionError,
    compile_workgraph,
    main,
)
from pydantic import ValidationError

HERE = Path(__file__).parent
DATASET = "/Muon/Run2024A-PromptReco-v1/MINIAOD"
DBS = JSONDBS(HERE / "example_dbs.json")
REPLICAS = JSONReplicas(HERE / "example_replicas.json")


def feed(scouting: bool = False, **kwargs):
    args = CRABDBSFeederArgs(dataset=DATASET, **kwargs)
    return list(
        crab_dbs_feeder(feeder_args=args, feeder_state={}, scouting=scouting, dbs=DBS)
    )


def as_packer_inputs(items):
    return [
        PackerInput(uuid4(), i.lfn, i.lfn_size, i.descriptor)
        for i in items
        if isinstance(i, FileInput)
    ]


def pack(inputs, feeder_active=False, flush=False, **kwargs):
    return list(
        crab_splitter_packer(
            packer_args=CRABSplitterArgs(**kwargs),
            inputs=iter(inputs),
            flush=flush,
            feeder_active=feeder_active,
            input_overview={},
            replicas=REPLICAS,
        )
    )


# -- feeders ------------------------------------------------------------------


def test_dbs_feeder_applies_mask_and_exhausts():
    items = feed(lumi_mask={"1": [[1, 3]], "2": [[2, 6]], "3": [[1, 3]]})
    files = [i for i in items if isinstance(i, FileInput)]
    assert [f.lfn.rsplit("/", 1)[1] for f in files] == ["f1.root", "f2.root", "f3.root"]
    assert sum(f.descriptor["lumis"] for f in files) == 11
    assert isinstance(items[-2], SetFeederState) and items[-2].state["complete"]
    assert isinstance(items[-1], NoMoreInputs)


def test_dbs_feeder_bookmark_short_circuits():
    args = CRABDBSFeederArgs(dataset=DATASET)
    items = list(
        crab_dbs_feeder(
            feeder_args=args, feeder_state={"complete": True}, scouting=False, dbs=DBS
        )
    )
    assert items == [NoMoreInputs()]


def test_dbs_feeder_chunks_and_run_range():
    files = [
        i
        for i in feed(run_range="2", max_lumis_per_input=2)
        if isinstance(i, FileInput)
    ]
    assert [f.descriptor["lumis"] for f in files] == [2, 1, 2, 2]
    assert {run for f in files for run, _ in expand(f.descriptor["lumi_mask"])} == {2}


def test_dbs_feeder_total_units_fraction():
    files = [i for i in feed(total_units=0.5) if isinstance(i, FileInput)]
    assert len(files) == 2


def test_dbs_feeder_scouting_is_a_stable_subset():
    full = {i.lfn for i in feed() if isinstance(i, FileInput)}
    scout = feed(scouting=True, scouting_fraction=0.5)
    assert not any(isinstance(i, NoMoreInputs) for i in scout)
    sample = {i.lfn for i in scout if isinstance(i, FileInput)}
    assert sample <= full
    assert sample == {
        i.lfn
        for i in feed(scouting=True, scouting_fraction=0.5)
        if isinstance(i, FileInput)
    }


def test_private_mc_feeder_cursor():
    args = CRABPrivateMCFeederArgs(
        total_events=1050, events_per_job=500, events_per_lumi=100, batch=2
    )
    first = list(
        crab_private_mc_feeder(feeder_args=args, feeder_state={}, scouting=False)
    )
    ranges = [i.descriptor for i in first if isinstance(i, Input)]
    assert [(r["first_event"], r["last_event"]) for r in ranges] == [
        (1, 500),
        (501, 1000),
    ]
    assert not any(isinstance(i, NoMoreInputs) for i in first)
    state = next(i.state for i in first if isinstance(i, SetFeederState))
    rest = list(
        crab_private_mc_feeder(feeder_args=args, feeder_state=state, scouting=False)
    )
    last = [i.descriptor for i in rest if isinstance(i, Input)]
    assert last == [
        {
            "first_event": 1001,
            "last_event": 1050,
            "first_lumi": 11,
            "last_lumi": 11,
            "events": 50,
        }
    ]
    assert isinstance(rest[-1], NoMoreInputs)


def test_private_mc_max_jobs_refused_at_submission():
    with pytest.raises(ValidationError, match="would generate 3 jobs"):
        CRABPrivateMCFeederArgs(total_events=1050, events_per_job=500, max_jobs=2)


# -- packer -------------------------------------------------------------------


def test_file_based_keeps_storage_together():
    parcels = pack(as_packer_inputs(feed()), algorithm="FileBased", units_per_job=2)
    assert all(isinstance(p, ComputeParcel) for p in parcels)
    assert [len(p.parameters["input-data"]) for p in parcels] == [2, 1, 1]
    assert parcels[2].requirements == {"sites": ["T1_IT_CNAF", "T2_IT_Pisa"]}


def test_lumi_based_holds_remainder_while_feeder_active():
    inputs = as_packer_inputs(feed(max_lumis_per_input=1))
    held = pack(inputs, feeder_active=True, algorithm="LumiBased", units_per_job=4)
    drained = pack(inputs, feeder_active=False, algorithm="LumiBased", units_per_job=4)
    flushed = pack(
        inputs, feeder_active=True, flush=True, algorithm="LumiBased", units_per_job=4
    )
    assert all(p.metadata["lumis"] == 4 for p in held)
    assert sum(p.metadata["lumis"] for p in drained) == 14
    assert len(flushed) == len(drained)
    mask = json.loads(drained[0].parameters["lumi-mask"])
    assert mask == {"1": [[1, 3]], "2": [[1, 1]]}


def test_event_aware_and_automatic():
    inputs = as_packer_inputs(feed(max_lumis_per_input=1))
    parcels = pack(inputs, algorithm="EventAwareLumiBased", units_per_job=1200)
    assert all(p.metadata["events"] <= 1200 for p in parcels)
    probes = pack(inputs, algorithm="Automatic", minutes_per_job=180)
    assert all(p.metadata["files"] == 1 for p in probes)
    sized = pack(
        inputs, algorithm="Automatic", minutes_per_job=180, events_per_second=0.1
    )
    assert all(p.metadata["events"] <= 1080 for p in sized)
    with pytest.raises(ValidationError, match="Minimum runtime"):
        CRABSplitterArgs(algorithm="Automatic", minutes_per_job=10)


def test_bad_inputs():
    no_replica = PackerInput(
        uuid4(), "/store/unknown.root", 1, {"lumi_mask": {"1": [[1, 1]]}, "lumis": 1}
    )
    seed = PackerInput(uuid4(), None, None, {"seed": 1})
    out = pack([no_replica, seed], algorithm="LumiBased", units_per_job=1)
    assert [type(o) for o in out] == [BadInput, BadInput]


# -- HandleFailedInput ----------------------------------------------------------


def test_handle_failed_input_retry_split_give_up():
    args = CRABHandleFailedInputArgs(max_retries=1)
    desc = {"lumi_mask": {"1": [[1, 4]]}, "lumis": 4, "events": 400}
    retry = crab_handle_failed_input(
        input=PackerInput(uuid4(), "/f", 1, desc, 1), parcel=None, args=args
    )
    assert isinstance(retry, ToUnassigned)
    split = crab_handle_failed_input(
        input=PackerInput(uuid4(), "/f", 1, desc, 2), parcel=None, args=args
    )
    assert isinstance(split, SplitInto)
    assert [c.descriptor["lumi_mask"] for c in split.children] == [
        {"1": [[1, 2]]},
        {"1": [[3, 4]]},
    ]
    single = {"lumi_mask": {"1": [[1, 1]]}, "lumis": 1, "events": 100}
    give_up = crab_handle_failed_input(
        input=PackerInput(uuid4(), "/f", 1, single, 2), parcel=None, args=args
    )
    assert isinstance(give_up, ToProblematic)


# -- local core ---------------------------------------------------------------


def test_compile_and_dedup():
    (spec,) = compile_workgraph(HERE / "crab-analysis.cwl")
    assert (spec.feeder, spec.packer, spec.status_report) == (
        "CRABDBS",
        "CRABSplitter",
        "status-report",
    )
    core = LocalCore(spec, _services())
    first = core.sweep_feeder()
    core.feeder_enabled = True
    core.feeder_state = {}
    assert first > 0 and core.sweep_feeder() == 0


def test_status_report_split_and_failure():
    (spec,) = compile_workgraph(HERE / "crab-analysis.cwl")
    core = LocalCore(spec, _services())
    core.sweep_feeder()
    parcel = core.sweep_packer()[0]
    a = parcel.inputs[0]
    lfn = core.rows[a].lfn
    core.apply_report(
        parcel,
        {
            str(a): {
                "action": "SPLIT",
                "SUCCESS": [
                    {
                        "lfn": f"lfn:{lfn}",
                        "lfn_size": 1,
                        "data": {"lumi_mask": {"1": [[1, 1]]}, "lumis": 1},
                    }
                ],
                "FAILURE": [
                    {
                        "lfn": f"lfn:{lfn}",
                        "lfn_size": 1,
                        "data": {"lumi_mask": {"1": [[2, 3]]}, "lumis": 2},
                    }
                ],
            }
        },
    )
    assert parcel.status == "PartiallyDone"
    children = [r for r in core.rows.values() if r.parent == a]
    assert sorted(r.status for r in children) == ["Processed", "Unassigned"]
    other = core.sweep_packer()[0]
    core.apply_report(other, {})  # no report: every input failed
    assert all(core.rows[i].status == "Unassigned" for i in other.inputs)
    assert all(core.rows[i].error_count == 1 for i in other.inputs)


def test_submission_refused(tmp_path):
    doc = (
        (HERE / "crab-analysis.cwl")
        .read_text()
        .replace("units_per_job: 3", "units_per_job: -1")
    )
    bad = tmp_path / "bad.cwl"
    bad.write_text(doc)
    (tmp_path / "transformations").symlink_to(HERE / "transformations")
    (spec,) = compile_workgraph(bad)
    with pytest.raises(SubmissionError):
        LocalCore(spec, _services())


@pytest.mark.skipif(shutil.which("cwltool") is None, reason="cwltool not installed")
def test_end_to_end_with_cwltool(tmp_path, capsys):
    rc = main(
        [
            str(HERE / "crab-analysis.cwl"),
            "--dbs",
            str(HERE / "example_dbs.json"),
            "--replicas",
            str(HERE / "example_replicas.json"),
            "--outdir",
            str(tmp_path),
            "--run",
            "--set",
            "max-lumis=2",
        ]
    )
    assert rc == 0
    result = json.loads(capsys.readouterr().out)["analysis"]
    assert result["feeder_enabled"] is False
    assert set(result["inputs"]) <= {"Processed", "Split"}


def _services():
    from crab_plugins import DBSClient, ReplicaCatalogue

    return {DBSClient: DBS, ReplicaCatalogue: REPLICAS}
