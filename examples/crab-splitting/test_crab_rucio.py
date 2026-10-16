"""Tests for the Rucio input-lock packer and action.

Run from this directory, outside the repository's pytest configuration::

    python3 -m pytest -c /dev/null -p no:cacheprovider test_crab_rucio.py
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from adr006 import BadInput, ComputeParcel, DelayInput, PackerInput
from crab_rucio import (
    CRABReleaseInputLocksArgs,
    CRABRucioSplitterArgs,
    crab_release_input_locks,
    crab_rucio_splitter_packer,
    site_of,
)
from local_core import InMemoryLockTable, MockRucio, SimClock, main
from pydantic import ValidationError

HERE = Path(__file__).parent
STORE = "/store/data/Run2024A/Muon/MINIAOD/PromptReco-v1/000/"


class World:
    def __init__(self, tmp_path: Path, **rses: dict) -> None:
        data = json.loads((HERE / "example_rucio.json").read_text())
        data["rses"].update(rses)
        path = tmp_path / "rucio.json"
        path.write_text(json.dumps(data))
        self.clock = SimClock()
        self.rucio = MockRucio(path, self.clock)
        self.locks = InMemoryLockTable()

    def pack(self, inputs, feeder_active=False, **kwargs):
        args = {
            "algorithm": "LumiBased",
            "units_per_job": 3,
            "destinations": ["T2_IT_Legnaro", "T2_IT_Pisa", "T1_IT_CNAF_Disk"],
            **kwargs,
        }
        return list(
            crab_rucio_splitter_packer(
                packer_args=CRABRucioSplitterArgs(**args),
                inputs=iter(inputs),
                feeder_active=feeder_active,
                input_overview={},
                rucio=self.rucio,
                locks=self.locks,
                clock=self.clock,
            )
        )


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


def test_existing_replica_is_pinned_and_packed_at_once(tmp_path):
    world = World(tmp_path)
    out = world.pack([inp("f1.root", {"1": [[1, 3]]})])
    assert [type(o) for o in out] == [ComputeParcel]
    assert out[0].requirements == {"sites": ["T2_IT_Legnaro"]}
    assert out[0].metadata["locks"] == {STORE + "f1.root": "T2_IT_Legnaro"}
    (rule,) = world.rucio.rules.values()
    assert (rule["rse_expression"], rule["state"]) == ("T2_IT_Legnaro", "OK")
    assert rule["comment"] == "CRAB input lock"


def test_transfer_is_waited_for(tmp_path):
    world = World(tmp_path)
    f4 = inp("f4.root", {"4": [[1, 1]]}, n=1)  # only at T2_DE_DESY
    (delay,) = world.pack([f4])
    assert isinstance(delay, DelayInput)
    assert delay.input_ids == [f4.id]
    assert delay.delay_until == world.clock.now() + timedelta(minutes=15)
    world.clock.advance(timedelta(minutes=31))
    (parcel,) = world.pack([f4])
    assert isinstance(parcel, ComputeParcel)
    assert parcel.requirements == {"sites": ["T2_IT_Legnaro"]}
    assert len(world.rucio.rules) == 1  # the same rule, not a new one


def test_chunks_share_one_lock_and_hold_per_site(tmp_path):
    world = World(tmp_path)
    a = inp("f4.root", {"4": [[1, 1]]}, n=1)
    b = inp("f4.root", {"4": [[2, 2]]}, n=1)
    ready = inp("f2.root", {"2": [[4, 5]]}, n=2)
    out = world.pack([a, b, ready])
    # f2 is ready at Legnaro but f4 is still on its way there: hold f2 back
    assert out == [DelayInput([a.id, b.id], out[0].delay_until)]
    assert len(world.rucio.rules) == 2
    world.clock.advance(timedelta(minutes=31))
    parcels = world.pack([a, b, ready])
    assert [p.inputs for p in parcels] == [[a.id, b.id], [ready.id]]


def test_quota_moves_to_the_next_destination(tmp_path):
    world = World(tmp_path)
    world.pack([inp("f1.root", {"1": [[1, 3]]})])
    out = world.pack(
        [inp("f4.root", {"4": [[1, 1]]}, n=1)], destination_quota_bytes=1_000_000
    )
    assert isinstance(out[0], DelayInput)
    assert world.locks.get(STORE + "f4.root").rse == "T2_IT_Pisa"


def test_stuck_rule_is_rerouted_then_given_up(tmp_path):
    world = World(tmp_path)
    f3 = inp("f3.root", {"3": [[1, 3]]})  # tape only; Pisa is stuck
    order = ["T2_IT_Pisa", "T2_IT_Legnaro"]
    world.pack([f3], destinations=order)
    assert world.locks.get(STORE + "f3.root").rse == "T2_IT_Pisa"
    world.clock.advance(timedelta(hours=3))  # recall + transfer, then STUCK
    out = world.pack([f3], destinations=order)
    assert isinstance(out[0], DelayInput)
    lock = world.locks.get(STORE + "f3.root")
    assert (lock.rse, lock.reroutes) == ("T2_IT_Legnaro", 1)
    old = next(
        r for r in world.rucio.rules.values() if r["rse_expression"] == "T2_IT_Pisa"
    )
    assert old["expires_at"] <= world.clock.now() + timedelta(seconds=1)

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


def test_lock_is_idempotent_when_the_table_is_lost(tmp_path):
    world = World(tmp_path)
    f4 = inp("f4.root", {"4": [[1, 1]]}, n=1)
    world.pack([f4])
    world.locks = InMemoryLockTable()  # e.g. the packer task died after add_rule
    world.pack([f4])
    assert len(world.rucio.rules) == 1


def test_unknown_file_is_bad(tmp_path):
    world = World(tmp_path)
    (bad,) = world.pack([inp("nope.root", {"1": [[1, 1]]})])
    assert isinstance(bad, BadInput)


def test_event_based_is_refused():
    with pytest.raises(ValidationError, match="no input to lock"):
        CRABRucioSplitterArgs(
            algorithm="EventBased", units_per_job=10, destinations=["T2_IT_Pisa"]
        )


def test_release_shortens_lifetimes(tmp_path):
    world = World(tmp_path)
    world.pack([inp("f1.root", {"1": [[1, 3]]}), inp("f2.root", {"2": [[4, 6]]})])
    result = crab_release_input_locks(
        args=CRABReleaseInputLocksArgs(grace_hours=1),
        rucio=world.rucio,
        locks=world.locks,
    )
    assert result.result == "Done"
    for rule in world.rucio.rules.values():
        assert rule["expires_at"] == world.clock.now() + timedelta(hours=1)


def test_end_to_end_waits_reroutes_and_releases(tmp_path, capsys):
    rc = main(
        [
            str(HERE / "crab-analysis-rucio.cwl"),
            "--dbs",
            str(HERE / "example_dbs.json"),
            "--rucio",
            str(HERE / "example_rucio.json"),
            "--outdir",
            str(tmp_path),
        ]
    )
    assert rc == 0
    result = json.loads(capsys.readouterr().out)["analysis"]
    assert result["inputs"] == {"Assigned": 5}
    assert result["summary"]["total_lumis"] == 12
    assert result["locks"][STORE + "f3.root"] == {
        "rse": "T1_IT_CNAF_Disk",
        "reroutes": 1,
    }
    assert result["cycles"] > 1
