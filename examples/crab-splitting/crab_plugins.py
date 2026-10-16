"""CRAB input splitting as DiracX Transformation System plugins (DX-ADR-006).

CRABServer's TaskWorker turns a task into jobs in two actions:
``DataDiscovery`` (query DBS, apply the run range and lumi mask, attach
locations) and ``Splitter`` (run a ``WMCore.JobSplitting`` algorithm). In the
DiracX Transformation System (DIRACGrid/diracx#1042) the same work is split
across the roles of DX-ADR-006:

``CRABDBS`` feeder
    DataDiscovery. Evaluates the dataset query once per pass, applies
    ``run_range`` and ``lumi_mask`` and yields one :class:`FileInput` per file,
    or several pre-masked ones when ``max_lumis_per_input`` /
    ``max_events_per_input`` ask for upfront chunking (DX-ADR-006: "upfront
    chunking is just the feeder yielding several pre-masked inputs for one
    LFN"). ``total_units`` is applied here, since how much work exists is a
    feeder concern.

``CRABPrivateMC`` feeder
    The PrivateMC branch of the Splitter: yields one :class:`Input` per
    ``events_per_job`` range, with a cursor bookmark, like the seed feeder of
    DX-ADR-006.

``CRABSplitter`` packer
    The Splitter proper: groups inputs into compute parcels with the CRAB
    algorithms (``FileBased``, ``LumiBased``, ``EventAwareLumiBased``,
    ``EventBased``, ``Automatic``), never mixing storage locations, holding
    back incomplete groups while the feeder is still active.

``CRABHandleFailedInput`` hook
    CRAB's resubmission policy: retry, then split the lumi mask in two, then
    give up to ``Problematic``.

Plugins propose, the core writes: every function here is a pure generator or
function of its arguments and of the injected services.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import OrderedDict
from collections.abc import Generator, Iterable, Iterator
from typing import Any, Literal, Protocol

from adr006 import (
    BadInput,
    ComputeParcel,
    Decision,
    FeederYield,
    FileInput,
    Input,
    JSONSerializable,
    NoMoreInputs,
    PackerInput,
    PackerYield,
    SetFeederState,
    SplitInto,
    Status,
    ToProblematic,
    ToUnassigned,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator

LumiMask = dict[str, list[tuple[int, int]]]

#: TaskWorker defaults (``maxJobsPerTask``, ``numAutomaticProbes``,
#: ``minAutomaticRuntimeMins``) and the scouting sample, which DiracX would
#: resolve from the configuration service.
DEFAULT_MAX_JOBS = 10000
DEFAULT_NUM_PROBES = 5
DEFAULT_MIN_AUTOMATIC_RUNTIME = 180
DEFAULT_SCOUTING_FRACTION = 0.1


class SubmissionRefusedError(ValueError):
    """CRAB's SubmissionRefusedException, raised while validating arguments.

    It derives from ``ValueError`` so that pydantic reports it as a validation
    error, which is how DiracX refuses a document at submission (DX-ADR-006).
    """


# ---------------------------------------------------------------------------
# Injected services
# ---------------------------------------------------------------------------


class DBSClient(Protocol):
    """The metadata catalogue: what CRAB's DataDiscovery asks DBS."""

    def list_files(self, dataset: str) -> Iterable[dict[str, Any]]:
        """Yield ``{"lfn", "size", "events", "runs": {run: [lumi, ...]}}``."""
        ...


class ReplicaCatalogue(Protocol):
    """Where each file is; Rucio for CMS, a JSON replica map locally (DX-ADR-007)."""

    def locations(self, lfn: str) -> list[str]: ...


# ---------------------------------------------------------------------------
# Lumi helpers
# ---------------------------------------------------------------------------


def parse_run_range(run_range: str | None) -> set[int] | None:
    """Parse a CRAB ``Data.runRange`` string such as ``"1-10,15,20-22"``."""
    if not run_range:
        return None
    runs: set[int] = set()
    for chunk in run_range.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "-" in chunk:
            first, last = (int(x) for x in chunk.split("-", 1))
            if first > last:
                raise SubmissionRefusedError(f"Invalid run range {chunk!r}")
            runs.update(range(first, last + 1))
        else:
            runs.add(int(chunk))
    return runs


def compact(run_lumis: dict[int, list[int]]) -> dict[str, list[list[int]]]:
    """``{run: [lumi, ...]}`` to the compact CMS form ``{"run": [[a, b], ...]}``."""
    result: dict[str, list[list[int]]] = OrderedDict()
    for run in sorted(run_lumis):
        ranges: list[list[int]] = []
        for lumi in sorted(set(run_lumis[run])):
            if ranges and ranges[-1][1] + 1 == lumi:
                ranges[-1][1] = lumi
            else:
                ranges.append([lumi, lumi])
        if ranges:
            result[str(run)] = ranges
    return result


def expand(mask: dict[str, Any]) -> Iterator[tuple[int, int]]:
    """Iterate the ``(run, lumi)`` pairs of a compact mask, in order."""
    for run in sorted(mask, key=int):
        for first, last in mask[run]:
            for lumi in range(int(first), int(last) + 1):
                yield int(run), lumi


def _in_mask(mask: LumiMask | None, run: int, lumi: int) -> bool:
    if mask is None:
        return True
    return any(first <= lumi <= last for first, last in mask.get(str(run), []))


def resolve_total(total_units: float, available: float) -> int | None:
    """CRAB ``totalUnits``: ``<= 0`` is everything, ``< 1`` a fraction, else absolute."""
    if total_units <= 0:
        return None
    if total_units < 1.0:
        return int(total_units * available + 0.5)
    return int(total_units)


def _sampled(key: str, fraction: float) -> bool:
    """Deterministic, stable sample: the same file is picked on every pass."""
    digest = hashlib.blake2b(key.encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64 < fraction


# ---------------------------------------------------------------------------
# CRABDBS feeder (DataDiscovery)
# ---------------------------------------------------------------------------


class CRABDBSFeederArgs(BaseModel):
    """``dirac:Feeder`` args; the names follow the CRAB client ``Data.*`` options."""

    model_config = ConfigDict(extra="forbid")

    dataset: str = Field(description="Data.inputDataset")
    lumi_mask: LumiMask | None = Field(None, description="Data.lumiMask, compact")
    run_range: str | None = Field(None, description="Data.runRange")
    total_units: float = Field(0, ge=0, description="Data.totalUnits")
    unit: Literal["files", "lumis", "events"] = Field(
        "files", description="What total_units counts; follows the splitting algorithm"
    )
    max_lumis_per_input: int | None = Field(None, gt=0)
    max_events_per_input: int | None = Field(None, gt=0)
    scouting_fraction: float | None = Field(None, gt=0, le=1)

    @model_validator(mode="after")
    def _check(self) -> CRABDBSFeederArgs:
        parse_run_range(self.run_range)
        return self


def _chunks(
    run_lumis: dict[int, list[int]],
    avg: float,
    max_lumis: int | None,
    max_events: int | None,
) -> Iterator[dict[int, list[int]]]:
    """Cut a file's selected lumis into masks; a lumi is never cut."""
    cur: dict[int, list[int]] = {}
    n = 0
    for run in sorted(run_lumis):
        for lumi in run_lumis[run]:
            full = n > 0 and (
                (max_lumis is not None and n >= max_lumis)
                or (max_events is not None and (n + 1) * avg > max_events)
            )
            if full:
                yield cur
                cur, n = {}, 0
            cur.setdefault(run, []).append(lumi)
            n += 1
    if n:
        yield cur


def crab_dbs_feeder(
    *,
    feeder_args: CRABDBSFeederArgs,
    feeder_state: dict[str, JSONSerializable],
    scouting: bool,
    # remaining arguments are injected
    dbs: DBSClient,
) -> Generator[FeederYield, None, None]:
    """Yield the dataset's files, masked and optionally chunked.

    A CRAB task processes a snapshot of its dataset, so outside scouting the
    feeder makes one full pass and exhausts itself. Each pass recomputes the
    same deterministic selection (sorted by LFN, truncated by ``total_units``),
    so the core's ``(lfn, descriptor)`` hash drops what an earlier pass, or the
    scout, already inserted and no list of yielded inputs needs keeping.
    """
    if feeder_state.get("complete"):
        yield NoMoreInputs()
        return

    args = feeder_args
    runs_whitelist = parse_run_range(args.run_range)
    started_at = time.time()

    # Discovery: apply run range and lumi mask, drop files with nothing left
    selected: list[tuple[dict[str, Any], dict[int, list[int]], float]] = []
    for entry in sorted(dbs.list_files(args.dataset), key=lambda e: e["lfn"]):
        run_lumis: dict[int, list[int]] = {}
        all_lumis = 0
        for run_str, lumis in entry.get("runs", {}).items():
            run = int(run_str)
            all_lumis += len(lumis)
            if runs_whitelist is not None and run not in runs_whitelist:
                continue
            kept = sorted(
                {lumi for lumi in lumis if _in_mask(args.lumi_mask, run, lumi)}
            )
            if kept:
                run_lumis[run] = kept
        if run_lumis:
            avg = int(entry.get("events", 0)) / all_lumis if all_lumis else 0.0
            selected.append((entry, run_lumis, avg))

    def units(run_lumis: dict[int, list[int]], avg: float) -> float:
        n = sum(len(v) for v in run_lumis.values())
        return {"files": 1, "lumis": n, "events": n * avg}[args.unit]

    limit = resolve_total(args.total_units, sum(units(r, a) for _, r, a in selected))
    fraction = args.scouting_fraction or DEFAULT_SCOUTING_FRACTION

    used = 0.0
    for entry, run_lumis, avg in selected:
        if limit is not None and used >= limit:
            break
        used += units(run_lumis, avg)
        if scouting and not _sampled(entry["lfn"], fraction):
            continue
        for chunk in _chunks(
            run_lumis, avg, args.max_lumis_per_input, args.max_events_per_input
        ):
            n = sum(len(v) for v in chunk.values())
            yield FileInput(
                lfn=entry["lfn"],
                lfn_size=int(entry.get("size", 0)),
                descriptor={
                    "lumi_mask": compact(chunk),
                    "lumis": n,
                    "events": int(round(n * avg)),
                },
            )

    yield SetFeederState(state={"last_check": started_at, "complete": not scouting})
    if not scouting:
        yield NoMoreInputs()


# ---------------------------------------------------------------------------
# CRABPrivateMC feeder (PrivateMC branch of the Splitter)
# ---------------------------------------------------------------------------


class CRABPrivateMCFeederArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_events: int = Field(gt=0, description="Data.totalUnits for PrivateMC")
    events_per_job: int = Field(gt=0, description="Data.unitsPerJob")
    events_per_lumi: int | None = Field(None, gt=0, description="JobType.eventsPerLumi")
    max_jobs: int = Field(
        DEFAULT_MAX_JOBS, gt=0, description="TaskWorker.maxJobsPerTask"
    )
    scouting_events: int | None = Field(None, gt=0)
    batch: int | None = Field(
        None, gt=0, description="Inputs per invocation (throttle)"
    )

    @model_validator(mode="after")
    def _check(self) -> CRABPrivateMCFeederArgs:
        # CRAB refuses this before splitting; DiracX refuses it at submission
        n_jobs = math.ceil(self.total_events / self.events_per_job)
        if n_jobs > self.max_jobs:
            raise SubmissionRefusedError(
                f"Your task would generate {n_jobs} jobs. "
                f"The maximum number of jobs in each task is {self.max_jobs}"
            )
        return self


def crab_private_mc_feeder(
    *,
    feeder_args: CRABPrivateMCFeederArgs,
    feeder_state: dict[str, JSONSerializable],
    scouting: bool,
) -> Generator[FeederYield, None, None]:
    """Yield one event range per job; the bookmark is an exact cursor."""
    args = feeder_args
    per_lumi = args.events_per_lumi or args.events_per_job
    target = args.total_events
    if scouting:
        target = min(
            target,
            args.scouting_events or args.events_per_job * DEFAULT_NUM_PROBES,
        )
    next_event = int(feeder_state.get("next_event", 1))
    next_lumi = int(feeder_state.get("next_lumi", 1))
    issued = 0
    while next_event <= target and (args.batch is None or issued < args.batch):
        n = min(args.events_per_job, args.total_events - next_event + 1)
        n_lumis = max(1, math.ceil(n / per_lumi))
        yield Input(
            descriptor={
                "first_event": next_event,
                "last_event": next_event + n - 1,
                "first_lumi": next_lumi,
                "last_lumi": next_lumi + n_lumis - 1,
                "events": n,
            }
        )
        next_event += n
        next_lumi += n_lumis
        issued += 1
    yield SetFeederState(state={"next_event": next_event, "next_lumi": next_lumi})
    if next_event > args.total_events:
        yield NoMoreInputs()


# ---------------------------------------------------------------------------
# CRABSplitter packer
# ---------------------------------------------------------------------------

Algorithm = Literal[
    "FileBased", "LumiBased", "EventAwareLumiBased", "EventBased", "Automatic"
]


class CRABSplitterArgs(BaseModel):
    """``dirac:Transformation`` packer args, named after ``Data.splitting``."""

    model_config = ConfigDict(extra="forbid")

    algorithm: Algorithm
    units_per_job: float | None = Field(None, gt=0, description="Data.unitsPerJob")
    minutes_per_job: int | None = Field(None, gt=0, description="Automatic only")
    events_per_second: float | None = Field(
        None, gt=0, description="Automatic only: throughput measured by the scout"
    )
    probe_files_per_job: int = Field(1, gt=0, description="Automatic only, scout jobs")
    min_automatic_runtime: int = DEFAULT_MIN_AUTOMATIC_RUNTIME
    keep_storage_together: bool = True
    halt_on_file_boundaries: bool = False

    @model_validator(mode="after")
    def _check(self) -> CRABSplitterArgs:
        if self.algorithm == "Automatic":
            if self.minutes_per_job is None:
                raise SubmissionRefusedError(
                    "Automatic splitting needs minutes_per_job"
                )
            if self.minutes_per_job < self.min_automatic_runtime:
                raise SubmissionRefusedError(
                    "Minimum runtime requirement for automatic splitting is "
                    f"{self.min_automatic_runtime} minutes."
                )
        elif self.units_per_job is None:
            raise SubmissionRefusedError(
                f"{self.algorithm} splitting needs units_per_job"
            )
        return self

    def effective(self) -> tuple[str, float]:
        """The algorithm actually run, and its target per parcel.

        Automatic splitting runs FileBased probes until the scout has measured
        the throughput, then EventAwareLumiBased sized to ``minutes_per_job``.
        """
        if self.algorithm == "Automatic":
            assert self.minutes_per_job is not None
            if self.events_per_second is None:
                return "FileBased", self.probe_files_per_job
            return (
                "EventAwareLumiBased",
                self.minutes_per_job * 60 * self.events_per_second,
            )
        assert self.units_per_job is not None
        return self.algorithm, self.units_per_job


class _Group:
    """The parcel being filled for one set of locations."""

    def __init__(self, locations: tuple[str, ...]) -> None:
        self.locations = locations
        self.inputs: list[PackerInput] = []
        self.lfns: list[str] = []
        self.lumis = 0
        self.events = 0

    def cost(self, algorithm: str, inp: PackerInput) -> float:
        if algorithm == "FileBased":
            return 0 if inp.lfn in self.lfns else 1
        if algorithm == "LumiBased":
            return int(inp.descriptor.get("lumis", 0))
        return int(inp.descriptor.get("events", 0))

    def size(self, algorithm: str) -> float:
        return {"FileBased": len(self.lfns), "LumiBased": self.lumis}.get(
            algorithm, self.events
        )

    def add(self, inp: PackerInput) -> None:
        self.inputs.append(inp)
        if inp.lfn is not None and inp.lfn not in self.lfns:
            self.lfns.append(inp.lfn)
        self.lumis += int(inp.descriptor.get("lumis", 0))
        self.events += int(inp.descriptor.get("events", 0))


def _manifest(inputs: list[PackerInput]) -> str:
    return json.dumps(
        [
            {
                "id": str(i.id),
                "lfn": i.lfn,
                "lfn_size": i.lfn_size,
                "descriptor": i.descriptor,
            }
            for i in inputs
        ]
    )


def _data_parcel(group: _Group, algorithm: str) -> ComputeParcel:
    merged: dict[int, list[int]] = {}
    for inp in group.inputs:
        for run, lumi in expand(inp.descriptor["lumi_mask"]):
            merged.setdefault(run, []).append(lumi)
    return ComputeParcel(
        inputs=[i.id for i in group.inputs],
        metadata={
            "algorithm": algorithm,
            "files": len(group.lfns),
            "lumis": group.lumis,
            "events": group.events,
        },
        requirements={"sites": list(group.locations)} if group.locations else None,
        parameters={
            "input-data": [f"lfn:{lfn}" for lfn in group.lfns],
            "lumi-mask": json.dumps(compact(merged)),
            "inputs-manifest": _manifest(group.inputs),
        },
    )


def crab_splitter_packer(
    *,
    packer_args: CRABSplitterArgs,
    inputs: Iterable[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[Status, int],
    # remaining arguments are injected
    replicas: ReplicaCatalogue,
) -> Generator[PackerYield, None, None]:
    """Group claimed inputs into parcels following the CRAB Splitter.

    Inputs are streamed; the packer only buffers one partial parcel per set of
    locations. A full parcel is yielded as soon as it is full; partial ones
    are yielded only once the feeder is off or on ``flush``, otherwise their
    inputs are simply not proposed and stay ``Unassigned`` for the next sweep.
    """
    algorithm, target = packer_args.effective()

    if algorithm == "EventBased":
        for inp in inputs:
            d = inp.descriptor
            if "first_event" not in d:
                yield BadInput(inp.id, reason="EventBased needs an event range")
                continue
            yield ComputeParcel(
                inputs=[inp.id],
                metadata={"algorithm": algorithm, "events": d["events"]},
                requirements=None,
                parameters={
                    "first-event": d["first_event"],
                    "last-event": d["last_event"],
                    "first-lumi": d["first_lumi"],
                    "inputs-manifest": _manifest([inp]),
                },
            )
        return

    groups: dict[tuple[str, ...], _Group] = OrderedDict()
    for inp in inputs:
        if inp.lfn is None or "lumi_mask" not in inp.descriptor:
            yield BadInput(inp.id, reason=f"{algorithm} needs a file with a lumi mask")
            continue
        locations = tuple(sorted(replicas.locations(inp.lfn)))
        if not locations:
            yield BadInput(inp.id, reason="No replica of the file is available")
            continue
        key = locations if packer_args.keep_storage_together else ()
        group = groups.setdefault(key, _Group(key))

        if group.inputs and (
            group.size(algorithm) + group.cost(algorithm, inp) > target
            or (packer_args.halt_on_file_boundaries and inp.lfn not in group.lfns)
        ):
            yield _data_parcel(group, algorithm)
            group = groups[key] = _Group(key)
        group.add(inp)
        if group.size(algorithm) >= target:
            yield _data_parcel(group, algorithm)
            groups[key] = _Group(key)

    # The remainder: packing it early would make small parcels, so wait for
    # more input unless nothing more is coming (DX-ADR-006 drain).
    if flush or not feeder_active:
        for group in groups.values():
            if group.inputs:
                yield _data_parcel(group, algorithm)


# ---------------------------------------------------------------------------
# CRABHandleFailedInput hook
# ---------------------------------------------------------------------------


class CRABHandleFailedInputArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_retries: int = Field(3, ge=0, description="Resubmissions before splitting")
    split_on_exhaustion: bool = True


def crab_handle_failed_input(
    *,
    input: PackerInput,
    parcel: dict[str, JSONSerializable] | None,
    args: CRABHandleFailedInputArgs,
) -> Decision:
    """Retry, then split the lumi mask in two, then quarantine."""
    if input.error_count <= args.max_retries:
        return ToUnassigned()
    mask = input.descriptor.get("lumi_mask")
    lumis = list(expand(mask)) if mask else []
    if args.split_on_exhaustion and input.lfn is not None and len(lumis) > 1:
        events = int(input.descriptor.get("events", 0))
        half = len(lumis) // 2
        children: list[Input | FileInput] = []
        for part in (lumis[:half], lumis[half:]):
            run_lumis: dict[int, list[int]] = {}
            for run, lumi in part:
                run_lumis.setdefault(run, []).append(lumi)
            children.append(
                FileInput(
                    lfn=input.lfn,
                    lfn_size=input.lfn_size or 0,
                    descriptor={
                        "lumi_mask": compact(run_lumis),
                        "lumis": len(part),
                        "events": int(round(events * len(part) / len(lumis))),
                    },
                )
            )
        return SplitInto(children=children)
    return ToProblematic(reason=f"Failed {input.error_count} times")


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------

#: What the entry points of the integration plan register, by the names the
#: CWL hints use. Each plugin's args model is read from the annotation of its
#: ``feeder_args`` / ``packer_args`` / ``args`` parameter.
FEEDERS: dict[str, Any] = {
    "CRABDBS": crab_dbs_feeder,
    "CRABPrivateMC": crab_private_mc_feeder,
}
PACKERS: dict[str, Any] = {"CRABSplitter": crab_splitter_packer}
HOOKS: dict[str, Any] = {"CRABHandleFailedInput": crab_handle_failed_input}
