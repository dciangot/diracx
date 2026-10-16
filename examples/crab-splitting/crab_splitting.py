#!/usr/bin/env python3
"""CRAB-like input splitting expressed as two independent steps: feeder and packer.

This is a draft, dependency-free (stdlib only) re-implementation of the input
splitting procedure performed by the CRABServer TaskWorker
(``TaskWorker/Actions/DataDiscovery.py`` + ``TaskWorker/Actions/Splitter.py``,
which delegates to the ``WMCore.JobSplitting`` algorithms).

The procedure is split in two steps so that it can be wired as a CWL workflow:

``feed``
    Equivalent of the CRAB data discovery post-processing. It takes the raw
    list of input files (as returned by DBS/Rucio: LFN, events, runs/lumis,
    locations), applies the user run range and lumi mask, drops files without
    any surviving lumi and emits a normalised *feed* document with per-file
    statistics (``lumiCount``, ``avgEvtsPerLumi``...). For ``PrivateMC`` tasks
    there is no input data, so the feed only carries the generation parameters.

``pack``
    Equivalent of ``Splitter.execute``: it reads the feed and packs the units
    (files, lumis or events) into jobs according to the requested algorithm
    (``FileBased``, ``LumiBased``, ``EventAwareLumiBased``, ``Automatic`` or
    ``EventBased``), enforces ``totalUnits`` and ``maxJobsPerTask`` and writes
    the job list together with a splitting summary (as ``SplittingSummary``).

Input file format for ``feed`` (JSON list)::

    [
        {
            "lfn": "/store/...root",
            "events": 1000,
            "size": 123,
            "locations": ["T2_IT_Legnaro"],
            "runs": {"1": [1, 2, 3]},
        },
        ...,
    ]

Lumi mask format is the usual CMS compact JSON: ``{"run": [[first, last], ...]}``.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import OrderedDict
from collections.abc import Iterator
from typing import Any

FEED_VERSION = 1
ALGORITHMS = (
    "FileBased",
    "LumiBased",
    "EventAwareLumiBased",
    "Automatic",
    "EventBased",
)


class SplittingError(Exception):
    """Equivalent of CRAB's SubmissionRefusedException for the splitting step."""


# ---------------------------------------------------------------------------
# Helpers
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
                raise SplittingError(f"Invalid run range {chunk!r}")
            runs.update(range(first, last + 1))
        else:
            runs.add(int(chunk))
    return runs


def mask_contains(mask: dict[str, list[list[int]]] | None, run: int, lumi: int) -> bool:
    """Return True if (run, lumi) is selected by the compact lumi mask."""
    if mask is None:
        return True
    return any(first <= lumi <= last for first, last in mask.get(str(run), []))


def compact_lumis(run_lumis: dict[int, list[int]]) -> dict[str, list[list[int]]]:
    """Convert ``{run: [lumi, ...]}`` into the compact CMS ``{run: [[a, b], ...]}``."""
    result: dict[str, list[list[int]]] = OrderedDict()
    for run in sorted(run_lumis):
        ranges: list[list[int]] = []
        for lumi in sorted(set(run_lumis[run])):
            if ranges and ranges[-1][1] + 1 == lumi:
                ranges[-1][1] = lumi
            else:
                ranges.append([lumi, lumi])
        result[str(run)] = ranges
    return result


def resolve_total(total_units: float, available: float) -> int | None:
    """Mimic CRAB ``tm_totalunits``: <=0 means all, <1 is a fraction, else absolute."""
    if total_units <= 0:
        return None
    if total_units < 1.0:
        return int(total_units * available + 0.5)
    return int(total_units)


def _load_json(path: str | None) -> Any:
    if not path:
        return None
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _dump_json(obj: Any, path: str) -> None:
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)
        fh.write("\n")


# ---------------------------------------------------------------------------
# Feeder
# ---------------------------------------------------------------------------


def feed(
    input_files: list[dict[str, Any]] | None,
    job_type: str = "Analysis",
    lumi_mask: dict[str, list[list[int]]] | None = None,
    run_range: str | None = None,
    total_events: int = 0,
    events_per_lumi: int | None = None,
) -> dict[str, Any]:
    """Normalise and filter the discovered input into a feed document."""
    if job_type == "PrivateMC":
        if total_events <= 0:
            raise SplittingError("PrivateMC tasks require a positive number of events")
        return {
            "version": FEED_VERSION,
            "job_type": job_type,
            "total_events": total_events,
            "events_per_lumi": events_per_lumi,
            "files": [],
        }

    if not input_files:
        raise SplittingError("No input files were provided to the feeder")

    runs_whitelist = parse_run_range(run_range)
    files = []
    for entry in input_files:
        run_lumis: dict[int, list[int]] = {}
        all_lumis = 0
        for run_str, lumis in entry.get("runs", {}).items():
            run = int(run_str)
            all_lumis += len(lumis)
            if runs_whitelist is not None and run not in runs_whitelist:
                continue
            kept = [lumi for lumi in lumis if mask_contains(lumi_mask, run, lumi)]
            if kept:
                run_lumis[run] = sorted(set(kept))
        lumi_count = sum(len(v) for v in run_lumis.values())
        if lumi_count == 0:
            continue
        events = int(entry.get("events", 0))
        # Same definition as WMCore: average over all the lumis in the file
        avg = events / all_lumis if all_lumis else 0.0
        files.append(
            {
                "lfn": entry["lfn"],
                "events": events,
                "size": int(entry.get("size", 0)),
                "locations": sorted(entry.get("locations", [])),
                "parents": entry.get("parents", []),
                "runs": compact_lumis(run_lumis),
                "lumiCount": lumi_count,
                "avgEvtsPerLumi": avg,
            }
        )

    files.sort(key=lambda f: f["lfn"])
    return {
        "version": FEED_VERSION,
        "job_type": job_type,
        "run_whitelist": sorted(runs_whitelist) if runs_whitelist else None,
        "files": files,
    }


# ---------------------------------------------------------------------------
# Packer
# ---------------------------------------------------------------------------


def _iter_lumis(file_: dict[str, Any]) -> Iterator[tuple[int, int]]:
    for run_str, ranges in file_["runs"].items():
        for first, last in ranges:
            for lumi in range(first, last + 1):
                yield int(run_str), lumi


def _location_groups(files: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    """WMCore splits per set of locations: a job never mixes files from different locations."""
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = OrderedDict()
    for f in files:
        groups.setdefault(tuple(f["locations"]), []).append(f)
    return list(groups.values())


def _make_job(
    files: dict[str, dict[str, Any]],
    run_lumis: dict[int, list[int]],
    events: float,
) -> dict[str, Any]:
    return {
        "input_files": [
            {"lfn": lfn, "events": f["events"], "locations": f["locations"]}
            for lfn, f in files.items()
        ],
        "locations": next(iter(files.values()))["locations"] if files else [],
        "mask": {"runAndLumis": compact_lumis(run_lumis)} if run_lumis else {},
        "estimated_events": int(round(events)),
    }


def _pack_file_based(
    files: list[dict[str, Any]], files_per_job: int, total_files: int | None
) -> list[dict[str, Any]]:
    if files_per_job <= 0:
        raise SplittingError("files_per_job must be positive")
    jobs: list[dict[str, Any]] = []
    used = 0
    for group in _location_groups(files):
        for i in range(0, len(group), files_per_job):
            chunk = group[i : i + files_per_job]
            if total_files is not None:
                chunk = chunk[: max(total_files - used, 0)]
            if not chunk:
                return jobs
            used += len(chunk)
            run_lumis: dict[int, list[int]] = {}
            for f in chunk:
                for run, lumi in _iter_lumis(f):
                    run_lumis.setdefault(run, []).append(lumi)
            jobs.append(
                _make_job(
                    OrderedDict((f["lfn"], f) for f in chunk),
                    run_lumis,
                    sum(f["events"] for f in chunk),
                )
            )
    return jobs


def _pack_lumis(
    files: list[dict[str, Any]],
    *,
    lumis_per_job: int | None = None,
    events_per_job: float | None = None,
    limit: int | None = None,
    halt_on_file_boundaries: bool = False,
) -> list[dict[str, Any]]:
    """Shared implementation of LumiBased and EventAwareLumiBased.

    Lumis are never split across jobs. For EventAwareLumiBased a job is closed
    as soon as adding the next lumi would exceed ``events_per_job`` (a single
    lumi larger than ``events_per_job`` still produces a one-lumi job).
    ``limit`` is the total number of lumis (LumiBased) or events
    (EventAwareLumiBased) to process, i.e. CRAB's ``totalUnits``.
    """
    if (lumis_per_job is None) == (events_per_job is None):
        raise ValueError("Exactly one of lumis_per_job/events_per_job is required")
    if (lumis_per_job or events_per_job or 0) <= 0:
        raise SplittingError("units per job must be positive")

    jobs: list[dict[str, Any]] = []
    consumed = 0.0
    for group in _location_groups(files):
        cur_files: dict[str, dict[str, Any]] = OrderedDict()
        cur_lumis: dict[int, list[int]] = {}
        cur_n = 0
        cur_events = 0.0

        def flush() -> None:
            nonlocal cur_files, cur_lumis, cur_n, cur_events
            if cur_n:
                jobs.append(_make_job(cur_files, cur_lumis, cur_events))
            cur_files, cur_lumis, cur_n, cur_events = OrderedDict(), {}, 0, 0.0

        for f in group:
            avg = f["avgEvtsPerLumi"]
            for run, lumi in _iter_lumis(f):
                if limit is not None and consumed >= limit:
                    flush()
                    return jobs
                if lumis_per_job is not None:
                    full = cur_n >= lumis_per_job
                else:
                    assert events_per_job is not None
                    full = cur_n > 0 and cur_events + avg > events_per_job
                if full:
                    flush()
                cur_files.setdefault(f["lfn"], f)
                cur_lumis.setdefault(run, []).append(lumi)
                cur_n += 1
                cur_events += avg
                consumed += 1 if lumis_per_job is not None else avg
            if halt_on_file_boundaries:
                flush()
        flush()
    return jobs


def _pack_event_based(
    feed_doc: dict[str, Any], events_per_job: int, total_events: int
) -> list[dict[str, Any]]:
    per_lumi = feed_doc.get("events_per_lumi") or events_per_job
    jobs: list[dict[str, Any]] = []
    first_event, first_lumi = 1, 1
    while first_event <= total_events:
        n = min(events_per_job, total_events - first_event + 1)
        n_lumis = max(1, math.ceil(n / per_lumi))
        jobs.append(
            {
                "input_files": [],
                "locations": [],
                "mask": {
                    "FirstEvent": first_event,
                    "LastEvent": first_event + n - 1,
                    "FirstLumi": first_lumi,
                    "LastLumi": first_lumi + n_lumis - 1,
                },
                "estimated_events": n,
            }
        )
        first_event += n
        first_lumi += n_lumis
    return jobs


def pack(
    feed_doc: dict[str, Any],
    algorithm: str,
    units_per_job: float,
    total_units: float = 0,
    max_jobs: int = 10000,
    num_probes: int = 5,
    min_automatic_runtime: int = 180,
    halt_on_file_boundaries: bool = False,
) -> list[dict[str, Any]]:
    """Pack the feed into jobs following the CRAB Splitter logic."""
    if algorithm not in ALGORITHMS:
        raise SplittingError(f"Unknown splitting algorithm {algorithm!r}")
    files = feed_doc.get("files", [])
    job_type = feed_doc.get("job_type", "Analysis")

    if job_type == "PrivateMC":
        if algorithm != "EventBased":
            raise SplittingError("PrivateMC tasks only support EventBased splitting")
        events_per_job = int(units_per_job)
        if events_per_job <= 0:
            raise SplittingError("events_per_job must be positive")
        total_events = int(total_units) or int(feed_doc["total_events"])
        n_jobs = total_events // events_per_job
        if n_jobs > max_jobs:
            raise SplittingError(
                f"Your task would generate {n_jobs} jobs. "
                f"The maximum number of jobs in each task is {max_jobs}"
            )
        jobs = _pack_event_based(feed_doc, events_per_job, total_events)
    elif algorithm == "EventBased":
        raise SplittingError("EventBased splitting requires a PrivateMC feed")
    elif algorithm == "FileBased":
        jobs = _pack_file_based(
            files, int(units_per_job), resolve_total(total_units, len(files))
        )
    elif algorithm == "Automatic":
        # units_per_job is minutes_per_job: CRAB first runs FileBased probe jobs
        if units_per_job < min_automatic_runtime:
            raise SplittingError(
                "Minimum runtime requirement for automatic splitting is "
                f"{min_automatic_runtime} minutes."
            )
        files_per_job = (len(files) + num_probes - 1) // num_probes
        jobs = _pack_file_based(files, max(files_per_job, 1), len(files))
    elif algorithm == "LumiBased":
        jobs = _pack_lumis(
            files,
            lumis_per_job=int(units_per_job),
            limit=resolve_total(total_units, sum(f["lumiCount"] for f in files)),
            halt_on_file_boundaries=halt_on_file_boundaries,
        )
    else:  # EventAwareLumiBased
        available = sum(f["avgEvtsPerLumi"] * f["lumiCount"] for f in files)
        jobs = _pack_lumis(
            files,
            events_per_job=units_per_job,
            limit=resolve_total(total_units, available),
            halt_on_file_boundaries=halt_on_file_boundaries,
        )

    if not jobs:
        msg = f"Splitting with {algorithm} method does not generate any job."
        if feed_doc.get("run_whitelist"):
            rw = feed_doc["run_whitelist"]
            msg += f"\nRun list from task configuration is inside the range: {rw[0]} - {rw[-1]}"
        raise SplittingError(msg)
    if len(jobs) > max_jobs:
        raise SplittingError(
            f"The splitting on your task generated {len(jobs)} jobs. "
            f"The maximum number of jobs in each task is {max_jobs}"
        )
    for job_id, job in enumerate(jobs, start=1):
        job["job_id"] = job_id
    return jobs


def summarize(algorithm: str, jobs: list[dict[str, Any]]) -> dict[str, Any]:
    """Equivalent of CRAB ``SplittingSummary.dump``."""
    events = [j["estimated_events"] for j in jobs]
    if algorithm == "EventBased":
        lumis = [j["mask"]["LastLumi"] - j["mask"]["FirstLumi"] + 1 for j in jobs]
    else:
        lumis = [
            sum(
                last - first + 1
                for ranges in j["mask"].get("runAndLumis", {}).values()
                for first, last in ranges
            )
            for j in jobs
        ]
    summary: dict[str, Any] = {
        "algo": algorithm,
        "total_jobs": len(jobs),
        "total_lumis": sum(lumis),
        "total_events": sum(events),
        "max_lumis": max(lumis),
        "max_events": max(events),
        "avg_lumis": sum(lumis) / len(lumis),
        "avg_events": sum(events) / len(events),
        "min_lumis": min(lumis),
        "min_events": min(events),
    }
    if algorithm in ("FileBased", "Automatic"):
        nfiles = [len(j["input_files"]) for j in jobs]
        summary.update(
            {
                "total_files": sum(nfiles),
                "max_files": max(nfiles),
                "avg_files": sum(nfiles) / len(nfiles),
                "min_files": min(nfiles),
            }
        )
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Command line entry point."""
    parser = argparse.ArgumentParser(description="CRAB-like feeder/packer splitting")
    sub = parser.add_subparsers(dest="command", required=True)

    p_feed = sub.add_parser("feed", help="Filter and normalise the input data")
    p_feed.add_argument("--input-files", help="JSON list of discovered input files")
    p_feed.add_argument(
        "--job-type", choices=("Analysis", "PrivateMC"), default="Analysis"
    )
    p_feed.add_argument("--lumi-mask", help="CMS compact JSON lumi mask")
    p_feed.add_argument("--run-range", help="e.g. '1-10,15'")
    p_feed.add_argument("--total-events", type=int, default=0, help="PrivateMC only")
    p_feed.add_argument("--events-per-lumi", type=int, help="PrivateMC only")
    p_feed.add_argument("--output", default="feed.json")

    p_pack = sub.add_parser("pack", help="Pack the feed into jobs")
    p_pack.add_argument("--feed", required=True)
    p_pack.add_argument("--algorithm", choices=ALGORITHMS, required=True)
    p_pack.add_argument(
        "--units-per-job",
        type=float,
        required=True,
        help="files, lumis, events or minutes (Automatic) per job",
    )
    p_pack.add_argument("--total-units", type=float, default=0)
    p_pack.add_argument("--max-jobs", type=int, default=10000)
    p_pack.add_argument("--num-probes", type=int, default=5)
    p_pack.add_argument("--halt-on-file-boundaries", action="store_true")
    p_pack.add_argument("--output", default="jobs.json")
    p_pack.add_argument("--summary", default="splitting_summary.json")

    args = parser.parse_args(argv)
    try:
        if args.command == "feed":
            doc = feed(
                _load_json(args.input_files),
                job_type=args.job_type,
                lumi_mask=_load_json(args.lumi_mask),
                run_range=args.run_range,
                total_events=args.total_events,
                events_per_lumi=args.events_per_lumi,
            )
            _dump_json(doc, args.output)
        else:
            jobs = pack(
                _load_json(args.feed),
                args.algorithm,
                args.units_per_job,
                total_units=args.total_units,
                max_jobs=args.max_jobs,
                num_probes=args.num_probes,
                halt_on_file_boundaries=args.halt_on_file_boundaries,
            )
            _dump_json(jobs, args.output)
            _dump_json(summarize(args.algorithm, jobs), args.summary)
    except SplittingError as exc:
        print(f"Submission refused: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
