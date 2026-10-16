#!/usr/bin/env python3
r"""A local stand-in for the Transformation System core, to exercise the CRAB plugins.

DiracX does not implement DX-ADR-002..007 yet. This harness plays the core's
part for one transformation so that the plugins in ``crab_plugins.py`` and the
workgraph documents next to it can be run end to end on a laptop:

1. **Submission** (DX-ADR-007): read the workgraph CWL document, check the
   ``dirac:`` hints, resolve the ``dirac:Feeder`` of the driving input and
   the packer and hooks of the ``dirac:Transformation``, and validate their
   arguments against each plugin's own model.
2. **Feeder sweep** (DX-ADR-006): run the feeder, insert what it yields as
   ``Unassigned`` inputs deduplicated on a hash of ``(lfn, descriptor)``
   (DX-ADR-004), store its bookmark, clear ``FeederEnabled`` on exhaustion.
3. **Packer sweep**: stream the ``Unassigned`` inputs to the packer and turn
   its proposals into parcels, ``Problematic`` inputs or delays.
4. **Dispatch** (optional, ``--run``): write each parcel's CWL input object
   and run the step's process with a stock ``cwltool``; read the job's
   status report (DX-ADR-007) and apply it, calling ``HandleFailedInput``
   for failures (DX-ADR-005).

Plugins are looked up through entry points first (the integration plan's
groups below) and then in the ``crab_plugins`` registries.

Usage::

    python3 local_core.py crab-analysis.cwl --dbs example_dbs.json \\
        --replicas example_replicas.json --outdir out [--scouting] [--run]
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import shlex
import subprocess
import sys
import typing
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any
from uuid import UUID

import yaml
from adr006 import (
    BadInput,
    ComputeParcel,
    DelayInput,
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
    FEEDERS,
    HOOKS,
    PACKERS,
    DBSClient,
    ReplicaCatalogue,
)
from pydantic import BaseModel, ValidationError

ENTRY_POINT_GROUPS = {
    "feeder": "diracx.transformations.feeders",
    "packer": "diracx.transformations.packers",
    "hook": "diracx.transformations.hooks",
}
ARGS_PARAMETER = {"feeder": "feeder_args", "packer": "packer_args", "hook": "args"}
DIRAC = "dirac:"


class SubmissionError(Exception):
    """The document or a plugin's arguments are refused at submission."""


# ---------------------------------------------------------------------------
# Local services: the DX-ADR-007 "JSON file instead of the grid" catalogues
# ---------------------------------------------------------------------------


class JSONDBS:
    """``{dataset: [{"lfn", "size", "events", "runs"}]}`` read from a file."""

    def __init__(self, path: Path) -> None:
        self._data: dict[str, list[dict[str, Any]]] = json.loads(path.read_text())

    def list_files(self, dataset: str) -> list[dict[str, Any]]:
        return self._data.get(dataset, [])


class JSONReplicas:
    """``{lfn: {"replicas": [...], "size": n}}``, the local replica map."""

    def __init__(self, path: Path | None) -> None:
        self._data: dict[str, dict[str, Any]] = (
            json.loads(path.read_text()) if path else {}
        )

    def locations(self, lfn: str) -> list[str]:
        return list(self._data.get(lfn, {}).get("replicas", []))


# ---------------------------------------------------------------------------
# Plugin lookup and invocation
# ---------------------------------------------------------------------------


def lookup(kind: str, name: str) -> Any:
    for ep in entry_points(group=ENTRY_POINT_GROUPS[kind]):
        if ep.name == name:
            return ep.load()
    registry = {"feeder": FEEDERS, "packer": PACKERS, "hook": HOOKS}[kind]
    if name not in registry:
        raise SubmissionError(f"Unknown {kind} plugin {name!r}")
    return registry[name]


def validate_args(kind: str, fn: Any, args: dict[str, Any]) -> BaseModel:
    model: type[BaseModel] = typing.get_type_hints(fn)[ARGS_PARAMETER[kind]]
    try:
        return model.model_validate(args)
    except ValidationError as exc:
        raise SubmissionError(f"Invalid {kind} arguments: {exc}") from exc


def call(fn: Any, services: dict[Any, Any], **kwargs: Any) -> Any:
    """Call a plugin, injecting the services its extra parameters ask for."""
    hints = typing.get_type_hints(fn)
    for name, param in inspect.signature(fn).parameters.items():
        if name in kwargs:
            continue
        if hints.get(name) in services:
            kwargs[name] = services[hints[name]]
        elif param.default is inspect.Parameter.empty:
            raise TypeError(f"Cannot inject {name!r} into {fn.__name__}")
    return fn(**kwargs)


# ---------------------------------------------------------------------------
# Submission: compiling the workgraph document
# ---------------------------------------------------------------------------


def _hints(node: dict[str, Any]) -> dict[str, Any]:
    """CWL hints as a mapping keyed by class; the list form means the same."""
    hints = node.get("hints") or {}
    if isinstance(hints, list):
        return {h["class"]: {k: v for k, v in h.items() if k != "class"} for h in hints}
    return dict(hints)


def _by_id(items: Any) -> dict[str, Any]:
    if isinstance(items, dict):
        return {
            k: (v if isinstance(v, dict) else {"type": v}) for k, v in items.items()
        }
    return {i["id"].lstrip("#"): i for i in items or []}


def _sources(step: dict[str, Any]) -> dict[str, str]:
    raw = step.get("in") or {}
    if isinstance(raw, list):
        raw = {i["id"]: i.get("source", i["id"]) for i in raw}
    return {k: (v["source"] if isinstance(v, dict) else v) for k, v in raw.items()}


@dataclass
class TransformationSpec:
    step_id: str
    run: Path
    driving_input: str
    feeder: str
    feeder_args: dict[str, Any]
    packer: str
    packer_args: dict[str, Any]
    hooks: dict[str, Any]
    requirements_template: dict[str, Any]
    defaults: dict[str, Any]
    status_report: str | None


def compile_workgraph(path: Path) -> list[TransformationSpec]:
    doc = yaml.safe_load(path.read_text())
    if doc.get("class") != "Workflow" or f"{DIRAC}Workgraph" not in _hints(doc):
        raise SubmissionError("A workgraph is a Workflow carrying dirac:Workgraph")
    wf_inputs = _by_id(doc.get("inputs"))
    specs = []
    for step_id, step in _by_id(doc.get("steps")).items():
        hint = _hints(step).get(f"{DIRAC}Transformation")
        if hint is None:
            raise SubmissionError(f"Step {step_id!r} has no dirac:Transformation hint")
        sources = _sources(step)
        fed = {
            k: s for k, s in sources.items() if f"{DIRAC}Feeder" in wf_inputs.get(s, {})
        }
        driving = hint.get("driving_input") or (
            next(iter(fed)) if len(fed) == 1 else None
        )
        if driving is None or driving not in sources:
            raise SubmissionError(f"Step {step_id!r} has no unambiguous driving input")
        if sources[driving] not in fed.values():
            raise SubmissionError(
                f"Step {step_id!r} is fed by {sources[driving]!r}: the edge feeder "
                "is not provided by this local harness"
            )
        feeder = wf_inputs[sources[driving]][f"{DIRAC}Feeder"]
        defaults = {
            k: wf_inputs[s]["default"]
            for k, s in sources.items()
            if k != driving and "default" in wf_inputs.get(s, {})
        }
        run = (path.parent / step["run"]).resolve()
        body = yaml.safe_load(run.read_text())
        report = next(
            (
                o
                for o, v in _by_id(body.get("outputs")).items()
                if v.get(f"{DIRAC}StatusReport")
            ),
            None,
        )
        packer = hint.get("packer") or {"name": "CRABSplitter", "args": {}}
        specs.append(
            TransformationSpec(
                step_id=step_id,
                run=run,
                driving_input=driving,
                feeder=feeder["name"],
                feeder_args=feeder.get("args") or {},
                packer=packer["name"],
                packer_args=packer.get("args") or {},
                hooks=hint.get("hooks") or {},
                requirements_template=hint.get("requirements_template") or {},
                defaults=defaults,
                status_report=report,
            )
        )
    return specs


# ---------------------------------------------------------------------------
# The core loop for one transformation
# ---------------------------------------------------------------------------


@dataclass
class Row:
    """One row of ``Inputs`` (DX-ADR-004)."""

    id: UUID
    lfn: str | None
    lfn_size: int | None
    descriptor: dict[str, Any]
    status: str = "Unassigned"
    error_count: int = 0
    parent: UUID | None = None
    delayed_until: datetime | None = None
    parcel: int | None = None


@dataclass
class Parcel:
    id: int
    inputs: list[UUID]
    metadata: dict[str, Any]
    requirements: dict[str, Any] | None
    parameters: dict[str, Any]
    status: str = "Unassigned"
    kind: str = "compute"


def input_hash(lfn: str | None, descriptor: dict[str, Any]) -> bytes:
    """The DX-ADR-004 ``InputHash`` (XXH3-128 there; BLAKE2b-128 from the stdlib here)."""
    canonical = json.dumps([lfn, descriptor], sort_keys=True, separators=(",", ":"))
    return hashlib.blake2b(canonical.encode(), digest_size=16).digest()


@dataclass
class LocalCore:
    spec: TransformationSpec
    services: dict[Any, Any]
    scouting: bool = False
    rows: dict[UUID, Row] = field(default_factory=dict)
    hashes: set[bytes] = field(default_factory=set)
    feeder_state: dict[str, Any] = field(default_factory=dict)
    feeder_enabled: bool = True
    parcels: list[Parcel] = field(default_factory=list)

    def __post_init__(self) -> None:
        # Submission-time validation of every plugin binding
        self.feeder_fn = lookup("feeder", self.spec.feeder)
        self.feeder_args = validate_args(
            "feeder", self.feeder_fn, self.spec.feeder_args
        )
        self.packer_fn = lookup("packer", self.spec.packer)
        self.packer_args = validate_args(
            "packer", self.packer_fn, self.spec.packer_args
        )
        binding = self.spec.hooks.get("HandleFailedInput")
        self.failed_fn = lookup("hook", binding["hook"]) if binding else None
        self.failed_args = (
            validate_args("hook", self.failed_fn, binding.get("args") or {})
            if binding
            else None
        )

    # -- inputs -------------------------------------------------------------

    def insert(
        self, inp: Input, *, status: str = "Unassigned", parent: UUID | None = None
    ) -> Row | None:
        lfn = inp.lfn if isinstance(inp, FileInput) else None
        size = inp.lfn_size if isinstance(inp, FileInput) else None
        digest = input_hash(lfn, inp.descriptor)
        if digest in self.hashes:
            return None  # refused on the unique InputHash
        self.hashes.add(digest)
        row = Row(uuid.uuid4(), lfn, size, dict(inp.descriptor), status, parent=parent)
        self.rows[row.id] = row
        return row

    def overview(self) -> dict[str, int]:
        return dict(Counter(r.status for r in self.rows.values()))

    # -- sweeps -------------------------------------------------------------

    def sweep_feeder(self) -> int:
        if not self.feeder_enabled:
            return 0
        inserted = 0
        for item in call(
            self.feeder_fn,
            self.services,
            feeder_args=self.feeder_args,
            feeder_state=dict(self.feeder_state),
            scouting=self.scouting,
        ):
            if isinstance(item, SetFeederState):
                self.feeder_state = dict(item.state)
            elif isinstance(item, NoMoreInputs):
                self.feeder_enabled = False
            elif self.insert(item) is not None:
                inserted += 1
        return inserted

    def sweep_packer(self, *, flush: bool = False) -> list[Parcel]:
        now = datetime.now(timezone.utc)
        claimed = [
            r
            for r in self.rows.values()
            if r.status == "Unassigned"
            and (r.delayed_until is None or r.delayed_until <= now)
        ]
        stream = (
            PackerInput(r.id, r.lfn, r.lfn_size, r.descriptor, r.error_count)
            for r in claimed
        )
        new = []
        for item in call(
            self.packer_fn,
            self.services,
            packer_args=self.packer_args,
            inputs=stream,
            flush=flush,
            feeder_active=self.feeder_enabled,
            input_overview=self.overview(),
        ):
            if isinstance(item, ComputeParcel):
                requirements = {
                    **self.spec.requirements_template,
                    **(item.requirements or {}),
                }
                parcel = Parcel(
                    len(self.parcels) + 1,
                    list(item.inputs),
                    dict(item.metadata),
                    requirements or None,
                    {**self.spec.defaults, **item.parameters},
                )
                for input_id in item.inputs:
                    self.rows[input_id].status = "Assigned"
                    self.rows[input_id].parcel = parcel.id
                self.parcels.append(parcel)
                new.append(parcel)
            elif isinstance(item, BadInput):
                self.rows[item.input_id].status = "Problematic"
            elif isinstance(item, DelayInput):
                for input_id in item.input_ids:
                    self.rows[input_id].delayed_until = item.delay_until
        return new

    # -- dispatch and status report -----------------------------------------

    def run_parcel(self, parcel: Parcel, outdir: Path, cwltool: list[str]) -> None:
        workdir = outdir / "runs" / f"parcel-{parcel.id:04d}"
        workdir.mkdir(parents=True, exist_ok=True)
        job = workdir / "inputs.json"
        job.write_text(json.dumps(parcel.parameters, indent=2))
        proc = subprocess.run(  # noqa: S603
            [*cwltool, "--outdir", str(workdir / "out"), str(self.spec.run), str(job)],
            capture_output=True,
            text=True,
            check=False,
        )
        (workdir / "cwltool.log").write_text(proc.stderr)
        report: dict[str, Any] = {}
        if proc.returncode == 0 and self.spec.status_report:
            outputs = json.loads(proc.stdout)
            report_file = outputs.get(self.spec.status_report)
            if report_file:
                report = json.loads(Path(report_file["path"]).read_text())
        self.apply_report(parcel, report)

    def apply_report(self, parcel: Parcel, report: dict[str, Any]) -> None:
        outcomes = set()
        for input_id in parcel.inputs:
            row = self.rows[input_id]
            # Open issue in DX-ADR-006: an input missing from the report is a failure here
            entry = report.get(str(input_id), {"action": "FAILURE"})
            action = entry["action"]
            outcomes.add(action)
            if action == "SUCCESS":
                row.status = "Processed"
            elif action == "NOT_TRIED":
                # Open issue in DX-ADR-005/006; returned directly to the pool here
                row.status = "Unassigned"
            elif action == "SPLIT":
                row.status = "Split"
                recovery = Parcel(
                    len(self.parcels) + 1,
                    [],
                    {"recovery_of": parcel.id},
                    None,
                    {},
                    "Done",
                    "recovery",
                )
                for child in entry.get("SUCCESS", []):
                    new = self.insert(_child(child), status="Processed", parent=row.id)
                    if new:
                        new.parcel = recovery.id
                        recovery.inputs.append(new.id)
                for child in entry.get("FAILURE", []):
                    self.insert(_child(child), parent=row.id)
                self.parcels.append(recovery)
            else:
                row.status = "Failed"
                row.error_count += 1
                self.handle_failed(row, parcel)
        if outcomes <= {"SUCCESS"}:
            parcel.status = "Done"
        elif outcomes & {"SUCCESS", "SPLIT"}:
            parcel.status = "PartiallyDone"
        else:
            parcel.status = "Failed"

    def handle_failed(self, row: Row, parcel: Parcel) -> None:
        if self.failed_fn is None:
            row.status = "Unassigned" if row.error_count <= 3 else "Problematic"
            return
        decision = call(
            self.failed_fn,
            self.services,
            input=PackerInput(
                row.id, row.lfn, row.lfn_size, row.descriptor, row.error_count
            ),
            parcel={"id": parcel.id, "metadata": parcel.metadata},
            args=self.failed_args,
        )
        if isinstance(decision, ToUnassigned):
            row.status = "Unassigned"
        elif isinstance(decision, ToProblematic):
            row.status = "Problematic"
        elif isinstance(decision, SplitInto):
            row.status = "Split"
            for child in decision.children:
                self.insert(child, parent=row.id)

    def drained(self) -> bool:
        live = {"Unassigned", "Assigned", "Failed"}
        return not self.feeder_enabled and not any(
            r.status in live for r in self.rows.values()
        )


def _child(entry: dict[str, Any]) -> Input:
    """A status-report portion, in the shape a feeder yields it."""
    lfn = entry.get("lfn")
    if lfn is None:
        return Input(descriptor=entry.get("data", {}))
    return FileInput(
        lfn=lfn.removeprefix("lfn:"),
        lfn_size=int(entry.get("lfn_size", 0)),
        descriptor=entry.get("data", {}),
    )


def splitting_summary(parcels: list[Parcel]) -> dict[str, Any]:
    """The CRAB ``SplittingSummary`` over the compute parcels."""
    compute = [p for p in parcels if p.kind == "compute"]
    if not compute:
        return {"total_jobs": 0}
    summary: dict[str, Any] = {
        "algo": compute[0].metadata.get("algorithm"),
        "total_jobs": len(compute),
    }
    for key in ("files", "lumis", "events"):
        values = [p.metadata[key] for p in compute if key in p.metadata]
        if values:
            summary.update(
                {
                    f"total_{key}": sum(values),
                    f"max_{key}": max(values),
                    f"avg_{key}": sum(values) / len(values),
                    f"min_{key}": min(values),
                }
            )
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    """Run the feeder/packer loop of each transformation of a workgraph."""
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0] if __doc__ else None
    )
    parser.add_argument("workgraph", type=Path)
    parser.add_argument("--dbs", type=Path, help="JSON DBS dump {dataset: [files]}")
    parser.add_argument("--replicas", type=Path, help="JSON replica map {lfn: {...}}")
    parser.add_argument("--outdir", type=Path, default=Path("out"))
    parser.add_argument(
        "--scouting", action="store_true", help="Run the scouting phase"
    )
    parser.add_argument("--flush", action="store_true", help="Flush the packer once")
    parser.add_argument("--run", action="store_true", help="Run parcels with cwltool")
    parser.add_argument("--cwltool", default="cwltool --quiet --no-container")
    parser.add_argument("--max-jobs", type=int, default=10000, help="maxJobsPerTask")
    parser.add_argument("--max-cycles", type=int, default=20)
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="INPUT=JSON",
        help="Extra per-parcel value for the process, e.g. max-lumis=2",
    )
    args = parser.parse_args(argv)
    extra = {k: json.loads(v) for k, v in (item.split("=", 1) for item in args.set)}

    services: dict[Any, Any] = {ReplicaCatalogue: JSONReplicas(args.replicas)}
    if args.dbs:
        services[DBSClient] = JSONDBS(args.dbs)

    try:
        specs = compile_workgraph(args.workgraph)
        for spec in specs:
            spec.defaults.update(extra)
        cores = [LocalCore(spec, services, scouting=args.scouting) for spec in specs]
    except SubmissionError as exc:
        print(f"Submission refused: {exc}", file=sys.stderr)
        return 2

    result = {}
    for core in cores:
        for _ in range(args.max_cycles):
            core.sweep_feeder()
            new = core.sweep_packer(flush=args.flush)
            if len([p for p in core.parcels if p.kind == "compute"]) > args.max_jobs:
                print(
                    f"Submission refused: the splitting generated more than "
                    f"{args.max_jobs} jobs",
                    file=sys.stderr,
                )
                return 2
            if not args.run:
                break
            for parcel in new:
                core.run_parcel(parcel, args.outdir, shlex.split(args.cwltool))
            if core.drained() or not (new or core.feeder_enabled):
                break

        stepdir = args.outdir / core.spec.step_id
        (stepdir / "parcels").mkdir(parents=True, exist_ok=True)
        for parcel in core.parcels:
            if parcel.kind == "compute":
                (stepdir / "parcels" / f"parcel-{parcel.id:04d}.json").write_text(
                    json.dumps(parcel.parameters, indent=2) + "\n"
                )
        summary = splitting_summary(core.parcels)
        (stepdir / "splitting_summary.json").write_text(
            json.dumps(summary, indent=2) + "\n"
        )
        result[core.spec.step_id] = {
            "feeder_enabled": core.feeder_enabled,
            "feeder_state": core.feeder_state,
            "inputs": core.overview(),
            "parcels": dict(Counter(p.status for p in core.parcels)),
            "summary": summary,
        }
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
