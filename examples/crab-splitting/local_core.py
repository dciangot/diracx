#!/usr/bin/env python3
r"""A local stand-in for the Transformation System core, to exercise the CRAB plugins.

DiracX does not implement DX-ADR-002..007 yet. This harness plays the core's
part for the transformations of a workgraph, so that the plugins in
``crab_plugins.py`` and the workgraph documents next to it can be run end to
end on a laptop:

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
5. **Completing** (DX-ADR-003/007): register the outputs of a successful
   parcel through the catalogue (``crab_outputs.RucioOutputCatalogue`` with
   ``--rucio``, an LFN policy otherwise) and record them in ``ParcelOutputs``,
   which the core's edge feeder feeds to the steps that consume them.
6. **Finalizing** (DX-ADR-006): once the transformation has drained, run the
   ``Finalizing`` actions this harness knows about, calling a ``Pending`` one
   again on later cycles; the others are reported as skipped.

Every transformation of the workgraph is swept in each cycle, in document
order, so that a consumer sees in the same cycle what its producer registered.

Time is simulated: each cycle advances a clock by ``--cycle-minutes``, and
cycles continue while inputs are delayed (``DelayInput``), so that a packer
waiting on a transfer (``crab_rucio.py``) runs to completion in seconds.

Plugins are looked up through entry points first (the integration plan's
groups below) and then in the ``crab_plugins`` registries.

Usage::

    python3 local_core.py crab-analysis.cwl --dbs example_dbs.json \\
        --replicas example_replicas.json --outdir out [--scouting] [--run]
    python3 local_core.py crab-analysis-rucio.cwl --dbs example_dbs.json \\
        --rucio example_rucio.json --outdir out [--run]
    python3 local_core.py crab-analysis-merge.cwl --dbs example_dbs.json \\
        --rucio example_rucio.json --outdir out --run
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
import zlib
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
from crab_outputs import ACTIONS as OUTPUT_ACTIONS
from crab_outputs import PACKERS as OUTPUT_PACKERS
from crab_outputs import (
    OutputCatalogue,
    ProducedFile,
    RucioOutputCatalogue,
    RucioOutputClient,
    RucioRuleHint,
)
from crab_plugins import (
    FEEDERS,
    HOOKS,
    PACKERS,
    DBSClient,
    ReplicaCatalogue,
)
from crab_rucio import ACTIONS, Clock, InputLock, LockTable, RucioClient
from crab_rucio import PACKERS as RUCIO_PACKERS
from pydantic import BaseModel, ConfigDict, ValidationError

ENTRY_POINT_GROUPS = {
    "feeder": "diracx.transformations.feeders",
    "packer": "diracx.transformations.packers",
    "hook": "diracx.transformations.hooks",
    "action": "diracx.transformations.actions",
}
ARGS_PARAMETER = {
    "feeder": "feeder_args",
    "packer": "packer_args",
    "hook": "args",
    "action": "args",
}
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


class SimClock:
    """Simulated time, advanced by the CLI loop."""

    def __init__(self, start: datetime | None = None) -> None:
        self.t = start or datetime.now(timezone.utc)

    def now(self) -> datetime:
        return self.t

    def advance(self, delta: timedelta) -> None:
        self.t += delta


class InMemoryLockTable:
    """The extension's lock table (a database table in production)."""

    def __init__(self) -> None:
        self._locks: dict[str, InputLock] = {}

    def get(self, lfn: str) -> InputLock | None:
        return self._locks.get(lfn)

    def put(self, lock: InputLock) -> None:
        self._locks[lock.lfn] = lock

    def all(self) -> list[InputLock]:
        return list(self._locks.values())


class MockRucio:
    """Enough of ``rucio.client.Client`` to watch rules progress in simulated time.

    The JSON file holds ``{"rses": {rse: {...}}, "replicas": {lfn: {...}}}``.
    A rule covers a file, or every file of a dataset or container (including
    files attached after the rule was made). A file already at the rule's RSE
    is satisfied at once; otherwise it arrives after the destination's
    ``transfer_minutes``, plus the source's ``recall_minutes`` when every
    source is a tape, and the replica then appears at the destination. The
    rule is ``REPLICATING`` while a file is on its way, then ``OK``, or
    ``STUCK`` if the destination is marked ``stuck``.
    """

    def __init__(self, path: Path, clock: SimClock) -> None:
        data = json.loads(path.read_text())
        self.rses: dict[str, dict[str, Any]] = data["rses"]
        self.replicas: dict[str, dict[str, Any]] = data["replicas"]
        self.clock = clock
        self.rules: dict[str, dict[str, Any]] = {}
        # dataset and container name -> attached DID names
        self.collections: dict[str, dict[str, Any]] = {}

    def _files(self, name: str) -> list[str]:
        if name not in self.collections:
            return [name]
        return [
            f
            for child in self.collections[name]["children"]
            for f in self._files(child)
        ]

    def _delay(self, name: str, rse: str) -> float:
        sources = self.replicas[name]["rses"]
        if rse in sources:
            return 0.0
        delay = float(self.rses[rse].get("transfer_minutes", 0))
        if sources and all(self.rses.get(s, {}).get("tape") for s in sources):
            delay += max(self.rses.get(s, {}).get("recall_minutes", 0) for s in sources)
        return delay

    def _refresh(self, rule: dict[str, Any]) -> dict[str, Any]:
        now = self.clock.now()
        rse = rule["rse_expression"]
        stuck = bool(self.rses[rse].get("stuck"))
        arrivals = rule["arrivals"]
        for name in self._files(rule["name"]):
            if name not in arrivals:
                delay = self._delay(name, rse)
                arrivals[name] = (now + timedelta(minutes=delay), delay > 0)
        pending = [n for n, (t, _) in arrivals.items() if t > now]
        moved = [n for n, (t, needed) in arrivals.items() if t <= now and needed]
        if pending:
            rule["state"] = "REPLICATING"
        elif stuck and moved:
            rule["state"] = "STUCK"
        else:
            rule["state"] = "OK"
        if not stuck:
            for name in moved:
                if rse not in self.replicas[name]["rses"]:
                    self.replicas[name]["rses"].append(rse)
        rule["ready_at"] = max((t for t, _ in arrivals.values()), default=now)
        return rule

    def list_replicas(self, dids: list[dict[str, str]]) -> list[dict[str, Any]]:
        return [
            {
                "scope": did["scope"],
                "name": did["name"],
                "bytes": self.replicas[did["name"]]["bytes"],
                "rses": {rse: [] for rse in self.replicas[did["name"]]["rses"]},
            }
            for did in dids
            if did["name"] in self.replicas
        ]

    def list_did_rules(self, scope: str, name: str) -> list[dict[str, Any]]:
        return [
            self._refresh(r)
            for r in self.rules.values()
            if r["scope"] == scope and r["name"] == name
        ]

    def add_replication_rule(
        self,
        dids: list[dict[str, str]],
        copies: int,
        rse_expression: str,
        lifetime: int | None = None,
        activity: str | None = None,
        comment: str | None = None,
    ) -> list[str]:
        if rse_expression not in self.rses:
            raise ValueError(f"InvalidRSEExpression: {rse_expression}")
        now = self.clock.now()
        ids = []
        for did in dids:
            if did["name"] not in self.collections and did["name"] not in self.replicas:
                raise KeyError(f"DataIdentifierNotFound: {did['name']}")
            rule_id = uuid.uuid4().hex
            self.rules[rule_id] = {
                "id": rule_id,
                "scope": did["scope"],
                "name": did["name"],
                "copies": copies,
                "rse_expression": rse_expression,
                "activity": activity,
                "comment": comment,
                "arrivals": {},
                "expires_at": now + timedelta(seconds=lifetime) if lifetime else None,
            }
            self._refresh(self.rules[rule_id])
            ids.append(rule_id)
        return ids

    def get_replication_rule(self, rule_id: str) -> dict[str, Any]:
        return self._refresh(self.rules[rule_id])

    def update_replication_rule(self, rule_id: str, options: dict[str, Any]) -> bool:
        if "lifetime" in options:
            self.rules[rule_id]["expires_at"] = self.clock.now() + timedelta(
                seconds=options["lifetime"]
            )
        return True

    # -- registration (crab_outputs.py) -------------------------------------

    def add_replicas(self, rse: str, files: list[dict[str, Any]]) -> bool:
        if rse not in self.rses:
            raise ValueError(f"RSENotFound: {rse}")
        for f in files:
            replica = self.replicas.setdefault(
                f["name"],
                {"bytes": f["bytes"], "adler32": f.get("adler32"), "rses": []},
            )
            if rse not in replica["rses"]:
                replica["rses"].append(rse)
        return True

    def get_did(self, scope: str, name: str) -> dict[str, Any]:
        if name in self.collections:
            return {
                "scope": scope,
                "name": name,
                "type": self.collections[name]["type"],
            }
        if name in self.replicas:
            return {"scope": scope, "name": name, "type": "FILE"}
        raise KeyError(f"DataIdentifierNotFound: {name}")

    def _add_collection(self, scope: str, name: str, kind: str) -> bool:
        if name in self.collections or name in self.replicas:
            raise ValueError(f"DataIdentifierAlreadyExists: {name}")
        self.collections[name] = {"scope": scope, "type": kind, "children": []}
        return True

    def add_container(self, scope: str, name: str) -> bool:
        return self._add_collection(scope, name, "CONTAINER")

    def add_dataset(self, scope: str, name: str) -> bool:
        return self._add_collection(scope, name, "DATASET")

    def attach_dids(self, scope: str, name: str, dids: list[dict[str, str]]) -> bool:
        children = self.collections[name]["children"]
        for did in dids:
            self.get_did(did["scope"], did["name"])
            if did["name"] not in children:
                children.append(did["name"])
        return True

    def list_content(self, scope: str, name: str) -> list[dict[str, str]]:
        return [
            {"scope": scope, "name": child}
            for child in self.collections[name]["children"]
        ]


# ---------------------------------------------------------------------------
# Core-provided: ParcelOutputs, the edge feeder, a catalogue without Rucio
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OutputRow:
    """One row of ``ParcelOutputs`` (DX-ADR-007), written at ``Done``."""

    output_id: int  # a UUIDv7 in DX-ADR-007; only its order matters here
    step: str
    output: str
    parcel_id: int
    lfn: str
    lfn_size: int


@dataclass
class ParcelOutputs:
    """The ``ParcelOutputs`` table, plus whether each producer has drained."""

    rows: list[OutputRow] = field(default_factory=list)
    drained: dict[str, bool] = field(default_factory=dict)

    def add(self, step: str, output: str, parcel_id: int, lfn: str, size: int) -> None:
        self.rows.append(
            OutputRow(len(self.rows) + 1, step, output, parcel_id, lfn, size)
        )

    def after(self, step: str, output: str, output_id: int) -> list[OutputRow]:
        return [
            r
            for r in self.rows
            if r.step == step and r.output == output and r.output_id > output_id
        ]


class EdgeFeederArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str  # "<step>/<output>"


def edge_feeder(
    *,
    feeder_args: EdgeFeederArgs,
    feeder_state: dict[str, Any],
    scouting: bool = False,
    # remaining arguments are injected
    outputs: ParcelOutputs,
) -> Iterator[Any]:
    """The core's edge feeder (DX-ADR-006): the producer's outputs, in order."""
    step, port = feeder_args.source.split("/", 1)
    drained = outputs.drained.get(step, False)  # read before the rows
    last = int(feeder_state.get("last_output_id", 0))
    for row in outputs.after(step, port, last):
        yield FileInput(
            descriptor={"producer_parcel": row.parcel_id},
            lfn=row.lfn,
            lfn_size=row.lfn_size,
        )
        last = row.output_id
    yield SetFeederState({"last_output_id": last})
    if drained:
        yield NoMoreInputs()


class LocalCatalogue:
    """The catalogue when there is no Rucio: an LFN policy and nothing else."""

    def register(self, files: list[ProducedFile]) -> list[str]:
        return [
            f"/store/local/{f.workgraph}/{f.step}/{f.output}/{f.basename}"
            for f in files
        ]


REGISTRIES: dict[str, dict[str, Any]] = {
    "feeder": {**FEEDERS, "Edge": edge_feeder},
    "packer": {**PACKERS, **RUCIO_PACKERS, **OUTPUT_PACKERS},
    "hook": HOOKS,
    "action": {**ACTIONS, **OUTPUT_ACTIONS},
}

# ---------------------------------------------------------------------------
# Plugin lookup and invocation
# ---------------------------------------------------------------------------


def lookup(kind: str, name: str) -> Any:
    for ep in entry_points(group=ENTRY_POINT_GROUPS[kind]):
        if ep.name == name:
            return ep.load()
    registry = REGISTRIES[kind]
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
    actions: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    workgraph: str = "workgraph"
    # data output port -> (intermediate, dirac:RucioRule or None)
    outputs: dict[str, tuple[bool, dict[str, Any] | None]] = field(default_factory=dict)


def compile_workgraph(path: Path) -> list[TransformationSpec]:
    doc = yaml.safe_load(path.read_text())
    if doc.get("class") != "Workflow" or f"{DIRAC}Workgraph" not in _hints(doc):
        raise SubmissionError("A workgraph is a Workflow carrying dirac:Workgraph")
    wf_inputs = _by_id(doc.get("inputs"))
    declared: dict[str, dict[str, Any] | None] = {}
    for out_id, out in _by_id(doc.get("outputs")).items():
        rule = out.get(f"{DIRAC}RucioRule")
        if rule is not None:
            try:
                RucioRuleHint.model_validate(rule)
            except ValidationError as exc:
                raise SubmissionError(
                    f"Invalid dirac:RucioRule on {out_id}: {exc}"
                ) from exc
        source = out.get("outputSource")
        for src in source if isinstance(source, list) else [source]:
            declared[src] = rule
    specs = []
    for step_id, step in _by_id(doc.get("steps")).items():
        hint = _hints(step).get(f"{DIRAC}Transformation")
        if hint is None:
            raise SubmissionError(f"Step {step_id!r} has no dirac:Transformation hint")
        sources = _sources(step)
        fed = {
            k: s
            for k, s in sources.items()
            if f"{DIRAC}Feeder" in wf_inputs.get(s, {}) or "/" in s
        }
        driving = hint.get("driving_input") or (
            next(iter(fed)) if len(fed) == 1 else None
        )
        if driving is None or driving not in fed:
            raise SubmissionError(f"Step {step_id!r} has no unambiguous driving input")
        feeder: dict[str, Any]
        if "/" in sources[driving]:
            # Fed by another step's output: the core's edge feeder (DX-ADR-006)
            feeder = {"name": "Edge", "args": {"source": sources[driving]}}
        else:
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
        outs = [o if isinstance(o, str) else o["id"] for o in step.get("out") or []]
        data_outputs = {
            o: (f"{step_id}/{o}" not in declared, declared.get(f"{step_id}/{o}"))
            for o in outs
            if o != report
        }
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
                actions=hint.get("actions") or {},
                workgraph=str(doc.get("id") or path.stem).lstrip("#"),
                outputs=data_outputs,
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
    finalizing: list[dict[str, Any]] = field(default_factory=list)
    finalized: bool = False
    final_pos: int = 0
    lfns: list[str] = field(default_factory=list)

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
        # Finalizing actions this harness provides; the others are reported
        self.final_actions: list[tuple[str, Any, BaseModel | None]] = []
        for entry in self.spec.actions.get("Finalizing") or []:
            try:
                fn = lookup("action", entry["action"])
            except SubmissionError:
                fn = None
            args = validate_args("action", fn, entry.get("args") or {}) if fn else None
            self.final_actions.append((entry["action"], fn, args))

    def now(self) -> datetime:
        clock = self.services.get(Clock)
        return clock.now() if clock else datetime.now(timezone.utc)

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
        now = self.now()
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
                prefix = self.spec.defaults.get("output-prefix")
                if prefix is not None and "output-prefix" not in item.parameters:
                    # Injected per parcel (DX-ADR-007), as CRAB's <name>_<jobid>
                    parcel.parameters["output-prefix"] = f"{prefix}_{parcel.id}"
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
        workdir = outdir / self.spec.step_id / "runs" / f"parcel-{parcel.id:04d}"
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
        outputs: dict[str, Any] = {}
        if proc.returncode == 0:
            outputs = json.loads(proc.stdout)
        if self.spec.status_report:
            report_file = outputs.get(self.spec.status_report)
            if report_file:
                report = json.loads(Path(report_file["path"]).read_text())
        self.apply_report(parcel, report)
        if parcel.status in {"Done", "PartiallyDone"}:
            # Completing (DX-ADR-003): register, then record ParcelOutputs
            self.complete(parcel, outputs)

    def complete(self, parcel: Parcel, outputs: dict[str, Any]) -> None:
        produced = []
        sites = (parcel.requirements or {}).get("sites") or ["LOCAL"]
        for port, (intermediate, rule) in self.spec.outputs.items():
            value = outputs.get(port) or []
            for f in value if isinstance(value, list) else [value]:
                data = Path(f["path"]).read_bytes()
                produced.append(
                    ProducedFile(
                        workgraph=self.spec.workgraph,
                        step=self.spec.step_id,
                        output=port,
                        parcel_id=parcel.id,
                        site=str(sites[0]),
                        basename=f["basename"],
                        bytes=len(data),
                        adler32=f"{zlib.adler32(data):08x}",
                        intermediate=intermediate,
                        rule=rule,
                    )
                )
        catalogue = self.services.get(OutputCatalogue) or LocalCatalogue()
        lfns = catalogue.register(produced)
        table = self.services.get(ParcelOutputs)
        for f, lfn in zip(produced, lfns, strict=True):
            self.lfns.append(lfn)
            if table is not None:
                table.add(f.step, f.output, f.parcel_id, lfn, f.bytes)

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

    def waiting(self) -> bool:
        """Whether some inputs are delayed, i.e. a later sweep has work to do."""
        now = self.now()
        return any(
            r.status == "Unassigned" and r.delayed_until and r.delayed_until > now
            for r in self.rows.values()
        )

    def finalize(self) -> bool:
        """Run the Finalizing actions from where they stopped; whether they are over.

        A ``Pending`` action is called again on the next cycle; a ``Failed``
        one stops the sequence.
        """
        while self.final_pos < len(self.final_actions):
            name, fn, args = self.final_actions[self.final_pos]
            if fn is None:
                entry = {"action": name, "result": "Skipped"}
            else:
                result = call(fn, self.services, args=args)
                entry = {
                    "action": name,
                    "result": result.result,
                    "message": result.message,
                }
            del self.finalizing[self.final_pos :]
            self.finalizing.append(entry)
            if entry["result"] == "Pending":
                return False
            if entry["result"] == "Failed":
                break
            self.final_pos += 1
        return True

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
    parser.add_argument("--rucio", type=Path, help="JSON for the mock Rucio")
    parser.add_argument(
        "--cycle-minutes", type=float, default=15, help="Simulated time per cycle"
    )
    parser.add_argument("--outdir", type=Path, default=Path("out"))
    parser.add_argument(
        "--scouting", action="store_true", help="Run the scouting phase"
    )
    parser.add_argument("--flush", action="store_true", help="Flush the packer once")
    parser.add_argument("--run", action="store_true", help="Run parcels with cwltool")
    parser.add_argument("--cwltool", default="cwltool --quiet --no-container")
    parser.add_argument("--max-jobs", type=int, default=10000, help="maxJobsPerTask")
    parser.add_argument("--max-cycles", type=int, default=100)
    parser.add_argument(
        "--account", default="crabuser", help="Rucio account owning the outputs"
    )
    parser.add_argument(
        "--set",
        action="append",
        default=[],
        metavar="INPUT=JSON",
        help="Extra per-parcel value for the process, e.g. max-lumis=2",
    )
    args = parser.parse_args(argv)
    extra = {k: json.loads(v) for k, v in (item.split("=", 1) for item in args.set)}

    clock = SimClock()
    table = ParcelOutputs()
    services: dict[Any, Any] = {
        ReplicaCatalogue: JSONReplicas(args.replicas),
        Clock: clock,
        ParcelOutputs: table,
    }
    if args.dbs:
        services[DBSClient] = JSONDBS(args.dbs)
    catalogue: RucioOutputCatalogue | None = None
    if args.rucio:
        rucio = MockRucio(args.rucio, clock)
        catalogue = RucioOutputCatalogue(rucio, account=args.account)
        services[RucioClient] = services[RucioOutputClient] = rucio
        services[LockTable] = InMemoryLockTable()
        services[OutputCatalogue] = services[RucioOutputCatalogue] = catalogue

    try:
        specs = compile_workgraph(args.workgraph)
        for spec in specs:
            spec.defaults.update(extra)
        cores = [LocalCore(spec, services, scouting=args.scouting) for spec in specs]
    except SubmissionError as exc:
        print(f"Submission refused: {exc}", file=sys.stderr)
        return 2

    # One cycle runs every transformation in document order, so that a
    # consumer sees in the same cycle what its producer registered.
    cycles = 0
    cwltool = shlex.split(args.cwltool)
    for _ in range(args.max_cycles):
        busy = waiting = False
        for core in cores:
            inserted = core.sweep_feeder()
            new = core.sweep_packer(flush=args.flush)
            if len([p for p in core.parcels if p.kind == "compute"]) > args.max_jobs:
                print(
                    f"Submission refused: the splitting generated more than "
                    f"{args.max_jobs} jobs",
                    file=sys.stderr,
                )
                return 2
            if args.run:
                for parcel in new:
                    core.run_parcel(parcel, args.outdir, cwltool)
            table.drained[core.spec.step_id] = core.drained()
            if core.drained() and not core.finalized:
                core.finalized = core.finalize()
                busy |= not core.finalized
            waiting |= core.waiting()
            busy |= bool(inserted or new)
        cycles += 1
        clock.advance(timedelta(minutes=args.cycle_minutes))
        if waiting:
            continue
        if not args.run or not busy:
            break

    result: dict[str, Any] = {}
    for core in cores:
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
            "cycles": cycles,
        }
        if core.lfns:
            result[core.spec.step_id]["outputs"] = core.lfns
        if core.finalizing:
            result[core.spec.step_id]["finalizing"] = core.finalizing
        if LockTable in typing.get_type_hints(core.packer_fn).values():
            result[core.spec.step_id]["locks"] = {
                lock.lfn: {"rse": lock.rse, "reroutes": lock.reroutes}
                for lock in sorted(services[LockTable].all(), key=lambda x: x.lfn)
            }
    if catalogue is not None and catalogue.rules:
        rules = {}
        for rule_id, (kind, name) in sorted(
            catalogue.rules.items(), key=lambda x: x[1]
        ):
            rule = catalogue.rucio.get_replication_rule(rule_id)
            rules[name] = {
                "kind": kind,
                "rse_expression": rule["rse_expression"],
                "state": rule["state"],
                "expires_at": rule.get("expires_at"),
            }
        result["rucio_rules"] = rules
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
