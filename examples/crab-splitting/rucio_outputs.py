"""Outputs (DX-ADR-010): register them in Rucio, merge them, rule them to their destination.

- **Registration is the core's** (DX-ADR-003): a parcel's outputs are
  registered while it is ``Completing``, through the installation's
  :class:`OutputToDestination`. :class:`RucioOutputToDestination` is that
  interface for a Rucio installation: each file is declared where the job
  wrote it (the site's temporary RSE), under its final LFN, and attached to a
  dataset of its output container.
- **Intermediates stay where they are.** A step output that is not a
  workgraph output (DX-ADR-007) is registered at the temporary RSE only; no
  rule protects or moves it, and its consumer reads it there.
- **Merging is a transformation** (DX-ADR-006/007): a step fed by the core's
  edge feeder; :func:`rucio_merge_by_size_packer` groups files by the RSE
  they are at and sends each merge job to that site.
- **The transfer is a rule on the container**, declared on the workgraph
  output with ``dirac:RucioRule``. It is created at once, or, with
  ``min_completion``, by the ``RucioRuleKeeper`` hook (``rucio_hooks.py``)
  once that fraction of the transformation's inputs is processed; at the
  latest by :func:`wait_for_rucio_transfers` at ``Finalizing``.
- **The transformation is over when the data is there**:
  :func:`wait_for_rucio_transfers` is ``Pending`` while a rule replicates.
  :func:`remove_rucio_rules` deletes every rule at ``Cleaning``.
"""

from __future__ import annotations

import json
from collections.abc import Generator, Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Protocol

from adr006 import (
    ActionResult,
    BadInput,
    ComputeParcel,
    PackerInput,
    PackerYield,
    Status,
    Transformation,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator
from rucio_common import (
    WAITING_STATES,
    Clock,
    OutputContainerRow,
    RucioClient,
    RucioTables,
    ensure_rule,
    site_of,
)

# ---------------------------------------------------------------------------
# dirac:RucioRule and the core-facing interface
# ---------------------------------------------------------------------------


class RucioRuleHint(BaseModel):
    """``dirac:RucioRule`` on a workgraph output parameter."""

    model_config = ConfigDict(extra="forbid")

    rse_expression: str
    container: str | None = None
    lifetime_days: float | None = Field(None, gt=0)
    activity: str = "User Subscriptions"
    copies: int = Field(1, ge=1)
    # create the rule once the feeder is exhausted and this fraction of the
    # transformation's inputs is Processed (0: at the first registration)
    min_completion: float = Field(0, ge=0, le=1)


@dataclass(frozen=True)
class ProducedFile:
    """One file a parcel produced on an output port, as the core sees it."""

    transformation_id: int
    workgraph: str
    step: str
    output: str  # the output port
    parcel_id: int  # a UUID in DiracX
    location: str  # where the job wrapper uploaded the file (a site here)
    basename: str
    bytes: int
    adler32: str
    intermediate: bool  # DX-ADR-007 TransformationOutputs.Intermediate
    # the dirac: fields of the workgraph output, without the prefix
    hints: dict[str, Any] = field(default_factory=dict)


class OutputToDestination(Protocol):
    """What the core calls for a parcel in ``Completing``; must be idempotent."""

    def register(self, files: list[ProducedFile]) -> list[str]:
        """Register one parcel's files and return their LFNs, in order."""
        ...


def create_output_rule(
    rucio: RucioClient,
    tables: RucioTables,
    transformation: Transformation,
    row: OutputContainerRow,
    now: datetime,
) -> OutputContainerRow:
    """Create the ``Output`` rule of a container and record it."""
    hint = RucioRuleHint.model_validate(row.rule)
    rule_id = ensure_rule(
        rucio,
        tables,
        transformation,
        "Output",
        scope=row.scope,
        name=row.container,
        rse_expression=hint.rse_expression,
        lifetime=int(hint.lifetime_days * 86400) if hint.lifetime_days else None,
        activity=hint.activity,
        copies=hint.copies,
        now=now,
    )
    row = replace(row, rule_id=rule_id)
    tables.put_output(row)
    return row


@dataclass
class RucioOutputToDestination:
    """:class:`OutputToDestination` for a Rucio installation.

    For every file, at the temporary RSE of the site it was written at:

    1. ``add_replicas`` under its LFN: ``<lfn_base>/...`` for a workgraph
       output, ``<temp_lfn_base>/...`` for an intermediate;
    2. attach it to the dataset ``<container>#<rse>`` (created, and attached
       to the container, on first use);
    3. for a workgraph output with a ``dirac:RucioRule``, record the container
       in ``RucioOutputContainers`` and, unless ``min_completion`` asks to
       wait, create its rule at once. Files registered later are attached to
       the same container and are covered by the same rule.

    Every step can be repeated: DIDs are found by ``get_did``, rules by
    :func:`rucio_common.ensure_rule`. The LFN policy is CMS's: user analysis
    by default (``/store/user/<account>``, ``/store/temp/user/<account>``);
    a production installation passes ``/store/<era>`` and ``/store/unmerged``.
    """

    rucio: RucioClient
    tables: RucioTables
    clock: Clock
    scope: str
    lfn_base: str
    temp_lfn_base: str
    temp_suffix: str = "_Temp"
    _dids: set[str] = field(default_factory=set, init=False, repr=False)

    def lfn(self, f: ProducedFile) -> str:
        base = self.temp_lfn_base if f.intermediate else self.lfn_base
        return str(PurePosixPath(base, f.workgraph, f.step, f.output, f.basename))

    def container(self, f: ProducedFile) -> str:
        rule = f.hints.get("RucioRule") or {}
        if not f.intermediate and rule.get("container"):
            return str(rule["container"])
        kind = "UNMERGED" if f.intermediate else "USER"
        return f"/{f.workgraph}/{f.step}-{f.output}/{kind}"

    def register(self, files: list[ProducedFile]) -> list[str]:
        lfns = []
        for f in files:
            rse = f.location + self.temp_suffix
            lfn = self.lfn(f)
            self.rucio.add_replicas(
                rse,
                [
                    {
                        "scope": self.scope,
                        "name": lfn,
                        "bytes": f.bytes,
                        "adler32": f.adler32,
                    }
                ],
            )
            container = self.container(f)
            dataset = f"{container}#{rse}"
            self._ensure(container, "container")
            if self._ensure(dataset, "dataset"):
                self.rucio.attach_dids(
                    self.scope, container, [{"scope": self.scope, "name": dataset}]
                )
            self.rucio.attach_dids(
                self.scope, dataset, [{"scope": self.scope, "name": lfn}]
            )
            rule = f.hints.get("RucioRule")
            if not f.intermediate and rule:
                self._output(f, container, rule)
            lfns.append(lfn)
        return lfns

    def _output(self, f: ProducedFile, container: str, rule: dict[str, Any]) -> None:
        row = self.tables.output(f.transformation_id, self.scope, container)
        if row is None:
            row = OutputContainerRow(f.transformation_id, self.scope, container, rule)
            self.tables.put_output(row)
        if (
            row.rule_id is None
            and RucioRuleHint.model_validate(rule).min_completion == 0
        ):
            t = Transformation(
                f.transformation_id, f"{f.workgraph}-{f.step}", f.workgraph
            )
            create_output_rule(self.rucio, self.tables, t, row, self.clock.now())

    def _ensure(self, name: str, kind: str) -> bool:
        """Create a collection if needed; whether it was created."""
        if name in self._dids:
            return False
        self._dids.add(name)
        try:
            self.rucio.get_did(self.scope, name)
        except Exception:  # noqa: BLE001 (DataIdentifierNotFound)
            if kind == "container":
                self.rucio.add_container(self.scope, name)
            else:
                self.rucio.add_dataset(self.scope, name)
            return True
        return False


# ---------------------------------------------------------------------------
# RucioMergeBySize packer
# ---------------------------------------------------------------------------


class RucioMergeBySizeArgs(BaseModel):
    """WMCore's ``ParentlessMergeBySize`` settings."""

    model_config = ConfigDict(extra="forbid")

    min_merge_size: int = Field(2_000_000_000, gt=0)
    max_merge_size: int = Field(4_000_000_000, gt=0)
    max_merge_files: int = Field(1000, ge=1)
    scope: str = "cms"

    @model_validator(mode="after")
    def _check_sizes(self) -> RucioMergeBySizeArgs:
        if self.min_merge_size > self.max_merge_size:
            raise ValueError("min_merge_size must not exceed max_merge_size")
        return self


@dataclass
class _MergeGroup:
    rse: str
    inputs: list[PackerInput] = field(default_factory=list)
    bytes: int = 0


def _merge_parcel(group: _MergeGroup) -> ComputeParcel:
    return ComputeParcel(
        inputs=[i.id for i in group.inputs],
        metadata={
            "algorithm": "MergeBySize",
            "files": len(group.inputs),
            "bytes": group.bytes,
            "rse": group.rse,
        },
        requirements={"sites": [site_of(group.rse)]},
        parameters={
            "input-data": [f"lfn:{i.lfn}" for i in group.inputs],
            "inputs-manifest": json.dumps(
                [
                    {
                        "id": str(i.id),
                        "lfn": i.lfn,
                        "lfn_size": i.lfn_size,
                        "descriptor": i.descriptor,
                    }
                    for i in group.inputs
                ]
            ),
        },
    )


def rucio_merge_by_size_packer(
    *,
    packer_args: RucioMergeBySizeArgs,
    inputs: Iterable[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[Status, int],
    # remaining arguments are injected
    rucio: RucioClient,
) -> Generator[PackerYield, None, None]:
    """Merge the files at one RSE into files of ``min..max_merge_size`` bytes.

    Fed by the edge feeder with the upstream step's registered outputs. Files
    are grouped by the RSE their replica is at, never across RSEs, and each
    merge job runs at that RSE's site, so only the merged file is transferred.
    A group is cut as soon as it reaches ``min_merge_size`` (or would exceed
    ``max_merge_size``, or reaches ``max_merge_files``); what is left is
    merged once the feeder is off (the upstream step has drained) or on
    ``flush``. A file larger than ``max_merge_size`` is merged alone.
    """
    groups: dict[str, _MergeGroup] = {}
    for inp in inputs:
        if inp.lfn is None:
            yield BadInput(inp.id, reason="MergeBySize needs files")
            continue
        replica = next(
            iter(rucio.list_replicas([{"scope": packer_args.scope, "name": inp.lfn}])),
            None,
        )
        rses = sorted((replica or {}).get("rses") or {})
        if not rses:
            yield BadInput(inp.id, reason=f"No replica of {inp.lfn}")
            continue
        rse = rses[0]
        size = int(inp.lfn_size or (replica or {}).get("bytes") or 0)
        group = groups.setdefault(rse, _MergeGroup(rse))
        if group.inputs and group.bytes + size > packer_args.max_merge_size:
            yield _merge_parcel(group)
            group = groups[rse] = _MergeGroup(rse)
        group.inputs.append(inp)
        group.bytes += size
        if (
            group.bytes >= packer_args.min_merge_size
            or len(group.inputs) >= packer_args.max_merge_files
        ):
            yield _merge_parcel(group)
            del groups[rse]
    if flush or not feeder_active:
        for group in groups.values():
            yield _merge_parcel(group)


# ---------------------------------------------------------------------------
# Actions
# ---------------------------------------------------------------------------


class WaitForRucioTransfersArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail_on_stuck: bool = True


def wait_for_rucio_transfers(
    *,
    args: WaitForRucioTransfersArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    tables: RucioTables,
    clock: Clock,
    transformation: Transformation,
) -> ActionResult:
    """``Pending`` until every ``Output`` rule is ``OK``: over when the data is there.

    A container whose rule waited for ``min_completion`` gets it now: the
    transformation has drained, so whatever it was going to produce exists.
    """
    for row in tables.outputs(transformation.id):
        if row.rule_id is None:
            create_output_rule(rucio, tables, transformation, row, clock.now())
    states: dict[str, int] = {}
    for rule in tables.rules(transformation.id):
        if rule.kind == "Output":
            state = str(rucio.get_replication_rule(rule.rule_id)["state"])
            states[state] = states.get(state, 0) + 1
    summary = ", ".join(f"{n} {s}" for s, n in sorted(states.items())) or "no rules"
    if states.get("STUCK") and args.fail_on_stuck:
        return ActionResult("Failed", message=f"Output transfer stuck: {summary}")
    if any(s in WAITING_STATES for s in states):
        return ActionResult("Pending", message=f"Output transfer: {summary}")
    return ActionResult("Passed", message=f"Output transfer: {summary}")


class RemoveRucioRulesArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")


def remove_rucio_rules(
    *,
    args: RemoveRucioRulesArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    tables: RucioTables,
    transformation: Transformation,
) -> ActionResult:
    """Delete every rule of the transformation, outputs included (``Cleaning``)."""
    rows = tables.rules(transformation.id)
    for row in rows:
        rucio.delete_replication_rule(row.rule_id)
        tables.delete_rule(transformation.id, row.rule_id)
    return ActionResult("Done", message=f"{len(rows)} rules deleted")


PACKERS: dict[str, Any] = {"RucioMergeBySize": rucio_merge_by_size_packer}
ACTIONS: dict[str, Any] = {
    "WaitForRucioTransfers": wait_for_rucio_transfers,
    "RemoveRucioRules": remove_rucio_rules,
}
