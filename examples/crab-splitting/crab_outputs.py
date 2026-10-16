"""Register CRAB outputs in Rucio, merge them and transfer them to their destination.

A third example, after the input locks of ``crab_rucio.py``. It follows CRAB's
Rucio ASO model (``ASO/Rucio`` in dmwm/CRABServer): a job writes its output
to the *Temp* area of the site it ran at, the file is registered there under
its final LFN, attached to a dataset of the user's output container, and one
replication rule on the container moves everything to the destination the
user asked for (``Data.outLFNDirBase`` + ``Site.storageSite``). Small outputs
can first be merged at the site they were written at, as WMCore's merge tasks
do, so that only the merged files travel.

How it maps onto the Transformation System of DIRACGrid/diracx#1042:

- **Registration is the core's** (DX-ADR-003): outputs are registered into the
  installation's catalogue(s) while the parcel is ``Completing``, a
  re-entrant step, so ``Done`` means registered and a failed parcel registers
  nothing. :class:`RucioOutputCatalogue` is that catalogue for a Rucio
  installation; :class:`OutputCatalogue` is the interface the core would call.
  The ADRs do not define it as a plugin point yet (see the README).
- **The merge is a transformation** (DX-ADR-006/007): a second step fed by the
  core's *edge feeder* from the first step's ``ParcelOutputs``. The first
  step's outputs are then *intermediate* (not workgraph outputs), registered
  under ``/store/temp/user`` and pinned at the Temp RSE until the merge is
  done. :func:`crab_merge_packer` groups them by the RSE they sit at, up to a
  size, and sends each merge job to that site.
- **The transfer is a Rucio rule** (DX-ADR-008): Rucio is not a data backend
  (a rule is a standing declaration, not a request that completes), so the
  transfer is not a data transformation but a property of the *workgraph
  output*: a ``dirac:RucioRule`` field on the output parameter (a proposal
  for the ADR's open issue "Rucio rules for workgraph outputs"). The
  catalogue creates the rule once, on the container, when the first file is
  registered; files registered later are covered by it.
- **The task is over when the data is there**: the ``Finalizing`` action
  :func:`crab_wait_for_transfers` stays ``Pending`` while the rule is
  replicating and fails on a ``STUCK`` rule; :func:`crab_release_intermediates`
  then lets the pins of the unmerged files expire.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Literal, Protocol

from adr006 import (
    ActionResult,
    BadInput,
    ComputeParcel,
    PackerInput,
    PackerYield,
    Status,
)
from crab_plugins import _manifest
from crab_rucio import WAITING_STATES, RucioClient, site_of
from pydantic import BaseModel, ConfigDict, Field, model_validator

# ---------------------------------------------------------------------------
# The core-facing interface (DX-ADR-003 registration at Completing)
# ---------------------------------------------------------------------------


class RucioRuleHint(BaseModel):
    """``dirac:RucioRule`` on a workgraph output parameter (proposal)."""

    model_config = ConfigDict(extra="forbid")

    rse_expression: str
    container: str | None = None
    lifetime_days: float | None = Field(None, gt=0)
    activity: str = "User Subscriptions"
    copies: int = Field(1, ge=1)


@dataclass(frozen=True)
class ProducedFile:
    """One file a parcel produced on an output port, as the core sees it."""

    workgraph: str
    step: str
    output: str
    parcel_id: int
    site: str
    basename: str
    bytes: int
    adler32: str
    # not declared as a workgraph output (DX-ADR-007 TransformationOutputs.Intermediate)
    intermediate: bool
    rule: dict[str, Any] | None = None


class OutputCatalogue(Protocol):
    """What the core calls for a parcel in ``Completing``; must be idempotent."""

    def register(self, files: list[ProducedFile]) -> list[str]:
        """Register the files and return their LFNs, in order."""
        ...


class RucioOutputClient(RucioClient, Protocol):
    """The registration calls of ``rucio.client.Client``, on top of the rule calls."""

    def add_replicas(self, rse: str, files: list[dict[str, Any]]) -> bool:
        """Declare replicas (``scope``, ``name``, ``bytes``, ``adler32``) at an RSE."""
        ...

    def get_did(self, scope: str, name: str) -> dict[str, Any]:
        """A DID; raises ``DataIdentifierNotFound`` if there is none."""
        ...

    def add_container(self, scope: str, name: str) -> bool:
        """Create an empty container."""
        ...

    def add_dataset(self, scope: str, name: str) -> bool:
        """Create an empty, open dataset."""
        ...

    def attach_dids(self, scope: str, name: str, dids: list[dict[str, str]]) -> bool:
        """Attach files to a dataset, or datasets to a container."""
        ...


@dataclass
class RucioOutputCatalogue:
    """Registers outputs in Rucio the way CRAB's Rucio ASO does.

    For every file, at the Temp RSE of the site the parcel ran at:

    1. ``add_replicas`` under its LFN: ``/store/user/<account>/...`` for a
       workgraph output, ``/store/temp/user/<account>/...`` for an intermediate;
    2. attach it to the dataset ``<container>#<rse>`` (created, and attached to
       the container, on first use);
    3. an intermediate's dataset is pinned at the Temp RSE by a rule with a
       lifetime, so that the site's Temp cleanup does not remove it before it is
       consumed; a workgraph output's container gets the ``dirac:RucioRule``
       rule to its destination, once.

    Every step can be repeated: rules are found again by their ``comment``,
    DIDs by ``get_did``. The rules are remembered in :attr:`rules` (an
    extension table in production) for the ``Finalizing`` actions.
    """

    rucio: RucioOutputClient
    account: str
    intermediate_lifetime_days: float = 7
    temp_suffix: str = "_Temp"
    rules: dict[str, tuple[Literal["final", "intermediate"], str]] = field(
        default_factory=dict
    )
    _dids: set[str] = field(default_factory=set, init=False, repr=False)

    @property
    def scope(self) -> str:
        return f"user.{self.account}"

    def lfn(self, f: ProducedFile) -> str:
        area = "temp/user" if f.intermediate else "user"
        return str(
            PurePosixPath(
                "/store", area, self.account, f.workgraph, f.step, f.output, f.basename
            )
        )

    def container(self, f: ProducedFile) -> str:
        if not f.intermediate and f.rule and f.rule.get("container"):
            return str(f.rule["container"])
        kind = "UNMERGED" if f.intermediate else "USER"
        return f"/{f.workgraph}/{self.account}-{f.step}-{f.output}/{kind}"

    def register(self, files: list[ProducedFile]) -> list[str]:
        lfns = []
        for f in files:
            rse = f.site + self.temp_suffix
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
            if f.intermediate:
                self._rule(
                    dataset,
                    rse,
                    "intermediate",
                    lifetime=int(self.intermediate_lifetime_days * 86400),
                    activity="User Subscriptions",
                )
            elif f.rule:
                hint = RucioRuleHint.model_validate(f.rule)
                self._rule(
                    container,
                    hint.rse_expression,
                    "final",
                    lifetime=(
                        int(hint.lifetime_days * 86400) if hint.lifetime_days else None
                    ),
                    activity=hint.activity,
                    copies=hint.copies,
                )
            lfns.append(lfn)
        return lfns

    def _ensure(self, name: str, kind: Literal["container", "dataset"]) -> bool:
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

    def _rule(
        self,
        name: str,
        rse_expression: str,
        kind: Literal["final", "intermediate"],
        *,
        lifetime: int | None,
        activity: str,
        copies: int = 1,
    ) -> str:
        comment = f"DiracX {kind} output rule"
        for rule in self.rucio.list_did_rules(self.scope, name):
            if (
                rule.get("rse_expression") == rse_expression
                and rule.get("comment") == comment
            ):
                self.rules[str(rule["id"])] = (kind, name)
                return str(rule["id"])
        rule_id = self.rucio.add_replication_rule(
            [{"scope": self.scope, "name": name}],
            copies,
            rse_expression,
            lifetime=lifetime,
            activity=activity,
            comment=comment,
        )[0]
        self.rules[rule_id] = (kind, name)
        return rule_id


# ---------------------------------------------------------------------------
# CRABMerge packer
# ---------------------------------------------------------------------------


class CRABMergeArgs(BaseModel):
    """WMCore's ``ParentlessMergeBySize`` settings (``Data.*`` in CRAB terms)."""

    model_config = ConfigDict(extra="forbid")

    min_merge_size: int = Field(2_000_000_000, gt=0)
    max_merge_size: int = Field(4_000_000_000, gt=0)
    max_merge_files: int = Field(1000, ge=1)
    scope: str = "cms"

    @model_validator(mode="after")
    def _check_sizes(self) -> CRABMergeArgs:
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
            "inputs-manifest": _manifest(group.inputs),
        },
    )


def crab_merge_packer(
    *,
    packer_args: CRABMergeArgs,
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
# Finalizing actions
# ---------------------------------------------------------------------------


class CRABWaitForTransfersArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    fail_on_stuck: bool = True


def crab_wait_for_transfers(
    *,
    args: CRABWaitForTransfersArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    catalogue: RucioOutputCatalogue,
) -> ActionResult:
    """``Pending`` until every output rule is ``OK``: the task is over when the data is there."""
    states: dict[str, int] = {}
    for rule_id, (kind, _) in catalogue.rules.items():
        if kind == "final":
            state = str(rucio.get_replication_rule(rule_id)["state"])
            states[state] = states.get(state, 0) + 1
    summary = ", ".join(f"{n} {s}" for s, n in sorted(states.items())) or "no rules"
    if states.get("STUCK") and args.fail_on_stuck:
        return ActionResult("Failed", message=f"Output transfer stuck: {summary}")
    if any(s in WAITING_STATES for s in states):
        return ActionResult("Pending", message=f"Output transfer: {summary}")
    return ActionResult("Passed", message=f"Output transfer: {summary}")


class CRABReleaseIntermediatesArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grace_hours: float = Field(24, gt=0)


def crab_release_intermediates(
    *,
    args: CRABReleaseIntermediatesArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    catalogue: RucioOutputCatalogue,
) -> ActionResult:
    """Let the pins of the unmerged files expire ``grace_hours`` from now."""
    lifetime = int(args.grace_hours * 3600)
    rules = sorted(
        r for r, (kind, _) in catalogue.rules.items() if kind == "intermediate"
    )
    for rule_id in rules:
        rucio.update_replication_rule(rule_id, {"lifetime": lifetime})
    return ActionResult(
        "Done", message=f"{len(rules)} intermediate pins expire in {lifetime}s"
    )


PACKERS: dict[str, Any] = {"CRABMerge": crab_merge_packer}
ACTIONS: dict[str, Any] = {
    "CRABWaitForTransfers": crab_wait_for_transfers,
    "CRABReleaseIntermediates": crab_release_intermediates,
}
