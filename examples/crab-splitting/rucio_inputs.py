"""Input rules (DX-ADR-010): hold each input dataset at the job's site with Rucio.

Any workgraph input that carries a ``dirac:Feeder`` can also carry a
``dirac:RucioInputRule``. The core then wraps the step's packer in
:func:`rucio_input_rules_packer`, with the hint as its arguments, so the
community's ordinary packer (``CRABSplitter`` for CMS analysis, or any other)
only ever sees inputs whose dataset is at the site the parcel will run at.

For each claimed input:

1. **Dataset and container.** Find the Rucio dataset the file belongs to,
   and the container that dataset belongs to (``list_parent_dids``; a CMS
   block and a CMS dataset).
2. **Choose, once per dataset.** Pick a destination RSE among the hint's
   ``destinations``: the first one that already holds the whole dataset (the
   rule is then a pin), otherwise the first one under the per-RSE thresholds
   ``max_files_per_rse`` and ``max_bytes_per_rse`` of the transformation. The
   choice is a row of ``RucioInputDatasets``.
3. **Cloned container and rule.** Create a container
   ``<container>__dx-<transformation>__<RSE>`` in ``clone_scope``, put one rule
   on it at that RSE, and attach the original dataset to it. A rule on a
   container covers the datasets attached after it was made, so the clone
   holds what the transformation reads, and two transformations (even of the
   same Rucio account) never share a rule. Retried tasks find the clone by
   its name and the rule by its DID, RSE and comment.
4. **Monitor, dataset by dataset.** A dataset whose replica at the
   destination is complete (``list_dataset_replicas``, ``AVAILABLE``) passes
   its inputs on to the wrapped packer with that RSE's site as their only
   location; the parcel's ``requirements.sites`` is therefore where the data
   is. The inputs of any other dataset are delayed (``DelayInput``) for
   ``poll_minutes``.
5. **Re-route.** If the clone's rule is ``STUCK``, or the dataset is still
   incomplete after ``max_wait_hours``, the dataset is detached from that
   clone and attached to the clone of the next candidate, up to
   ``max_reroutes``; then its inputs are ``BadInput``. The old rule keeps
   holding the other datasets of its clone, if any.

Rule lifetimes are renewed while inputs still need them by the
``RucioRuleKeeper`` hook (``rucio_hooks.py``) and released at the end by
:func:`release_rucio_input_rules`.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from adr006 import (
    ActionResult,
    BadInput,
    ComputeParcel,
    DelayInput,
    PackerInput,
    PackerYield,
    Status,
    Transformation,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator
from rucio_common import (
    WAITING_STATES,
    Clock,
    InputDatasetRow,
    RucioClient,
    RucioTables,
    ensure_rule,
    release_rule,
    site_of,
)

CLONE_MARK = "__dx-"


class RucioInputRule(BaseModel):
    """``dirac:RucioInputRule`` on a workgraph input, next to its ``dirac:Feeder``."""

    model_config = ConfigDict(extra="forbid")

    destinations: list[str] = Field(min_length=1)
    scope: str = "cms"
    clone_scope: str | None = None  # the scope of the clones; ``scope`` if unset
    activity: str = "Analysis Input"
    max_files_per_rse: int | None = Field(None, ge=1)
    max_bytes_per_rse: int | None = Field(None, ge=1)
    lifetime_days: float = Field(14, gt=0)
    poll_minutes: float = Field(15, gt=0)
    max_wait_hours: float = Field(48, gt=0)
    max_reroutes: int = Field(1, ge=0)

    @model_validator(mode="after")
    def _distinct(self) -> RucioInputRule:
        if len(set(self.destinations)) != len(self.destinations):
            raise ValueError("destinations must be distinct")
        return self


def clone_name(container: str, transformation: Transformation, rse: str) -> str:
    """The transformation's clone of an input container, for one destination RSE."""
    return f"{container}{CLONE_MARK}{transformation.name}__{rse}"


# ---------------------------------------------------------------------------
# The wrapped packer, as the core passes it
# ---------------------------------------------------------------------------


class Locations(Protocol):
    def locations(self, lfn: str) -> list[str]: ...


class InnerPacker(Protocol):
    """The step's own packer, bound to its arguments and services by the core.

    ``replicas`` replaces the replica view the inner packer would otherwise
    get injected, so that a file is only at the site it is held at.
    """

    def __call__(
        self,
        *,
        inputs: Iterable[PackerInput],
        flush: bool,
        feeder_active: bool,
        input_overview: dict[Status, int],
        replicas: Locations,
    ) -> Iterable[PackerYield]: ...


@dataclass(frozen=True)
class _HeldAt:
    sites: dict[str, str]

    def locations(self, lfn: str) -> list[str]:
        return [self.sites[lfn]] if lfn in self.sites else []


# ---------------------------------------------------------------------------
# Choosing, cloning and ruling, for one packer invocation
# ---------------------------------------------------------------------------


class _Planner:
    def __init__(
        self,
        hint: RucioInputRule,
        rucio: RucioClient,
        tables: RucioTables,
        transformation: Transformation,
        now: datetime,
    ) -> None:
        self.hint = hint
        self.rucio = rucio
        self.tables = tables
        self.t = transformation
        self.now = now
        self.clone_scope = hint.clone_scope or hint.scope
        self._parents: dict[tuple[str, str], str | None] = {}
        self._complete: dict[str, set[str]] = {}
        self._states: dict[str, str] = {}

    def did(self, name: str) -> dict[str, str]:
        return {"scope": self.hint.scope, "name": name}

    def _parent(self, name: str, kind: str) -> str | None:
        if (name, kind) not in self._parents:
            self._parents[(name, kind)] = next(
                (
                    str(p["name"])
                    for p in self.rucio.list_parent_dids(self.hint.scope, name)
                    if p.get("type") == kind
                    and p.get("scope") == self.hint.scope
                    and CLONE_MARK not in str(p["name"])
                ),
                None,
            )
        return self._parents[(name, kind)]

    def dataset_of(self, lfn: str) -> str | None:
        return self._parent(lfn, "DATASET")

    def container_of(self, dataset: str) -> str | None:
        return self._parent(dataset, "CONTAINER")

    def complete_at(self, dataset: str) -> set[str]:
        """The RSEs holding every file of a dataset (one call per dataset)."""
        if dataset not in self._complete:
            self._complete[dataset] = {
                str(r["rse"])
                for r in self.rucio.list_dataset_replicas(self.hint.scope, dataset)
                if r.get("state") == "AVAILABLE"
            }
        return self._complete[dataset]

    def _used(self, rse: str) -> tuple[int, int]:
        rows = [d for d in self.tables.datasets(self.t.id) if d.rse == rse]
        return sum(d.files for d in rows), sum(d.bytes for d in rows)

    def _choose(self, dataset: str, exclude: set[str]) -> tuple[str, int, int] | None:
        files = list(self.rucio.list_files(self.hint.scope, dataset))
        n, size = len(files), sum(int(f.get("bytes") or 0) for f in files)
        candidates = [d for d in self.hint.destinations if d not in exclude]
        holders = self.complete_at(dataset)
        for rse in candidates:
            if rse in holders:
                return rse, n, size  # already there: the rule only pins it
        for rse in candidates:
            used_files, used_bytes = self._used(rse)
            if (
                self.hint.max_files_per_rse is None
                or used_files + n <= self.hint.max_files_per_rse
            ) and (
                self.hint.max_bytes_per_rse is None
                or used_bytes + size <= self.hint.max_bytes_per_rse
            ):
                return rse, n, size
        return None

    def _hold(self, container: str, dataset: str, rse: str) -> tuple[str, str]:
        """Attach ``dataset`` to the clone of ``container`` for ``rse``; its rule."""
        clone = clone_name(container, self.t, rse)
        try:
            self.rucio.get_did(self.clone_scope, clone)
        except Exception:  # noqa: BLE001 (DataIdentifierNotFound)
            self.rucio.add_container(self.clone_scope, clone)
        rule_id = ensure_rule(
            self.rucio,
            self.tables,
            self.t,
            "Input",
            scope=self.clone_scope,
            name=clone,
            rse_expression=rse,
            lifetime=int(self.hint.lifetime_days * 86400),
            activity=self.hint.activity,
            now=self.now,
        )
        attached = {
            str(c["name"]) for c in self.rucio.list_content(self.clone_scope, clone)
        }
        if dataset not in attached:
            self.rucio.attach_dids(self.clone_scope, clone, [self.did(dataset)])
        return clone, rule_id

    def placement(self, dataset: str, container: str) -> InputDatasetRow | None:
        """Where a dataset is held for the transformation, choosing it if needed."""
        row = self.tables.dataset(self.t.id, self.hint.scope, dataset)
        if row is not None:
            return row
        choice = self._choose(dataset, set())
        if choice is None:
            return None
        rse, n, size = choice
        clone, rule_id = self._hold(container, dataset, rse)
        row = InputDatasetRow(
            transformation_id=self.t.id,
            scope=self.hint.scope,
            dataset=dataset,
            container=container,
            clone_scope=self.clone_scope,
            clone=clone,
            rse=rse,
            rule_id=rule_id,
            files=n,
            bytes=size,
            since=self.now,
            tried=(rse,),
        )
        self.tables.put_dataset(row)
        return row

    def state(self, row: InputDatasetRow) -> str:
        if row.rule_id not in self._states:
            rule = self.rucio.get_replication_rule(row.rule_id)
            self._states[row.rule_id] = str(rule["state"])
        return self._states[row.rule_id]

    def reroute(self, row: InputDatasetRow) -> InputDatasetRow | None:
        """Move the dataset to the clone of the next candidate, if any."""
        if row.reroutes >= self.hint.max_reroutes:
            return None
        choice = self._choose(row.dataset, set(row.tried))
        if choice is None:
            return None
        rse = choice[0]
        self.rucio.detach_dids(row.clone_scope, row.clone, [self.did(row.dataset)])
        clone, rule_id = self._hold(row.container, row.dataset, rse)
        new = replace(
            row,
            clone=clone,
            rse=rse,
            rule_id=rule_id,
            since=self.now,
            reroutes=row.reroutes + 1,
            tried=(*row.tried, rse),
        )
        self.tables.put_dataset(new)
        return new


# ---------------------------------------------------------------------------
# RucioInputRules packer
# ---------------------------------------------------------------------------


def rucio_input_rules_packer(
    *,
    packer_args: RucioInputRule,
    inputs: Iterable[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[Status, int],
    # remaining arguments are injected
    rucio: RucioClient,
    tables: RucioTables,
    clock: Clock,
    transformation: Transformation,
    inner: InnerPacker,
) -> Generator[PackerYield, None, None]:
    """Pass an input to the wrapped packer only once its dataset is at the job's site."""
    now = clock.now()
    plan = _Planner(packer_args, rucio, tables, transformation, now)
    max_wait = timedelta(hours=packer_args.max_wait_hours)

    # 1-3: the dataset and container of every file, and where it is held
    held: dict[str, list[PackerInput]] = {}
    for inp in inputs:
        if inp.lfn is None:
            yield BadInput(inp.id, reason="No file to hold with Rucio")
            continue
        dataset = plan.dataset_of(inp.lfn)
        if dataset is None:
            yield BadInput(inp.id, reason=f"{inp.lfn} is in no Rucio dataset")
            continue
        container = plan.container_of(dataset)
        if container is None:
            yield BadInput(inp.id, reason=f"{dataset} is in no Rucio container")
            continue
        if plan.placement(dataset, container) is None:
            yield BadInput(inp.id, reason=f"No destination left for {dataset}")
            continue
        held.setdefault(dataset, []).append(inp)

    # 4-5: dataset by dataset
    ready: list[PackerInput] = []
    rses: dict[str, str] = {}
    delayed: list[UUID] = []
    waiting_sites: set[str] = set()
    for dataset, group in held.items():
        row = tables.dataset(transformation.id, packer_args.scope, dataset)
        assert row is not None
        if row.rse in plan.complete_at(dataset):
            for inp in group:
                assert inp.lfn is not None
                ready.append(inp)
                rses[inp.lfn] = row.rse
            continue
        state = plan.state(row)
        # OK while incomplete: attached after the rule was evaluated, or read
        # before the replicas were; either way, worth waiting on
        if state not in {*WAITING_STATES, "OK"} or now - row.since > max_wait:
            # STUCK (or any state that will not resolve itself), or too slow
            moved = plan.reroute(row)
            if moved is None:
                for inp in group:
                    yield BadInput(
                        inp.id,
                        reason=f"Rucio rule {row.rule_id} at {row.rse} is {state}",
                    )
                continue
            row = moved
        delayed.extend(i.id for i in group)
        waiting_sites.add(site_of(row.rse))

    if delayed:
        yield DelayInput(delayed, now + timedelta(minutes=packer_args.poll_minutes))

    # Pack each destination site on its own, so that a partial group is held
    # back only while a transfer to that same site may still complete it
    sites = {lfn: site_of(rse) for lfn, rse in rses.items()}
    by_site: dict[str, list[PackerInput]] = {}
    for inp in ready:
        assert inp.lfn is not None
        by_site.setdefault(sites[inp.lfn], []).append(inp)
    for site, site_inputs in by_site.items():
        lfn_of = {i.id: i.lfn for i in site_inputs}
        for item in inner(
            inputs=iter(site_inputs),
            flush=flush,
            feeder_active=feeder_active or site in waiting_sites,
            input_overview=input_overview,
            replicas=_HeldAt(sites),
        ):
            if isinstance(item, ComputeParcel):
                held_at = {
                    lfn: rses[lfn] for i in item.inputs if (lfn := lfn_of.get(i))
                }
                item = replace(item, metadata={**item.metadata, "input_rses": held_at})
            yield item


# ---------------------------------------------------------------------------
# ReleaseRucioInputRules action (Finalizing)
# ---------------------------------------------------------------------------


class ReleaseRucioInputRulesArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grace_hours: float = Field(24, gt=0)


def release_rucio_input_rules(
    *,
    args: ReleaseRucioInputRulesArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    tables: RucioTables,
    clock: Clock,
    transformation: Transformation,
) -> ActionResult:
    """Let Rucio expire the transformation's input rules ``grace_hours`` from now."""
    grace = int(args.grace_hours * 3600)
    rows = [
        r
        for r in tables.rules(transformation.id)
        if r.kind == "Input" and r.released_at is None
    ]
    for row in rows:
        release_rule(rucio, tables, row, grace=grace, now=clock.now())
    return ActionResult("Done", message=f"{len(rows)} input rules expire in {grace}s")


PACKERS: dict[str, Any] = {"RucioInputRules": rucio_input_rules_packer}
ACTIONS: dict[str, Any] = {"ReleaseRucioInputRules": release_rucio_input_rules}
