"""Lock CRAB inputs with Rucio and wait for them at the job's site before packing.

A second example on top of ``crab_plugins.py``. CRAB's TaskWorker does
something similar when it recalls a dataset from tape: it creates a Rucio rule
for the input at a chosen RSE, parks the task until the rule is ``OK`` and
only then splits it into jobs. In the Transformation System of
DIRACGrid/diracx#1042 that wait belongs to the packer, input by input:

- DX-ADR-005: "the packer asks for the delay, for example because a file is
  not yet at the storage element the transformation needs";
- DX-ADR-008: Rucio is *not* a data backend (a rule is a standing declaration,
  not a request that completes), data transformations are sinks, and "a
  transformation that needs files another one has replicated [...] its packer
  delays each input until the replica exists"; "a destination is a decision
  with memory [...] the assignment table belongs to its extension".

``CRABRucioSplitter`` packer
    For every claimed input the packer makes sure its file is *locked* at a
    destination RSE, and packs only inputs whose lock is satisfied:

    1. **Choose** a destination once per LFN and remember it in the extension's
       :class:`LockTable`, so every chunk of a file goes to the same place. A
       candidate that already has a replica is preferred (the rule just pins
       it); otherwise the candidates are filled in order up to
       ``destination_quota_bytes``.
    2. **Lock**: create a Rucio replication rule for the file at that RSE, with
       a lifetime, or adopt the one this task already created (found by its
       ``comment``, which makes the step idempotent if the packer task is
       retried between creating the rule and recording it).
    3. **Monitor**: read the rule state. ``OK`` means every lock of the rule is
       satisfied, i.e. the replica is available at the destination, so the
       input is handed to the ``CRABSplitter`` logic with the destination's site
       as its only location: the parcel's ``requirements.sites`` is where the
       data has been locked. ``REPLICATING`` (and ``SUSPENDED``, ``WAITING_APPROVAL``,
       ``INJECT``) yields a :class:`DelayInput` for ``poll_minutes``.
    4. **Re-route or give up**: a ``STUCK`` rule, or one not ``OK`` after
       ``max_wait_hours``, is released and the file is locked at the next
       candidate, up to ``max_reroutes`` times; then the input is a
       :class:`BadInput` and goes to ``Problematic`` for an operator.

    While an input is still on its way to a site, partial groups for that site
    are held back as if the feeder were still active, so that a parcel is not
    cut short by a transfer that is about to finish.

``CRABReleaseInputLocks`` action
    A ``Finalizing`` (or ``Cleaning``) action: shortens the lifetime of every
    rule the transformation holds to ``grace_hours``, so Rucio expires the
    locks once the task is over. Shortening rather than deleting leaves the
    replicas alone if something else still needs them.

The services the plugins use are injected (DX-ADR-001): :class:`RucioClient` is
the subset of ``rucio.client.Client`` they call, so a real client satisfies it
as is; :class:`LockTable` is the extension-owned table; :class:`Clock` is there
so the local harness can simulate time.
"""

from __future__ import annotations

from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any, Protocol

from adr006 import (
    ActionResult,
    BadInput,
    ComputeParcel,
    DelayInput,
    PackerInput,
    PackerYield,
    Status,
)
from crab_plugins import CRABSplitterArgs, SubmissionRefusedError, crab_splitter_packer
from pydantic import BaseModel, ConfigDict, Field, model_validator

#: Rule states (``rucio.db.sqla.constants.RuleState``) that are worth waiting on.
WAITING_STATES = {"REPLICATING", "SUSPENDED", "WAITING_APPROVAL", "INJECT"}

# ---------------------------------------------------------------------------
# Injected services
# ---------------------------------------------------------------------------


class RucioClient(Protocol):
    """The part of ``rucio.client.Client`` the plugins call."""

    def list_replicas(self, dids: list[dict[str, str]]) -> Iterable[dict[str, Any]]:
        """Replicas of each DID, with ``bytes`` and ``rses`` ({rse: [pfn]})."""
        ...

    def list_did_rules(self, scope: str, name: str) -> Iterable[dict[str, Any]]:
        """The replication rules on a DID."""
        ...

    def add_replication_rule(
        self,
        dids: list[dict[str, str]],
        copies: int,
        rse_expression: str,
        lifetime: int | None = None,
        activity: str | None = None,
        comment: str | None = None,
    ) -> list[str]:
        """Create one rule per DID and return their ids."""
        ...

    def get_replication_rule(self, rule_id: str) -> dict[str, Any]:
        """A rule, with its ``state``."""
        ...

    def update_replication_rule(self, rule_id: str, options: dict[str, Any]) -> bool:
        """Change a rule's options, e.g. ``{"lifetime": seconds}``."""
        ...


@dataclass(frozen=True)
class InputLock:
    """One row of the extension's lock table: where a file is held for the task."""

    lfn: str
    rse: str
    rule_id: str
    bytes: int
    since: datetime
    reroutes: int = 0


class LockTable(Protocol):
    """The extension-owned assignment table of DX-ADR-008, scoped to one transformation."""

    def get(self, lfn: str) -> InputLock | None:
        """The current lock of a file, if any."""
        ...

    def put(self, lock: InputLock) -> None:
        """Record (or replace) the lock of a file."""
        ...

    def all(self) -> list[InputLock]:
        """Every lock of the transformation."""
        ...


class Clock(Protocol):
    def now(self) -> datetime:
        """The current time (timezone aware)."""
        ...


def site_of(rse: str) -> str:
    """The CMS site of an RSE: ``T1_IT_CNAF_Disk`` -> ``T1_IT_CNAF``."""
    for suffix in ("_Disk", "_Tape", "_Test", "_Temp"):
        if rse.endswith(suffix):
            return rse.removesuffix(suffix)
    return rse


# ---------------------------------------------------------------------------
# CRABRucioSplitter packer
# ---------------------------------------------------------------------------


class CRABRucioSplitterArgs(CRABSplitterArgs):
    """``CRABSplitter`` args plus where and how to lock the input."""

    model_config = ConfigDict(extra="forbid")

    destinations: list[str] = Field(
        min_length=1, description="Candidate disk RSEs, in order (Site.whitelist)"
    )
    scope: str = "cms"
    activity: str = "Analysis Input"
    comment: str = Field(
        "CRAB input lock",
        min_length=1,
        description="Identifies this task's rules; unique per transformation",
    )
    lock_lifetime_days: float = Field(14, gt=0)
    destination_quota_bytes: int | None = Field(None, gt=0)
    poll_minutes: float = Field(15, gt=0)
    max_wait_hours: float = Field(72, gt=0)
    max_reroutes: int = Field(1, ge=0)

    @model_validator(mode="after")
    def _check_inputs(self) -> CRABRucioSplitterArgs:
        if self.algorithm == "EventBased":
            raise SubmissionRefusedError("EventBased tasks have no input to lock")
        return self


class _Locker:
    """Choose, create and read the locks of one packer invocation."""

    def __init__(
        self,
        args: CRABRucioSplitterArgs,
        rucio: RucioClient,
        locks: LockTable,
        now: datetime,
    ) -> None:
        self.args = args
        self.rucio = rucio
        self.locks = locks
        self.now = now
        self._states: dict[str, str] = {}
        self._used: dict[str, int] = {}
        for lock in locks.all():
            self._used[lock.rse] = self._used.get(lock.rse, 0) + lock.bytes

    def _did(self, lfn: str) -> dict[str, str]:
        return {"scope": self.args.scope, "name": lfn}

    def _replica(self, lfn: str) -> dict[str, Any] | None:
        return next(iter(self.rucio.list_replicas([self._did(lfn)])), None)

    def _choose(self, lfn: str, size: int, exclude: set[str]) -> str | None:
        candidates = [d for d in self.args.destinations if d not in exclude]
        replica = self._replica(lfn) or {}
        for rse in candidates:
            if rse in (replica.get("rses") or {}):
                return rse  # already there: the rule only pins it
        quota = self.args.destination_quota_bytes
        for rse in candidates:
            if quota is None or self._used.get(rse, 0) + size <= quota:
                return rse
        return None

    def _rule(self, lfn: str, rse: str) -> str:
        lifetime = int(self.args.lock_lifetime_days * 86400)
        for rule in self.rucio.list_did_rules(self.args.scope, lfn):
            if (
                rule.get("rse_expression") == rse
                and rule.get("comment") == self.args.comment
            ):
                self.rucio.update_replication_rule(rule["id"], {"lifetime": lifetime})
                return str(rule["id"])
        return self.rucio.add_replication_rule(
            [self._did(lfn)],
            1,
            rse,
            lifetime=lifetime,
            activity=self.args.activity,
            comment=self.args.comment,
        )[0]

    def lock(
        self, lfn: str, size: int, *, previous: InputLock | None = None
    ) -> InputLock | None:
        """Lock a file at a new destination; ``None`` if there is nowhere to put it."""
        exclude = {previous.rse} if previous else set()
        replica = self._replica(lfn)
        if replica is None or not replica.get("rses"):
            return None
        size = int(replica.get("bytes") or size)
        rse = self._choose(lfn, size, exclude)
        if rse is None:
            return None
        if previous is not None:
            self.release(previous)
        lock = InputLock(
            lfn=lfn,
            rse=rse,
            rule_id=self._rule(lfn, rse),
            bytes=size,
            since=self.now,
            reroutes=previous.reroutes + 1 if previous else 0,
        )
        self.locks.put(lock)
        self._used[rse] = self._used.get(rse, 0) + size
        return lock

    def release(self, lock: InputLock) -> None:
        self.rucio.update_replication_rule(lock.rule_id, {"lifetime": 1})
        self._used[lock.rse] = self._used.get(lock.rse, 0) - lock.bytes
        self._states.pop(lock.rule_id, None)

    def state(self, lock: InputLock) -> str:
        if lock.rule_id not in self._states:
            rule = self.rucio.get_replication_rule(lock.rule_id)
            self._states[lock.rule_id] = str(rule["state"])
        return self._states[lock.rule_id]


class _LockedAt:
    """A replica view in which a ready file is only at its destination's site."""

    def __init__(self, sites: dict[str, str]) -> None:
        self.sites = sites

    def locations(self, lfn: str) -> list[str]:
        return [self.sites[lfn]] if lfn in self.sites else []


def crab_rucio_splitter_packer(
    *,
    packer_args: CRABRucioSplitterArgs,
    inputs: Iterable[PackerInput],
    flush: bool = False,
    feeder_active: bool,
    input_overview: dict[Status, int],
    # remaining arguments are injected
    rucio: RucioClient,
    locks: LockTable,
    clock: Clock,
) -> Generator[PackerYield, None, None]:
    """Pack only inputs whose file is locked and available at the job's site."""
    now = clock.now()
    locker = _Locker(packer_args, rucio, locks, now)
    max_wait = timedelta(hours=packer_args.max_wait_hours)
    ready: list[PackerInput] = []
    rses: dict[str, str] = {}
    delayed = []
    waiting_sites: set[str] = set()

    for inp in inputs:
        if inp.lfn is None:
            yield BadInput(inp.id, reason="No file to lock")
            continue
        lock = locks.get(inp.lfn)
        if lock is None:
            lock = locker.lock(inp.lfn, inp.lfn_size or 0)
            if lock is None:
                yield BadInput(
                    inp.id, reason="No replica, or no destination with quota left"
                )
                continue
        state = locker.state(lock)
        if state == "OK":
            ready.append(inp)
            rses[inp.lfn] = lock.rse
            continue
        if state not in WAITING_STATES or now - lock.since > max_wait:
            # STUCK (or any state that will not resolve itself), or too slow
            moved = (
                locker.lock(inp.lfn, lock.bytes, previous=lock)
                if lock.reroutes < packer_args.max_reroutes
                else None
            )
            if moved is None:
                yield BadInput(
                    inp.id,
                    reason=f"Rucio rule {lock.rule_id} at {lock.rse} is {state}",
                )
                continue
            lock = moved
        delayed.append(inp.id)
        waiting_sites.add(site_of(lock.rse))

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
        yield from _pack_site(
            packer_args,
            site_inputs,
            flush=flush,
            feeder_active=feeder_active or site in waiting_sites,
            input_overview=input_overview,
            sites=sites,
            rses=rses,
        )


def _pack_site(
    packer_args: CRABRucioSplitterArgs,
    inputs: list[PackerInput],
    *,
    flush: bool,
    feeder_active: bool,
    input_overview: dict[Status, int],
    sites: dict[str, str],
    rses: dict[str, str],
) -> Iterator[PackerYield]:
    for item in crab_splitter_packer(
        packer_args=packer_args,
        inputs=iter(inputs),
        flush=flush,
        feeder_active=feeder_active,
        input_overview=input_overview,
        replicas=_LockedAt(sites),
    ):
        if isinstance(item, ComputeParcel):
            lfns = [p.removeprefix("lfn:") for p in item.parameters["input-data"]]
            item = replace(
                item,
                metadata={
                    **item.metadata,
                    "locks": {lfn: rses[lfn] for lfn in lfns},
                },
            )
        yield item


# ---------------------------------------------------------------------------
# CRABReleaseInputLocks action
# ---------------------------------------------------------------------------


class CRABReleaseInputLocksArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grace_hours: float = Field(24, gt=0)


def crab_release_input_locks(
    *,
    args: CRABReleaseInputLocksArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    locks: LockTable,
) -> ActionResult:
    """Let Rucio expire the transformation's locks ``grace_hours`` from now."""
    lifetime = int(args.grace_hours * 3600)
    rules = sorted({lock.rule_id for lock in locks.all()})
    for rule_id in rules:
        rucio.update_replication_rule(rule_id, {"lifetime": lifetime})
    return ActionResult(
        "Done", message=f"{len(rules)} input locks expire in {lifetime}s"
    )


PACKERS: dict[str, Any] = {"CRABRucioSplitter": crab_rucio_splitter_packer}
ACTIONS: dict[str, Any] = {"CRABReleaseInputLocks": crab_release_input_locks}
