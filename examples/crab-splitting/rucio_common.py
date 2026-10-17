"""What the Rucio plugins of DX-ADR-010 share: services, tables and rule helpers.

Nothing here is CRAB specific. The plugins in ``rucio_inputs.py``,
``rucio_outputs.py`` and ``rucio_hooks.py`` receive these services by
injection (DX-ADR-001, DX-ADR-006):

- :class:`RucioClient`, the subset of ``rucio.client.Client`` they call; a
  real client satisfies it as is;
- :class:`RucioTables`, the extension-owned tables ``RucioRules``,
  ``RucioInputDatasets`` and ``RucioOutputContainers``;
- :class:`LiveInputs`, a read-only view of the transformation's inputs that
  are not over yet, which the lifetime hook needs;
- :class:`Clock`, so that tests and the local core can simulate time.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import datetime
from typing import Any, Literal, Protocol

from adr006 import Status, Transformation

#: Rule states (``rucio.db.sqla.constants.RuleState``) that are worth waiting on.
WAITING_STATES = {"REPLICATING", "SUSPENDED", "WAITING_APPROVAL", "INJECT"}

RuleKind = Literal["InputDataset", "Output"]


def site_of(rse: str) -> str:
    """The site of an RSE, by the CMS naming convention (``T2_IT_Pisa_Temp`` -> ``T2_IT_Pisa``)."""
    for suffix in ("_Disk", "_Tape", "_Temp", "_Test"):
        if rse.endswith(suffix):
            return rse.removesuffix(suffix)
    return rse


class Clock(Protocol):
    def now(self) -> datetime: ...


class RucioClient(Protocol):
    """The part of ``rucio.client.Client`` the plugins call."""

    def list_replicas(self, dids: list[dict[str, str]]) -> Iterable[dict[str, Any]]:
        """Replicas of each file DID, with ``bytes`` and ``rses`` ({rse: [pfn]})."""
        ...

    def list_parent_dids(self, scope: str, name: str) -> Iterable[dict[str, Any]]:
        """The datasets and containers a DID is attached to."""
        ...

    def list_dataset_replicas(self, scope: str, name: str) -> Iterable[dict[str, Any]]:
        """Per RSE, how much of a dataset is there; ``state`` is ``AVAILABLE`` if all."""
        ...

    def list_files(self, scope: str, name: str) -> Iterable[dict[str, Any]]:
        """The files of a dataset or container, with ``bytes``."""
        ...

    def list_content(self, scope: str, name: str) -> Iterable[dict[str, Any]]:
        """The DIDs directly attached to a dataset or container."""
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
        """A rule, with its ``state`` and ``expires_at``."""
        ...

    def update_replication_rule(self, rule_id: str, options: dict[str, Any]) -> bool:
        """Change a rule's options, e.g. ``{"lifetime": seconds}``."""
        ...

    def delete_replication_rule(self, rule_id: str) -> bool:
        """Delete a rule; Rucio's reaper then removes what nothing else protects."""
        ...

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


# ---------------------------------------------------------------------------
# Extension-owned tables
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuleRow:
    """One row of ``RucioRules``: a rule DiracX created, and who owns it."""

    transformation_id: int
    rule_id: str
    kind: RuleKind
    scope: str
    name: str
    rse_expression: str
    created_at: datetime
    released_at: datetime | None = None


@dataclass(frozen=True)
class InputDatasetRow:
    """One row of ``RucioInputDatasets``: where an input dataset is held, and how."""

    transformation_id: int
    scope: str
    dataset: str
    container: str  # the input container (the workflow input) it belongs to
    clone_scope: str
    clone: str
    rse: str
    rule_id: str
    files: int
    bytes: int
    since: datetime
    reroutes: int = 0
    tried: tuple[str, ...] = ()


@dataclass(frozen=True)
class OutputContainerRow:
    """One row of ``RucioOutputContainers``: a container whose rule may be pending."""

    transformation_id: int
    scope: str
    container: str
    rule: dict[str, Any]  # the dirac:RucioRule of the workgraph output
    rule_id: str | None = None


class RucioTables(Protocol):
    """The three extension tables, each scoped by ``TransformationID``."""

    def rules(self, transformation_id: int) -> list[RuleRow]: ...

    def put_rule(self, row: RuleRow) -> None: ...

    def delete_rule(self, transformation_id: int, rule_id: str) -> None: ...

    def dataset(
        self, transformation_id: int, scope: str, name: str
    ) -> InputDatasetRow | None: ...

    def datasets(self, transformation_id: int) -> list[InputDatasetRow]: ...

    def put_dataset(self, row: InputDatasetRow) -> None: ...

    def output(
        self, transformation_id: int, scope: str, container: str
    ) -> OutputContainerRow | None: ...

    def outputs(self, transformation_id: int) -> list[OutputContainerRow]: ...

    def put_output(self, row: OutputContainerRow) -> None: ...


@dataclass
class InMemoryRucioTables:
    """The tables of :class:`RucioTables` in memory (database tables in production)."""

    _rules: dict[tuple[int, str], RuleRow] = field(default_factory=dict)
    _datasets: dict[tuple[int, str, str], InputDatasetRow] = field(default_factory=dict)
    _outputs: dict[tuple[int, str, str], OutputContainerRow] = field(
        default_factory=dict
    )

    def rules(self, transformation_id: int) -> list[RuleRow]:
        return [r for (t, _), r in self._rules.items() if t == transformation_id]

    def all_rules(self) -> list[RuleRow]:
        return list(self._rules.values())

    def put_rule(self, row: RuleRow) -> None:
        self._rules[(row.transformation_id, row.rule_id)] = row

    def delete_rule(self, transformation_id: int, rule_id: str) -> None:
        self._rules.pop((transformation_id, rule_id), None)

    def dataset(
        self, transformation_id: int, scope: str, name: str
    ) -> InputDatasetRow | None:
        return self._datasets.get((transformation_id, scope, name))

    def datasets(self, transformation_id: int) -> list[InputDatasetRow]:
        return [d for (t, _, _), d in self._datasets.items() if t == transformation_id]

    def put_dataset(self, row: InputDatasetRow) -> None:
        self._datasets[(row.transformation_id, row.scope, row.dataset)] = row

    def output(
        self, transformation_id: int, scope: str, container: str
    ) -> OutputContainerRow | None:
        return self._outputs.get((transformation_id, scope, container))

    def outputs(self, transformation_id: int) -> list[OutputContainerRow]:
        return [o for (t, _, _), o in self._outputs.items() if t == transformation_id]

    def put_output(self, row: OutputContainerRow) -> None:
        self._outputs[(row.transformation_id, row.scope, row.container)] = row


# ---------------------------------------------------------------------------
# The live inputs of a transformation (read-only, provided by the core)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LiveInput:
    lfn: str | None
    status: Status
    # the sites of the parcel an Assigned input is in
    sites: tuple[str, ...] = ()


class LiveInputs(Protocol):
    def live(self) -> list[LiveInput]:
        """The ``Unassigned``, ``Assigned`` and ``Failed`` inputs."""
        ...


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------


def rule_comment(transformation: Transformation) -> str:
    return f"DiracX transformation {transformation.id}"


def ensure_rule(
    rucio: RucioClient,
    tables: RucioTables,
    transformation: Transformation,
    kind: RuleKind,
    *,
    scope: str,
    name: str,
    rse_expression: str,
    lifetime: int | None,
    activity: str,
    copies: int = 1,
    now: datetime,
) -> str:
    """The transformation's rule on a DID, adopted if it exists, created otherwise.

    A rule is found again by its DID and RSE expression, and by the comment
    naming the transformation, so that a retried task does not create a
    second one.
    """
    comment = rule_comment(transformation)
    rule_id = next(
        (
            str(r["id"])
            for r in rucio.list_did_rules(scope, name)
            if r.get("rse_expression") == rse_expression and r.get("comment") == comment
        ),
        None,
    )
    if rule_id is None:
        rule_id = rucio.add_replication_rule(
            [{"scope": scope, "name": name}],
            copies,
            rse_expression,
            lifetime=lifetime,
            activity=activity,
            comment=comment,
        )[0]
    if not any(r.rule_id == rule_id for r in tables.rules(transformation.id)):
        tables.put_rule(
            RuleRow(transformation.id, rule_id, kind, scope, name, rse_expression, now)
        )
    return rule_id


def release_rule(
    rucio: RucioClient, tables: RucioTables, row: RuleRow, *, grace: int, now: datetime
) -> None:
    """Let Rucio expire a rule ``grace`` seconds from now, and remember it."""
    rucio.update_replication_rule(row.rule_id, {"lifetime": grace})
    tables.put_rule(replace(row, released_at=now))
