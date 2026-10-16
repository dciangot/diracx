"""Local stand-ins for the Transformation System extension types of DX-ADR-006.

DX-ADR-006 (DIRACGrid/diracx#1042) fixes the *shape* of what feeders, packers
and hooks yield or return, and leaves the concrete Python API to
implementation time. Until DiracX ships those types, the CRAB plugins in
``crab_plugins.py`` are written against the definitions below, which mirror the
ADR one to one. Integrating the plugins into DiracX is then a matter of
replacing ``import adr006`` with the real module.

Plugins propose, the core writes: nothing here touches a database.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

JSONSerializable = Any

# ---------------------------------------------------------------------------
# Feeder proposals
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Input:
    """A unit of processable input that is not (only) a file: a seed, an event range."""

    descriptor: dict[str, JSONSerializable]


@dataclass(frozen=True)
class FileInput(Input):
    """A file, or a fraction of one selected by a mask in ``descriptor``.

    ``lfn_size`` is the size of the whole file, whatever the mask selects.
    """

    lfn: str
    lfn_size: int


@dataclass(frozen=True)
class SetFeederState:
    """The feeder's bookmark, committed with the inputs yielded before it."""

    state: dict[str, JSONSerializable]


@dataclass(frozen=True)
class NoMoreInputs:
    """Clears ``FeederEnabled``: the feeder's source is exhausted."""


FeederYield = Input | FileInput | SetFeederState | NoMoreInputs

# ---------------------------------------------------------------------------
# Packer inputs and proposals
# ---------------------------------------------------------------------------

Status = Literal[
    "Unassigned",
    "Assigned",
    "Processed",
    "Failed",
    "Problematic",
    "Split",
    "NotProcessed",
]


@dataclass(frozen=True)
class PackerInput:
    """A claimed ``Unassigned`` row of the ``Inputs`` table (DX-ADR-004)."""

    id: UUID
    lfn: str | None
    lfn_size: int | None
    descriptor: dict[str, JSONSerializable]
    error_count: int = 0


@dataclass(frozen=True)
class ComputeParcel:
    inputs: list[UUID]
    metadata: dict[str, JSONSerializable]
    # merged over the transformation's requirements template
    requirements: dict[str, JSONSerializable] | None
    # the CWL input object passed to the runner
    parameters: dict[str, JSONSerializable] = field(default_factory=dict)


@dataclass(frozen=True)
class BadInput:
    input_id: UUID
    reason: str = "Unknown reason"


@dataclass(frozen=True)
class DelayInput:
    input_ids: list[UUID]
    delay_until: datetime


PackerYield = ComputeParcel | BadInput | DelayInput

# ---------------------------------------------------------------------------
# HandleFailedInput decisions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ToUnassigned:
    pass


@dataclass(frozen=True)
class ToProblematic:
    reason: str


@dataclass(frozen=True)
class SplitInto:
    children: list[Input | FileInput]


Decision = ToUnassigned | ToProblematic | SplitInto
