"""``RucioRuleKeeper``: the transformation's ``Active`` hook of DX-ADR-010.

DX-ADR-006 runs a transformation's ``Active`` hook periodically while the
transformation is ``Active``. This one keeps the transformation's Rucio
rules in step with its inputs, and proposes no operation:

- **Renew input rules.** A rule whose lifetime ends within
  ``renew_before_hours`` is extended to ``extend_days`` from now if at least
  one file of its clone is still needed by an unfinished input: an
  ``Unassigned`` or ``Failed`` one for the dataset's current rule, an
  ``Assigned`` one whose parcel runs at the rule's site for any rule
  (including one released by a re-route). Other rules are left to expire.
- **Create output rules at a threshold.** A container whose
  ``dirac:RucioRule`` sets ``min_completion`` gets its rule once the
  transformation's feeder is exhausted (every input is known) and that
  fraction of its inputs is ``Processed``.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from adr006 import Operation, Status, Transformation
from pydantic import BaseModel, ConfigDict, Field
from rucio_common import Clock, LiveInputs, RucioClient, RucioTables, site_of
from rucio_outputs import RucioRuleHint, create_output_rule


class RucioRuleKeeperArgs(BaseModel):
    model_config = ConfigDict(extra="forbid")

    renew_before_hours: float = Field(72, gt=0)
    extend_days: float = Field(14, gt=0)


def _expires(rule: dict[str, Any]) -> datetime | None:
    value = rule.get("expires_at")
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if isinstance(value, datetime) and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def rucio_rule_keeper_hook(
    *,
    transformation: Transformation,
    input_overview: dict[Status, int],
    args: RucioRuleKeeperArgs,
    # remaining arguments are injected
    rucio: RucioClient,
    tables: RucioTables,
    live: LiveInputs,
    clock: Clock,
) -> Operation | None:
    """Renew the input rules still needed; create the output rules now due."""
    now = clock.now()
    inputs = live.live()
    current = {d.rule_id: d for d in tables.datasets(transformation.id)}
    clones = {(d.clone_scope, d.clone) for d in current.values()}
    content = {
        clone: {str(c["name"]) for c in rucio.list_content(scope, clone)}
        for scope, clone in clones
    }
    renew_before = timedelta(hours=args.renew_before_hours)
    for rule in tables.rules(transformation.id):
        if rule.kind != "InputDataset" or (rule.scope, rule.name) not in clones:
            continue
        files = content[rule.name]
        site = site_of(rule.rse_expression)
        needed = any(
            i.lfn in files
            and (
                (i.status == "Assigned" and site in i.sites)
                or (i.status != "Assigned" and rule.rule_id in current)
            )
            for i in inputs
        )
        expires = _expires(rucio.get_replication_rule(rule.rule_id))
        if needed and expires is not None and expires - now < renew_before:
            rucio.update_replication_rule(
                rule.rule_id, {"lifetime": int(args.extend_days * 86400)}
            )

    # Completion is only meaningful once every input is known
    total = sum(input_overview.values())
    done = input_overview.get("Processed", 0) / total if total else 0.0
    for row in tables.outputs(transformation.id):
        if row.rule_id is None and not transformation.feeder_enabled:
            if done >= RucioRuleHint.model_validate(row.rule).min_completion:
                create_output_rule(rucio, tables, transformation, row, now)
    return None


HOOKS: dict[str, Any] = {"RucioRuleKeeper": rucio_rule_keeper_hook}
