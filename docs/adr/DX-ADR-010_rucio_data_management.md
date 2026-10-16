# DX-ADR-010: Rucio data management for workgraphs

## Metadata

- **Created By:** dciangot
- **Date:** 2026-10-16
- **Status:** Draft
- **Decision Maker(s):** TBD
- **Stakeholders:** Communities whose DiracX installation uses Rucio as its file catalogue (CMS first)

## Abstract

This ADR fills the open issue "Rucio rules for workgraph outputs" left by DX-ADR-008 and extends it to the input side. It builds on the Transformation System ADRs DX-ADR-002 to DX-ADR-009 proposed in [DIRACGrid/diracx#1042](https://github.com/DIRACGrid/diracx/pull/1042), and changes none of them.

A workgraph running in a Rucio installation can ask for two things:

- **Input locks.** Each input file is pinned with a Rucio rule at the storage element (RSE) of the site its job will run at, recalled from tape if need be. No parcel is created until the rule is satisfied. This is done by a packer, which delays each input while its rule replicates.
- **Output transfer.** Each output file is registered where the job wrote it. A final output is then moved to its destination by one rule, declared on the workgraph output with a new `dirac:RucioRule` hint field. An intermediate output is pinned where it is until its consumer has read it, for example a merge step that runs at the same site.

Rucio stays what DX-ADR-008 says it is: a system of standing declarations, not a data backend. No parcel is ever made of a rule. DiracX creates rules, records them in an extension table, polls their state from packers and actions, and shortens their lifetime when it is done with them. Everything specific to Rucio lives in extension plugins and in the installation's catalogue implementation; the core gains a catalogue interface and nothing else.

## Motivation

DX-ADR-008 makes data management transformations RMS-driven and leaves Rucio to the communities: "declaring in the workgraph document that an output should carry a Rucio rule [...] is wanted by installations that use Rucio. Who creates the rule, who owns its lifetime, and what a workgraph's cleaning does to it are open." CMS needs an answer before CRAB can run on DiracX. CRAB already uses Rucio on both sides of a job:

- **Input.** When a dataset is only on tape, or not at a site the user may run at, CRAB's TaskWorker creates a Rucio rule (`TAPERECALL`). The task waits until the rule is `OK` and is only then split into jobs, which run where the data is. Analysis on disk-resident data relies on Rucio having placed it at the job's site.
- **Output.** A CRAB job stages its output out to the Temp area of the site it ran at. CRAB's ASO ("asynchronous stage-out") registers the file in Rucio under its final LFN, attaches it to a dataset of the user's output container, and lets one rule on the container move everything to the user's storage site (`Site.storageSite`). Small outputs are merged first by WMCore's merge tasks.

Neither fits the ADRs as they stand:

- **A rule is not a parcel.** DX-ADR-008 rejects Rucio as a data backend because a rule is a standing declaration rather than a request that completes. Transfers and recalls therefore cannot be data transformations, and the DX-ADR-009 counters cannot see them.
- **Waiting on data has a single sanctioned place.** DX-ADR-008 says a transformation that needs files another one has replicated has "its packer delay each input until the replica exists" (DX-ADR-005, DX-ADR-006 `DelayInput`). Nothing says who creates the replica when the replication is a Rucio rule rather than another transformation.
- **Registration has no interface.** DX-ADR-003 has the core register outputs "into the installation's catalogue(s)" while a parcel is `Completing`, and DX-ADR-007 marks outputs that are not workgraph outputs as intermediate. Neither defines what a catalogue receives, so a Rucio catalogue cannot learn where a file is, whether it is intermediate, or where it should end up.
- **The end of a task is not the end of its jobs.** A CRAB task is complete when its outputs are at the destination, not when its last job ends. DX-ADR-006 has `Pending` Finalizing actions, but nothing ties them to the data.

A draft implementation of everything below, with a local stand-in for the core and a mock Rucio, is in `examples/crab-splitting/`: `crab_rucio.py` for the input side, `crab_outputs.py` for the output side, `crab-analysis-rucio.cwl` and `crab-analysis-merge.cwl` for the workgraphs.

## Specification

### Principles

1. **Rules are declarations, owned by the transformation that made them.** Every rule DiracX creates is recorded in the extension's `RucioRules` table against its transformation, with a kind (`InputLock`, `IntermediatePin`, `Output`). Lifetimes are what DiracX uses to release a rule; deleting is reserved for cleaning.
2. **Nothing waits in a parcel.** The input side waits by delaying inputs (DX-ADR-005 `DelayedUntil`), so a parcel is created only once its data is in place. The output side waits in a `Pending` Finalizing action. No parcel ever sits in `Assigned` for a rule.
3. **Every Rucio call can be repeated.** Plugins run as DX-ADR-001 tasks, which are retried, and `Completing` is re-entrant (DX-ADR-003). A rule is found again by its `comment` before one is created, a DID by `get_did`, and a replica or attachment that already exists is not an error.
4. **The core does not learn Rucio.** It calls a catalogue interface at `Completing` and passes on the hint fields it does not interpret. Choosing RSEs, creating rules and reading their state are the extension's.

### Services

The plugins receive their services by injection (DX-ADR-001, DX-ADR-006):

| Service           | What it is                                                                                                                                                                                                                                                                                           |
| ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RucioClient`     | The subset of `rucio.client.Client` the plugins call: `list_replicas`, `list_did_rules`, `add_replication_rule`, `get_replication_rule`, `update_replication_rule`, and for registration `add_replicas`, `get_did`, `add_dataset`, `add_container`, `attach_dids`. A real client satisfies it as is. |
| `RucioRules`      | The extension's table of rules, below.                                                                                                                                                                                                                                                               |
| `RucioInputLocks` | The extension's assignment table of DX-ADR-008 ("a destination is a decision with memory"), one row per locked file.                                                                                                                                                                                 |
| `OutputCatalogue` | The installation's catalogue, called by the core at `Completing`. A Rucio installation registers `RucioOutputCatalogue`.                                                                                                                                                                             |

The client is built for the Rucio account that owns the workgraph's rules: the submitting user's account for user analysis, as in CRAB, or a production account. Which one is an installation policy (see Open Issues).

```sql
CREATE TABLE RucioRules (                          -- extension-owned
    TransformationID BIGINT       NOT NULL,
    RuleID           CHAR(32)     NOT NULL,        -- Rucio rule id
    Kind             ENUM('InputLock', 'IntermediatePin', 'Output') NOT NULL,
    Scope            VARCHAR(64)  NOT NULL,
    Name             VARCHAR(512) NOT NULL,        -- file, dataset or container
    RSEExpression    VARCHAR(512) NOT NULL,
    CreatedAt        DATETIME     NOT NULL,
    ReleasedAt       DATETIME     NULL,            -- lifetime shortened
    PRIMARY KEY (TransformationID, RuleID),
    FOREIGN KEY (TransformationID) REFERENCES Transformations (TransformationID)
);

CREATE TABLE RucioInputLocks (                     -- extension-owned
    TransformationID BIGINT       NOT NULL,
    LFN              VARCHAR(512) NOT NULL,
    RSE              VARCHAR(64)  NOT NULL,
    RuleID           CHAR(32)     NOT NULL,
    Bytes            BIGINT       NOT NULL,
    Since            DATETIME     NOT NULL,
    Reroutes         INT          NOT NULL DEFAULT 0,
    PRIMARY KEY (TransformationID, LFN),
    FOREIGN KEY (TransformationID) REFERENCES Transformations (TransformationID)
);
```

### Input: lock, then pack

A locking packer wraps a community's ordinary packer (for CMS, the CRAB splitter). For each claimed `Unassigned` input it makes sure the input's file is locked at a destination, and passes the input on only once the lock is satisfied:

1. **Choose.** Pick a destination RSE once per LFN and record it in `RucioInputLocks`, so that every chunk of a file (DX-ADR-004 masks) goes to the same place. A candidate that already holds a replica is preferred, because the rule then only pins it. Otherwise candidates are filled in order up to a quota. The quota is read from the lock table, since rule bytes are outside the DX-ADR-009 counters.
2. **Lock.** Create a rule for the file at that RSE, with a lifetime and a `comment` unique to the transformation, or adopt the transformation's existing rule found by that comment. Record it in `RucioRules` as `InputLock`. A file only on tape is recalled by the lock itself; no separate recall step exists.
3. **Monitor.** Read the rule's state:
    - `OK`: the replica is at the destination. The input goes to the wrapped packer with that RSE's site as its only location, so the parcel's `requirements.sites` is where the data was locked.
    - `REPLICATING`, `SUSPENDED`, `WAITING_APPROVAL` or `INJECT`: yield `DelayInput` until `now + poll_minutes`.
4. **Re-route or give up.** A `STUCK` rule, or one not `OK` after `max_wait_hours`, is released, and the file is locked at the next candidate, up to `max_reroutes` times. After that the input is a `BadInput` and goes to `Problematic` for an operator (DX-ADR-005).

While any input is still on its way to a site, the packer holds back partial groups for that site as if its feeder were still active (DX-ADR-006), so a parcel is not cut short by a transfer about to finish. Locks are released by an action (below), not per parcel.

The packer's arguments, validated at submission by its model (DX-ADR-006):

| Argument                         | Meaning                                                           | CRAB equivalent                 |
| -------------------------------- | ----------------------------------------------------------------- | ------------------------------- |
| `destinations`                   | Candidate disk RSEs, in order of preference                       | `Site.whitelist`                |
| `destination_quota_bytes`        | Bytes this transformation may lock per RSE                        | Rucio quota of the lock account |
| `scope`, `activity`              | DID scope of the input, FTS activity of the rule                  | `cms`, `Analysis Input`         |
| `comment`                        | Identifies this transformation's rules; unique per transformation | the task name                   |
| `lock_lifetime_days`             | Lifetime of a lock while the transformation runs                  | rule lifetime                   |
| `poll_minutes`                   | Delay between two reads of a replicating rule                     | TaskWorker's recall polling     |
| `max_wait_hours`, `max_reroutes` | When a rule is given up, and how many other RSEs are tried        | —                               |

This is the DX-ADR-008 pattern "the consumer's packer waits on the replica", with the packer also creating the replica.

### Output: register, pin or transfer

#### Registration at `Completing`

The core registers a parcel's outputs at `Completing` (DX-ADR-003) through the installation's `OutputCatalogue`. This ADR fixes the interface:

```python
@dataclass(frozen=True)
class ProducedFile:
    workgraph: str
    step: str
    output: str  # the output port
    parcel_id: UUID
    location: str  # where the job wrapper uploaded the file (a site or an RSE)
    basename: str
    bytes: int
    adler32: str
    intermediate: bool  # DX-ADR-007 TransformationOutputs.Intermediate
    hints: dict[str, JSONSerializable]  # the dirac: fields of the workgraph output


class OutputCatalogue(Protocol):
    def register(self, files: list[ProducedFile]) -> list[str]:
        """Register one parcel's files and return their LFNs, in order. Idempotent."""
```

The returned LFNs are what the core writes as `ParcelOutputs` rows, so the catalogue owns the LFN policy. A parcel that fails registers nothing, as in DX-ADR-003. A catalogue that raises leaves the parcel in `Completing`, and the core calls it again.

`RucioOutputCatalogue` does the following for each file, at the RSE the file was uploaded to (the site's Temp RSE in CMS):

1. Compute the LFN. A workgraph output goes under the user or production area, e.g. `/store/user/<account>/<workgraph>/<step>/<output>/<basename>`. An intermediate goes under the temporary area, e.g. `/store/temp/user/...`. The per-parcel output prefix injected at scheduling (DX-ADR-007) makes basenames unique.
2. Call `add_replicas` at the RSE, with size and checksum.
3. Attach the file to the dataset `<container>#<rse>`, creating the dataset and attaching it to the container on first use. One dataset per RSE keeps a dataset's files in one place, which is what CRAB does.
4. For an intermediate, pin the dataset at its RSE with an `IntermediatePin` rule with a lifetime, so that the site's Temp cleanup does not remove the file before its consumer reads it.
5. For a workgraph output with a `dirac:RucioRule`, create the `Output` rule on the container, once. Files registered later are attached to the same container and are covered by the same rule.

#### `dirac:RucioRule`

A new field on a workgraph **output parameter**, added to the `dirac:` JSON schema (DX-ADR-007). It is validated at submission and passed to the catalogue untouched:

```yaml
outputs:
  merged:
    type: File[]
    outputSource: merge/merged
    dirac:RucioRule:
      rse_expression: T2_DE_DESY
      container: /Muon/alice-Run2024A-v1/USER
      lifetime_days: 180
      activity: User Subscriptions
```

| Field            | Required | Meaning                                                                    | CRAB equivalent         |
| ---------------- | -------- | -------------------------------------------------------------------------- | ----------------------- |
| `rse_expression` | yes      | Where the output must end up                                               | `Site.storageSite`      |
| `container`      | no       | The output container; defaults to one derived from workgraph, step, output | `Data.outputDatasetTag` |
| `lifetime_days`  | no       | Lifetime of the rule; none by default                                      | —                       |
| `activity`       | no       | FTS activity; installation default                                         | ASO's activity          |
| `copies`         | no       | Number of copies; 1 by default                                             | —                       |

The hint sits on the workgraph output, not on a step, because an output is what a workgraph promises to deliver. A step output that is not a workgraph output is intermediate (DX-ADR-007) and is never transferred, only pinned.

#### Merging is a transformation

Merging is an ordinary compute step, edge-fed from the producer's outputs (DX-ADR-006), as in the ADR's sprucing example. Its packer is location-aware:

- it reads each input's replicas from Rucio and groups inputs by RSE, never across RSEs;
- it sends the merge parcel to that RSE's site, so the unmerged files are read locally and only merged files travel;
- it cuts a group at a minimum size, a maximum size or a maximum file count (WMCore `ParentlessMergeBySize`), and packs the remainder once the producer has drained and its feeder is off;
- an input with no replica is a `BadInput`.

```mermaid
flowchart LR
    In[(input dataset)] -. "locking packer:<br/>rule to the job's site" .-> A[analysis]
    A -- "intermediate:<br/>registered at Temp, pinned" --> M[merge<br/>same site]
    M -- "workgraph output:<br/>registered at Temp" --> C[(output container)]
    C -. "dirac:RucioRule" .-> D[(destination RSE)]
```

Without a merge step, `dirac:RucioRule` goes on an output sourced directly from the producer, whose files are then registered as workgraph outputs and transferred unmerged.

### Actions

The rules are tied to the transformation lifecycle (DX-ADR-005) through actions (DX-ADR-006). Because members finalise upstream first, a consumer's actions run after its producer is `Finalized`.

| List         | Action                      | Result                                                                                                                                               |
| ------------ | --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Finalizing` | `ReleaseRucioInputLocks`    | `Done`: shortens every `InputLock` rule of the transformation to a grace period and sets `ReleasedAt`.                                               |
| `Finalizing` | `WaitForRucioTransfers`     | `Pending` while an `Output` rule replicates; `Failed` on `STUCK`, which moves the transformation to `FinalizingBlocked`; `Passed` once all are `OK`. |
| `Archiving`  | `ReleaseRucioIntermediates` | `Done`: shortens every `IntermediatePin` rule to a grace period. Rucio and the site's Temp cleanup then remove the files.                            |
| `Cleaning`   | `RemoveRucioRules`          | `Done`: deletes every rule of the transformation, including `Output` rules, so the outputs are removed as cleaning requires.                         |

`WaitForRucioTransfers` makes a workgraph `Completed` mean "the data is at its destination", which is what a CRAB user means by a completed task. An operator who accepts an incomplete transfer forces the action to passed, as for any failed action.

In the example in `examples/crab-splitting/` these actions are `CRABReleaseInputLocks`, `CRABWaitForTransfers` and `CRABReleaseIntermediates`. It releases intermediates at `Finalizing`, for brevity in a single run; this ADR places it in `Archiving`, where DX-ADR-006 removes intermediates.

### Accounting

Rule bytes stay outside the DX-ADR-009 counters, as DX-ADR-008 states. A locking packer budgets from `RucioInputLocks`; the authoritative limit is the Rucio account's quota at each RSE, which Rucio enforces when a rule is created. A rule refused for quota is handled as a `STUCK` rule: re-routed, then `BadInput`.

### Mapping from CRAB

| CRAB / TaskWorker / ASO                     | Here                                                  |
| ------------------------------------------- | ----------------------------------------------------- |
| Tape recall rule, wait for `OK`             | Locking packer, `InputLock` rule, `DelayInput`        |
| `Site.whitelist`                            | Locking packer `destinations`                         |
| Job stage-out to the site's Temp area       | The job's upload; `ProducedFile.location`             |
| ASO `add_replicas` and `attach_dids`        | `RucioOutputCatalogue.register` at `Completing`       |
| `Site.storageSite`, `Data.outputDatasetTag` | `dirac:RucioRule.rse_expression`, `container`         |
| WMCore merge task                           | Edge-fed merge step with a location-aware packer      |
| Task `COMPLETED` once ASO is done           | `WaitForRucioTransfers` at `Finalizing`               |
| Rule expiry, Temp cleanup                   | `ReleaseRucioInputLocks`, `ReleaseRucioIntermediates` |

## Rationale

- **Packer for inputs, because DX-ADR-008 already puts waiting there.** The ADRs make the packer the place where an input waits for a replica. Making it also create the rule keeps the decision of where data goes and where the job runs in one plugin, which is what DX-ADR-008 means by "the packing decision places the compute that follows it".
- **Delaying inputs rather than parcels.** A parcel created before its data is ready would sit in `Assigned` with nothing to tell its backend. Delayed inputs cost one `get_replication_rule` per file per sweep, but they keep every parcel dispatchable and let the packer re-route without cancelling anything.
- **Registration in the catalogue, because DX-ADR-003 already puts it there.** `Completing` is re-entrant and gates `Done`, so a file that is in `ParcelOutputs` is in Rucio, and a consumer's edge feeder never sees an unregistered file.
- **One rule per container.** A rule per file would be thousands of rules per task, each to poll and release. A container rule is created once, covers files registered later, and is what CRAB's ASO does.
- **The rule on the workgraph output.** Transfer is a property of what is delivered, not of the job that produced it. Placing it on the output also makes "intermediate" and "transferred" mutually exclusive by construction.
- **Lifetimes rather than deletion.** Shortening a lifetime leaves a replica in place if something else still needs it, and turns a forgotten rule into one that expires. Deletion is kept for cleaning, where removing the outputs is the point.
- **Waiting at `Finalizing`.** The DX-ADR-006 `Pending` result exists for exactly this: an answer not yet knowable that does not need a person.

## Rejected Ideas

- **Rucio as a data backend.** Rejected by DX-ADR-008 for the same reason: a rule does not complete. A transfer or recall parcel would need a completion synthesised from rule state.
- **Creating rules at submission.** The output container does not exist until a first file is registered, and input destinations depend on which files the feeder yields and on quotas at the time. Submission-time rules would also outlive a refused or cancelled workgraph.
- **A data transformation per transfer, through the RMS.** Duplicates what a Rucio rule already does, with a second retry loop above FTS, and is the double management DX-ADR-008 avoids.
- **Waiting at dispatch.** A dispatcher that holds an `Assigned` parcel until its files are at the site needs a notion of data readiness in the core and cannot re-route without cancelling.
- **A rule per output file.** Many rules to create, poll and release, with no gain over a container rule.
- **A core `dirac:` hint for input locks.** Where to lock, with what quota and when to give up are community policy. They stay arguments of the community's packer, validated by its own model.
- **Transferring intermediates.** An intermediate exists to be consumed where it was produced; moving it is the job of the step that consumes it.

## Open Issues

- **Side effects in a packer.** DX-ADR-006 says plugins propose and the core writes; creating a rule is an external write. It is repeatable here, and the tables are the extension's, but DX-ADR-006 could name "external reservations made by a packer" explicitly, or provide a hook that runs before packing.
- **Shared rules.** Rucio refuses a second rule for the same DID, account and RSE expression. Two transformations of one account locking the same file at the same RSE must share a rule, which needs reference counting before release, or a rule per transformation through distinct accounts.
- **Rule ownership and account.** The rule's account pays the quota at the destination. Whether it is the user's account, as in CRAB, or a service account acting for the user, and how DiracX obtains a Rucio token for it, is installation policy that DX-ADR-001 services must support.
- **Release per parcel.** Locks and pins are released per transformation. Releasing a file as soon as its inputs are `Processed`, or its consumer's parcel is `Done`, needs a hook on input or parcel completion that DX-ADR-006 does not define.
- **Upload location.** `ProducedFile.location` must come from the job wrapper's report of where each file was uploaded (DX-ADR-007 abandoned uploads), not from the parcel's requirements.
- **Polling cost.** One rule read per locked file per sweep. Rules per dataset (a CMS block) would reduce it, at the cost of coarser destinations.
- **Stuck output transfers.** `WaitForRucioTransfers` blocks the transformation on a `STUCK` rule. Whether it should first try another RSE expression, and who decides, is open.
- **Multiple catalogues.** DX-ADR-003 allows a VO to register into several catalogues. Whether `dirac:RucioRule` is ignored by the others, or `ProducedFile.hints` should be namespaced per catalogue, is open.
- **Schema.** `dirac:RucioRule` must be added to the published `dirac:` JSON schema (DX-ADR-007), with its defaults resolved through the configuration service like other hint fields.
