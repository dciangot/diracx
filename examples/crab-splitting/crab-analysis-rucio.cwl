#!/usr/bin/env cwl-runner
# A CRAB Analysis task whose input is locked with Rucio at the job's site
# before any job is created (DX-ADR-005/006/008 + Rucio).
#
# Same document as crab-analysis.cwl except for the packer:
#   CRABRucioSplitter = CRABSplitter + "lock each file at a destination RSE with
#   a Rucio rule, delay the input until the rule is OK, then pack it for the
#   destination's site". The locks are released by a Finalizing action.
#
# CRAB / TaskWorker                 ->  where it lives here
#   Site.whitelist                  ->  packer args.destinations (disk RSEs, in order)
#   tape recall rule (TAPERECALL)   ->  the same rule: a tape-only file is recalled
#                                       to the destination by the lock itself
#   rule lifetime                   ->  packer args.lock_lifetime_days
#   Rucio quota of the lock account ->  packer args.destination_quota_bytes
#   end of task: rules expire       ->  Finalizing action CRABReleaseInputLocks
cwlVersion: v1.2
class: Workflow
id: crab_analysis_rucio
label: CRAB analysis task with Rucio input locks
doc: |-
  Run cmsRun over a DBS dataset, filtered by a lumi mask, in jobs of a fixed
  number of lumis; every file is locked at the site its job runs at, and a job
  is only created once all of its files are there.

$namespaces:
  dirac: https://diracgrid.org/cwl#

requirements:
  InlineJavascriptRequirement: {}

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: CRABAnalysis
    output_sandbox: ['*.log', 'FrameworkJobReport*.xml']

inputs:
  input-data:
    doc: Evaluated by the CRABDBS feeder (CRAB DataDiscovery)
    dirac:Feeder:
      name: CRABDBS
      args:
        dataset: /Muon/Run2024A-PromptReco-v1/MINIAOD
        lumi_mask: {'1': [[1, 3]], '2': [[2, 6]], '3': [[1, 3]], '4': [[1, 1]]}
        unit: lumis
        max_lumis_per_input: 3
    type: {type: array, items: [File, string]}
    default: []
  output-prefix:
    doc: Injected per parcel at dispatch in production
    type: string
    default: crab_analysis_rucio

steps:
  analysis:
    label: Analysis
    doc: cmsRun over the masked lumis, three lumis per job, on locked input
    hints:
      dirac:Transformation:
        packer:
          name: CRABRucioSplitter
          args:
            algorithm: LumiBased
            units_per_job: 3
            destinations: [T2_IT_Legnaro, T2_IT_Pisa, T1_IT_CNAF_Disk]
            destination_quota_bytes: 2000000
            comment: CRAB input lock crab_analysis_rucio
            lock_lifetime_days: 14
            poll_minutes: 15
            max_wait_hours: 48
            max_reroutes: 1
        hooks:
          HandleFailedInput: {hook: CRABHandleFailedInput, args: {max_retries: 2}}
        requirements_template: {priority: 10}
        actions:
          Finalizing:
          - {action: NoInputProcessedTwice}
          - {action: CRABReleaseInputLocks, args: {grace_hours: 24}}
    run: transformations/cmsrun.cwl
    in:
      input-data: input-data
      output-prefix: output-prefix
    out: [output-files, status-report]

outputs:
  output-files:
    label: The task's output files
    type: File[]
    outputSource: analysis/output-files
