#!/usr/bin/env cwl-runner
# A CMS analysis workgraph whose input datasets are held with Rucio at the
# job's site before any parcel is created (DX-ADR-010).
#
# Same document as crab-analysis.cwl, plus a dirac:RucioInputRule next to the
# dirac:Feeder of the driving input. The core then runs the step's packer
# (CRABSplitter here; any packer works) inside RucioInputRules, which:
#   - attaches each input dataset (CMS block) to a clone of its container,
#     named after the transformation and the destination, which carries one
#     rule at that destination: the first one that already holds the block,
#     or else the first one under the per-RSE thresholds;
#   - passes a dataset's files on only once the whole dataset is at the
#     destination, so a parcel runs where its data is; delays them otherwise;
#   - moves a dataset whose rule is STUCK or too slow to the next destination.
# The RucioRuleKeeper Active hook renews the rules while inputs need them;
# ReleaseRucioInputRules lets them expire at Finalizing.
cwlVersion: v1.2
class: Workflow
id: rucio_input_rules
label: CMS analysis with Rucio input rules
doc: |-
  Run cmsRun over a DBS dataset, filtered by a lumi mask, in jobs of a fixed
  number of lumis; every input block is held by a Rucio rule at the site its
  jobs run at, and a job is only created once its block is there.

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
    doc: Evaluated by the CRABDBS feeder; held by Rucio input rules
    dirac:Feeder:
      name: CRABDBS
      args:
        dataset: /Muon/Run2024A-PromptReco-v1/MINIAOD
        lumi_mask: {'1': [[1, 3]], '2': [[2, 6]], '3': [[1, 3]], '4': [[1, 1]]}
        unit: lumis
        max_lumis_per_input: 3
    dirac:RucioInputRule:
      destinations: [T2_IT_Legnaro, T2_IT_Pisa, T1_IT_CNAF_Disk]
      scope: cms
      clone_scope: user.cmsuser
      activity: Analysis Input
      max_bytes_per_rse: 2000000
      lifetime_days: 14
      poll_minutes: 15
      max_wait_hours: 48
      max_reroutes: 1
    type: {type: array, items: [File, string]}
    default: []
  output-prefix:
    doc: Injected per parcel at dispatch (<prefix>_<parcel id>)
    type: string
    default: rucio_input_rules

steps:
  analysis:
    label: Analysis
    doc: cmsRun over the masked lumis, three lumis per job, where the data is held
    hints:
      dirac:Transformation:
        packer:
          name: CRABSplitter
          args:
            algorithm: LumiBased
            units_per_job: 3
        hooks:
          HandleFailedInput: {hook: CRABHandleFailedInput, args: {max_retries: 2}}
          Active: {hook: RucioRuleKeeper, args: {renew_before_hours: 72, extend_days: 14}}
        requirements_template: {priority: 10}
        actions:
          Finalizing:
          - {action: NoInputProcessedTwice}
          - {action: ReleaseRucioInputRules, args: {grace_hours: 24}}
          Cleaning:
          - {action: RemoveRucioRules}
    run: transformations/cmsrun.cwl
    in:
      input-data: input-data
      output-prefix: output-prefix
    out: [output-files, status-report]

outputs:
  output-files:
    label: The workgraph's output files
    type: File[]
    outputSource: analysis/output-files
