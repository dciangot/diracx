#!/usr/bin/env cwl-runner
# A CMS analysis workgraph whose outputs are registered in Rucio, merged at
# the site they were written at, and transferred to their destination by one
# Rucio rule (DX-ADR-010).
#
# Same analysis step as rucio-input-rules.cwl, followed by a merge step fed by
# the core's edge feeder with the analysis outputs:
#
#   analysis (CRABSplitter in              merge (RucioMergeBySize)
#   RucioInputRules)                          -> merged files at <site>_Temp,
#     -> unmerged files at <site>_Temp,          /store/user/..., attached to the
#        /store/temp/user/..., no rule           container that dirac:RucioRule
#        (intermediate: not an output)           moves to the destination
#
# The output rule waits until half of the merge inputs are processed
# (min_completion); WaitForRucioTransfers creates it at the latest, then
# keeps the merge transformation Finalizing until the data is there.
cwlVersion: v1.2
class: Workflow
id: rucio_merge_transfer
label: CMS analysis with merged outputs transferred by Rucio
doc: |-
  Run cmsRun over a DBS dataset held at the jobs' sites by Rucio input rules,
  merge the outputs by size where they were produced, and replicate the
  merged files to the storage site with one Rucio rule on the output
  container.

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
      max_bytes_per_rse: 2000000
      max_reroutes: 1
    type: {type: array, items: [File, string]}
    default: []
  output-prefix:
    doc: Injected per parcel at dispatch (<prefix>_<parcel id>)
    type: string
    default: analysis
  merged-prefix:
    doc: Injected per parcel at dispatch (<prefix>_<parcel id>)
    type: string
    default: merged

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
          Active: {hook: RucioRuleKeeper}
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

  merge:
    label: Merge
    doc: Merge the analysis outputs by size, at the site they were written at
    hints:
      dirac:Transformation:
        packer:
          name: RucioMergeBySize
          args:
            # bytes; tiny so that the mock outputs make several merge jobs
            min_merge_size: 100
            max_merge_size: 200
            max_merge_files: 3
            scope: user.cmsuser
        hooks:
          Active: {hook: RucioRuleKeeper}
        requirements_template: {priority: 20}
        actions:
          Finalizing:
          - {action: WaitForRucioTransfers}
          Cleaning:
          - {action: RemoveRucioRules}
    run: transformations/merge.cwl
    in:
      input-data: analysis/output-files
      output-prefix: merged-prefix
    out: [merged, status-report]

outputs:
  merged:
    label: The workgraph's merged output files
    type: File[]
    outputSource: merge/merged
    dirac:RucioRule:
      rse_expression: T2_DE_DESY
      container: /Muon/cmsuser-Run2024A-rucio_merge_transfer/USER
      lifetime_days: 180
      activity: User Subscriptions
      min_completion: 0.5
