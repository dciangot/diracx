#!/usr/bin/env cwl-runner
# A CRAB Analysis task whose outputs are registered in Rucio, merged at the
# site they were written at, and transferred to the user's storage site
# (DX-ADR-003/006/007/008 + Rucio).
#
# Same analysis step as crab-analysis-rucio.cwl, followed by a merge step fed
# by the core's edge feeder with the analysis outputs:
#
#   analysis (CRABRucioSplitter)          merge (CRABMerge)
#     -> unmerged files at <site>_Temp      -> merged files at <site>_Temp
#        /store/temp/user/..., pinned          /store/user/..., in the container
#        (intermediate: not an output)         that dirac:RucioRule moves to
#                                              the destination
#
# CRAB / TaskWorker / ASO               ->  where it lives here
#   job stage-out to the site's Temp    ->  the job's output, registered at
#                                           Completing by the Rucio catalogue
#   Rucio ASO: add_replicas + attach    ->  RucioOutputCatalogue (crab_outputs.py)
#   Site.storageSite                    ->  dirac:RucioRule.rse_expression
#   Data.outputDatasetTag               ->  dirac:RucioRule.container
#   WMCore merge task (merge by size)   ->  merge step, packer CRABMerge
#   task COMPLETED once ASO is done     ->  Finalizing action CRABWaitForTransfers
#   /store/temp cleanup                 ->  Finalizing action CRABReleaseIntermediates
cwlVersion: v1.2
class: Workflow
id: crab_analysis_merge
label: CRAB analysis task with merged outputs transferred by Rucio
doc: |-
  Run cmsRun over a DBS dataset on input locked at the job's site, merge the
  outputs by size where they were produced, and replicate the merged files
  to the storage site with one Rucio rule on the output container.

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
    doc: Injected per parcel at dispatch (<prefix>_<parcel id>)
    type: string
    default: crab_analysis
  merged-prefix:
    doc: Injected per parcel at dispatch (<prefix>_<parcel id>)
    type: string
    default: crab_merged

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
            comment: CRAB input lock crab_analysis_merge
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

  merge:
    label: Merge
    doc: Merge the analysis outputs by size, at the site they were written at
    hints:
      dirac:Transformation:
        packer:
          name: CRABMerge
          args:
            # bytes; tiny so that the mock outputs make several merge jobs
            min_merge_size: 100
            max_merge_size: 200
            max_merge_files: 3
        requirements_template: {priority: 20}
        actions:
          Finalizing:
          - {action: CRABWaitForTransfers}
          - {action: CRABReleaseIntermediates, args: {grace_hours: 24}}
    run: transformations/merge.cwl
    in:
      input-data: analysis/output-files
      output-prefix: merged-prefix
    out: [merged, status-report]

outputs:
  merged:
    label: The task's merged output files
    type: File[]
    outputSource: merge/merged
    # Proposal for the DX-ADR-008 open issue "Rucio rules for workgraph outputs"
    dirac:RucioRule:
      rse_expression: T2_DE_DESY
      container: /Muon/crabuser-Run2024A-crab_analysis_merge/USER
      lifetime_days: 180
      activity: User Subscriptions
