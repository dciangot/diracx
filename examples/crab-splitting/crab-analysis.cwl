#!/usr/bin/env cwl-runner
# A CRAB Analysis task as a DiracX workgraph (DX-ADR-007).
#
# CRAB client option          ->  where it lives here
#   Data.inputDataset         ->  dirac:Feeder CRABDBS args.dataset
#   Data.lumiMask / runRange  ->  dirac:Feeder CRABDBS args.lumi_mask / run_range
#   Data.totalUnits           ->  dirac:Feeder CRABDBS args.total_units (+ unit)
#   Data.splitting            ->  dirac:Transformation packer CRABSplitter args.algorithm
#   Data.unitsPerJob          ->  dirac:Transformation packer CRABSplitter args.units_per_job
#   General.transferLogs etc. ->  dirac:Workgraph output_sandbox
#   Site.whitelist, priority  ->  dirac:Transformation requirements_template
#   JobType.psetName          ->  the step's run body
cwlVersion: v1.2
class: Workflow
id: crab_analysis
label: CRAB analysis task
doc: |-
  Run cmsRun over a DBS dataset, filtered by a lumi mask, in jobs of a fixed
  number of lumis. The files are never listed in the document: the CRABDBS
  feeder evaluates the query.

$namespaces:
  dirac: https://diracgrid.org/cwl#

requirements:
  InlineJavascriptRequirement: {}

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: CRABAnalysis
    output_sandbox: ['*.log', 'FrameworkJobReport*.xml']
    approving:
    - {action: CheckSuccessRate, args: {min_success_rate: 0.9}}

inputs:
  input-data:
    doc: Evaluated by the CRABDBS feeder (CRAB DataDiscovery)
    dirac:Feeder:
      name: CRABDBS
      args:
        dataset: /Muon/Run2024A-PromptReco-v1/MINIAOD
        lumi_mask: {'1': [[1, 3]], '2': [[2, 6]], '3': [[1, 3]]}
        unit: lumis
        # upfront chunking, so that no input is larger than a job
        max_lumis_per_input: 3
        scouting_fraction: 0.5
    type: {type: array, items: [File, string]}
    default: []
  output-prefix:
    doc: Injected per parcel at dispatch in production
    type: string
    default: crab_analysis

steps:
  analysis:
    label: Analysis
    doc: cmsRun over the masked lumis, three lumis per job
    hints:
      dirac:Transformation:
        packer:
          name: CRABSplitter
          args: {algorithm: LumiBased, units_per_job: 3}
        hooks:
          HandleFailedInput: {hook: CRABHandleFailedInput, args: {max_retries: 2}}
        requirements_template: {priority: 10}
        actions:
          Finalizing:
          - {action: ReconcileFeederInputs}
          - {action: NoInputProcessedTwice}
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
