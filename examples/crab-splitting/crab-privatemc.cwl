#!/usr/bin/env cwl-runner
# A CRAB PrivateMC task as a DiracX workgraph (DX-ADR-007).
#   Data.totalUnits / unitsPerJob ->  dirac:Feeder CRABPrivateMC args
#   JobType.eventsPerLumi         ->  dirac:Feeder CRABPrivateMC args.events_per_lumi
#   Data.splitting: EventBased    ->  dirac:Transformation packer CRABSplitter
cwlVersion: v1.2
class: Workflow
id: crab_privatemc
label: CRAB PrivateMC task
doc: Generate events in jobs of a fixed number of events, one event range per job.

$namespaces:
  dirac: https://diracgrid.org/cwl#

requirements:
  InlineJavascriptRequirement: {}

hints:
  dirac:Workgraph:
    schema_version: '1.0'
    type: CRABPrivateMC
    output_sandbox: ['*.log', 'FrameworkJobReport*.xml']

inputs:
  events:
    doc: Event ranges, issued by the CRABPrivateMC feeder
    dirac:Feeder:
      name: CRABPrivateMC
      args: {total_events: 1050, events_per_job: 500, events_per_lumi: 100}
    type: {type: array, items: [File, string]}
    default: []
  output-prefix:
    type: string
    default: crab_privatemc

steps:
  generation:
    label: Generation
    hints:
      dirac:Transformation:
        packer: {name: CRABSplitter, args: {algorithm: EventBased, units_per_job: 500}}
    run: transformations/cmsrun.cwl
    in:
      input-data: events
      output-prefix: output-prefix
    out: [output-files, status-report]

outputs:
  output-files:
    type: File[]
    outputSource: generation/output-files
