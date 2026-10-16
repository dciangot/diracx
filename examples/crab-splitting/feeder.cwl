#!/usr/bin/env cwl-runner
# Feeder: CRAB DataDiscovery post-processing (run range / lumi mask filtering,
# per-file statistics). Produces the feed consumed by packer.cwl.
cwlVersion: v1.2
class: CommandLineTool
label: CRAB-like input feeder
doc: |
  Filters and normalises the discovered input files (LFN, events, runs/lumis,
  locations) into a feed document. For PrivateMC no input files are needed.

requirements:
  InlineJavascriptRequirement: {}
hints:
  DockerRequirement:
    dockerPull: python:3.11-slim

baseCommand: [python3]
arguments:
  - position: 0
    valueFrom: $(inputs.script.path)
  - position: 1
    valueFrom: feed
  - position: 10
    prefix: --output
    valueFrom: feed.json

inputs:
  script:
    type: File
    default:
      class: File
      location: crab_splitting.py
  input_files:
    type: File?
    doc: JSON list of discovered input files (DBS/Rucio dump)
    inputBinding: {position: 2, prefix: --input-files}
  job_type:
    type:
      type: enum
      symbols: [Analysis, PrivateMC]
    default: Analysis
    inputBinding: {position: 3, prefix: --job-type}
  lumi_mask:
    type: File?
    doc: CMS compact JSON lumi mask (Data.lumiMask)
    inputBinding: {position: 4, prefix: --lumi-mask}
  run_range:
    type: string?
    doc: Data.runRange, e.g. "1-10,15"
    inputBinding: {position: 5, prefix: --run-range}
  total_events:
    type: int?
    doc: PrivateMC only, total number of events to generate
    inputBinding: {position: 6, prefix: --total-events}
  events_per_lumi:
    type: int?
    doc: PrivateMC only
    inputBinding: {position: 7, prefix: --events-per-lumi}

outputs:
  feed:
    type: File
    outputBinding:
      glob: feed.json
