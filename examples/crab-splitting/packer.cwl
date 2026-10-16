#!/usr/bin/env cwl-runner
# Packer: CRAB Splitter (WMCore JobSplitting) equivalent. Packs the feed into
# jobs and writes the splitting summary.
cwlVersion: v1.2
class: CommandLineTool
label: CRAB-like job packer
doc: |
  Packs files/lumis/events from the feed into jobs using one of the CRAB
  splitting algorithms (FileBased, LumiBased, EventAwareLumiBased, Automatic,
  EventBased). Exits with code 2 if the submission would be refused.

hints:
  DockerRequirement:
    dockerPull: python:3.11-slim

baseCommand: [python3]
arguments:
  - position: 0
    valueFrom: $(inputs.script.path)
  - position: 1
    valueFrom: pack
  - position: 20
    prefix: --output
    valueFrom: jobs.json
  - position: 21
    prefix: --summary
    valueFrom: splitting_summary.json

inputs:
  script:
    type: File
    default:
      class: File
      location: crab_splitting.py
  feed:
    type: File
    inputBinding: {position: 2, prefix: --feed}
  algorithm:
    type:
      type: enum
      symbols: [FileBased, LumiBased, EventAwareLumiBased, Automatic, EventBased]
    inputBinding: {position: 3, prefix: --algorithm}
  units_per_job:
    type: float
    doc: Data.unitsPerJob (files, lumis, events or minutes for Automatic)
    inputBinding: {position: 4, prefix: --units-per-job}
  total_units:
    type: float?
    doc: Data.totalUnits (<1 is a fraction of the available units)
    inputBinding: {position: 5, prefix: --total-units}
  max_jobs:
    type: int?
    doc: TaskWorker.maxJobsPerTask
    inputBinding: {position: 6, prefix: --max-jobs}
  num_probes:
    type: int?
    doc: TaskWorker.numAutomaticProbes
    inputBinding: {position: 7, prefix: --num-probes}
  halt_on_file_boundaries:
    type: boolean?
    inputBinding: {position: 8, prefix: --halt-on-file-boundaries}

outputs:
  jobs:
    type: File
    outputBinding:
      glob: jobs.json
  summary:
    type: File
    outputBinding:
      glob: splitting_summary.json

successCodes: [0]
permanentFailCodes: [2]
