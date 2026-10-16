#!/usr/bin/env cwl-runner
# CRAB-like input splitting: feeder -> packer
cwlVersion: v1.2
class: Workflow
label: CRAB-like input splitting (feeder + packer)
doc: |
  Equivalent of the CRABServer TaskWorker DataDiscovery -> Splitter chain.

inputs:
  input_files: File?
  job_type:
    type:
      type: enum
      symbols: [Analysis, PrivateMC]
    default: Analysis
  lumi_mask: File?
  run_range: string?
  total_events: int?
  events_per_lumi: int?
  algorithm:
    type:
      type: enum
      symbols: [FileBased, LumiBased, EventAwareLumiBased, Automatic, EventBased]
  units_per_job: float
  total_units: float?
  max_jobs: int?
  num_probes: int?
  halt_on_file_boundaries: boolean?

outputs:
  feed:
    type: File
    outputSource: feeder/feed
  jobs:
    type: File
    outputSource: packer/jobs
  summary:
    type: File
    outputSource: packer/summary

steps:
  feeder:
    run: feeder.cwl
    in:
      input_files: input_files
      job_type: job_type
      lumi_mask: lumi_mask
      run_range: run_range
      total_events: total_events
      events_per_lumi: events_per_lumi
    out: [feed]

  packer:
    run: packer.cwl
    in:
      feed: feeder/feed
      algorithm: algorithm
      units_per_job: units_per_job
      total_units: total_units
      max_jobs: max_jobs
      num_probes: num_probes
      halt_on_file_boundaries: halt_on_file_boundaries
    out: [jobs, summary]
