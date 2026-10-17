#!/usr/bin/env cwl-runner
# The process run by every parcel of the RucioMergeBySize transformation: a stand-in
# for the EDM merge job (cmsRun with a merge PSet) of WMCore's merge tasks.
cwlVersion: v1.2
class: CommandLineTool
id: mock-merge
label: EDM merge (mock)
doc: |
  Merges the parcel's files into one output file. The inputs are lfn strings,
  read in place at the site the packer sent the job to (DX-ADR-007); the mock
  only lists them in the merged file. Every input is reported as SUCCESS.

$namespaces:
  dirac: https://diracgrid.org/cwl#

requirements:
  InlineJavascriptRequirement: {}
  InitialWorkDirRequirement:
    listing:
      - entryname: parcel.json
        entry: |-
          $(JSON.stringify({
            input_data: (inputs["input-data"] || []).map(function (f) {
              return typeof f === "string" ? f : f.path;
            }),
            manifest: inputs["inputs-manifest"],
            output_prefix: inputs["output-prefix"]
          }))
      - entryname: mock_merge.py
        entry: |
          import json

          cfg = json.load(open("parcel.json"))
          manifest = json.loads(cfg["manifest"] or "[]")
          with open(cfg["output_prefix"] + ".root", "w") as out:
              for lfn in cfg["input_data"]:
                  out.write("merged " + lfn + "\n")
          with open("status_report.json", "w") as out:
              json.dump({item["id"]: {"action": "SUCCESS"} for item in manifest}, out)
          print("Merged", len(cfg["input_data"]), "files")

hints:
  DockerRequirement:
    dockerPull: python:3.11-slim

baseCommand: [python3, mock_merge.py]
stdout: merge.log

inputs:
  input-data:
    doc: The files to merge, all at the job's site
    type:
      - "null"
      - type: array
        items: [File, string]
  inputs-manifest:
    doc: Per parcel, set by the packer; the input ids the status report is keyed by
    type: string
    default: "[]"
  output-prefix:
    type: string
    default: merged

outputs:
  merged:
    type: File[]
    outputBinding:
      glob: "*.root"
  status-report:
    doc: The DX-ADR-007 status report, keyed by input id
    dirac:StatusReport: true
    type: File
    outputBinding:
      glob: status_report.json
  log:
    doc: Sandbox-bound in production, through the output_sandbox patterns
    type: File
    outputBinding:
      glob: merge.log
