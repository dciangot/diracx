#!/usr/bin/env cwl-runner
# The process run by every parcel of the CRAB transformations: a stand-in for
# cmsRun that honours the per-parcel lumi mask or event range set by the
# CRABSplitter packer and writes the DX-ADR-007 status report.
cwlVersion: v1.2
class: CommandLineTool
id: mock-cmsrun
label: cmsRun (mock)
doc: |
  Processes the lumis of its inputs in order. With max-lumis set it stops
  after that many lumis, as a job hitting its wall-time limit would, and
  reports the inputs it only partly processed as SPLIT and the ones it never
  reached as NOT_TRIED. Runnable standalone: without an inputs manifest each
  input-data entry is treated as a whole file.

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
            lumi_mask: inputs["lumi-mask"],
            manifest: inputs["inputs-manifest"],
            first_event: inputs["first-event"],
            last_event: inputs["last-event"],
            first_lumi: inputs["first-lumi"],
            output_prefix: inputs["output-prefix"],
            max_lumis: inputs["max-lumis"]
          }))
      - entryname: mock_cmsrun.py
        entry: |
          import json

          cfg = json.load(open("parcel.json"))
          manifest = json.loads(cfg["manifest"] or "[]")
          if not manifest:
              manifest = [
                  {"id": "local-%d" % i, "lfn": lfn, "lfn_size": 0, "descriptor": {}}
                  for i, lfn in enumerate(cfg["input_data"])
              ]
          budget = cfg["max_lumis"]

          def lumis_of(mask):
              return [
                  (run, lumi)
                  for run in sorted(mask, key=int)
                  for first, last in mask[run]
                  for lumi in range(first, last + 1)
              ]

          def portion(item, lumis):
              runs = {}
              for run, lumi in lumis:
                  ranges = runs.setdefault(run, [])
                  if ranges and ranges[-1][1] + 1 == lumi:
                      ranges[-1][1] = lumi
                  else:
                      ranges.append([lumi, lumi])
              d = item["descriptor"]
              total = max(int(d.get("lumis", 1)), 1)
              return {
                  "lfn": "lfn:" + item["lfn"].removeprefix("lfn:"),
                  "lfn_size": item["lfn_size"],
                  "data": {
                      "lumi_mask": runs,
                      "lumis": len(lumis),
                      "events": round(int(d.get("events", 0)) * len(lumis) / total),
                  },
              }

          report, processed = {}, []
          for item in manifest:
              mask = item["descriptor"].get("lumi_mask")
              if mask is None or budget is None:
                  report[item["id"]] = {"action": "SUCCESS"}
                  processed.append(item)
                  continue
              lumis = lumis_of(mask)
              if budget <= 0:
                  report[item["id"]] = {"action": "NOT_TRIED"}
              elif budget >= len(lumis):
                  report[item["id"]] = {"action": "SUCCESS"}
                  processed.append(item)
              else:
                  report[item["id"]] = {
                      "action": "SPLIT",
                      "SUCCESS": [portion(item, lumis[:budget])],
                      "FAILURE": [portion(item, lumis[budget:])],
                  }
                  processed.append(item)
              budget -= min(budget, len(lumis))

          if cfg["first_event"] is not None:
              what = "events %s-%s from lumi %s" % (
                  cfg["first_event"], cfg["last_event"], cfg["first_lumi"]
              )
          else:
              what = "lumis %s of %d files" % (cfg["lumi_mask"], len(processed))
          with open(cfg["output_prefix"] + ".root", "w") as out:
              out.write(what + "\n")
          with open("status_report.json", "w") as out:
              json.dump(report, out, indent=2)
          print("Processed", what)

hints:
  DockerRequirement:
    dockerPull: python:3.11-slim

baseCommand: [python3, mock_cmsrun.py]
stdout: cmsRun.log

inputs:
  input-data:
    doc: The parcel's files; an lfn string is streamed, an lfn File staged (DX-ADR-007)
    type:
      - "null"
      - type: array
        items: [File, string]
  lumi-mask:
    doc: Per parcel, set by the packer (CMS compact JSON)
    type: string
    default: "{}"
  inputs-manifest:
    doc: Per parcel, set by the packer; the input ids the status report is keyed by
    type: string
    default: "[]"
  first-event: int?
  last-event: int?
  first-lumi: int?
  output-prefix:
    type: string
    default: output
  max-lumis:
    doc: Simulates a wall-time limit, to exercise SPLIT
    type: int?

outputs:
  output-files:
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
      glob: cmsRun.log
