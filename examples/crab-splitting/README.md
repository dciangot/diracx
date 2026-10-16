# CRAB-like input splitting: feeder + packer (draft)

Draft CWL workflow and Python script reproducing the input splitting procedure of
[CRABServer](https://github.com/dmwm/CRABServer) (TaskWorker `DataDiscovery` → `Splitter`,
which relies on `WMCore.JobSplitting`) as two independent steps:

| Step   | CWL           | CRABServer equivalent                                   | Output                                 |
| ------ | ------------- | ------------------------------------------------------- | -------------------------------------- |
| feeder | `feeder.cwl`  | DataDiscovery post-processing (run range, lumi mask)    | `feed.json`                            |
| packer | `packer.cwl`  | `Splitter.execute` + `SplittingSummary`                 | `jobs.json`, `splitting_summary.json`  |

`crab_splitting.cwl` chains both steps. All the logic lives in `crab_splitting.py`
(stdlib only, subcommands `feed` and `pack`).

## Supported algorithms

- `FileBased`: `units_per_job` files per job, `total_units` files.
- `LumiBased`: `units_per_job` lumis per job, `total_units` lumis.
- `EventAwareLumiBased`: lumis are packed until `units_per_job` (estimated) events, `total_units` events.
- `Automatic`: `units_per_job` is minutes per job (≥ 180); produces `num_probes` FileBased probe jobs as the TaskWorker does.
- `EventBased`: `PrivateMC` only, `units_per_job` events per job, `total_events` from the feeder (or `total_units`).

As in CRAB, `total_units < 1` is interpreted as a fraction of the available units, jobs never
mix files with different locations, and the submission is refused (exit code `2`) if no job
or more than `max_jobs` jobs are produced.

## Usage

```bash
# Full workflow
cwltool crab_splitting.cwl example_job.yaml

# Single steps
python3 crab_splitting.py feed --input-files example_input_files.json \
    --lumi-mask example_lumi_mask.json --output feed.json
python3 crab_splitting.py pack --feed feed.json --algorithm LumiBased --units-per-job 3

# PrivateMC
python3 crab_splitting.py feed --job-type PrivateMC --total-events 1050 --output feed.json
python3 crab_splitting.py pack --feed feed.json --algorithm EventBased --units-per-job 500
```

## Known simplifications

This is a draft: it does not handle `splitOnRun`, lumis split across files
(`applyLumiCorrection`), parent files, LHE inputs or the tail/processing stages of
Automatic splitting.
