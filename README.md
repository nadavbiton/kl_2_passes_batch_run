# kl_2passes_batch

Two-pass batch project for KarmaLego processing.

## Purpose
Run Pass 1 candidate generation/union and Pass 2 CandidateCount for one dataset unit.

## Prerequisites (Required Before Running)
1. Build KarmaLego (`C:\KarmaLego`) in Release so `KarmaLegoConsoleApp.exe` exists.
2. Ensure DB and Mediator executable settings in `settings.py` are correct.
3. Ensure KarmaLego supports `--appsettings <path>` (already implemented in this setup).

This project now generates per-run/per-worker appsettings files automatically at runtime.

## Installation
From `C:\Users\binada\PycharmProjects\kl_2_passes_batch_run`:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Modes
- `full_run`: Prepare + Pass 1 + Pass 2
- `run_pass_1_2`: Pass 1 + Pass 2
- `run_pass_1`: Pass 1 only
- `run_pass_2`: Pass 2 only

## Input Model
Use one run config JSON (not CLI-heavy):

- `mode`: `full_run`, `run_pass_1_2`, `run_pass_1`, or `run_pass_2`
- `data_mode`: `sql` or `file`
- `input_package_dir`: folder containing:
  - `data/*` (exactly one events file: `.csv` or `.csv.gz`)
  - `concepts/*` (required in `file` mode; exactly one knowledge file: `.csv` or `.csv.gz`)
  - `taks/*`
  - `batch_configs/*.json`
- `output_dir`: where union and summaries are written
- `candidatecount_domain_name`: required by CandidateCount config
- `use_fast_count_engine` (optional, default `false`): `true` for fast engine, `false` for legacy engine in CandidateCount
- `pass_1_max_parallel` (optional, default `1`): number of pass-1 workers
- `pass_1_emit_karmalego_output` (optional, default `false`): print KarmaLego stdout/stderr to terminal during pass 1

Example file: `run_config.example.json`

### File mode behavior (`data_mode=file`)
- `prepare` DB stage is skipped.
- Runtime input files are copied to fixed targets:
  - `runtime/data/events.csv` or `runtime/data/events.csv.gz`
  - `runtime/data/knowledge.csv` or `runtime/data/knowledge.csv.gz`
- Hard-fail (no warning) if:
  - `input_package/data` missing
  - `input_package/concepts` missing
  - no `.csv`/`.csv.gz` file found in either folder
  - more than one candidate file found in either folder

## Runtime Layout (project-local)
- `runtime/wN/KarmalegoConfigPath` (pass 1 worker runtime, `w1`, `w2`, ...)
- `runtime/wN/ResultsPath`
- `runtime/wN/LogsPath`
- `runtime/wN/appsettings.json`
- `runtime/pass_2/KarmalegoConfigPath`
- `runtime/pass_2/ResultsPath`
- `runtime/pass_2/LogsPath`
- `runtime/pass_2/appsettings.json`
- `<output_dir>/archive/pass_1/<batch_id>/...`
- `<output_dir>/reuse_by_batch/<batch_id>/...`

## Pass 1 Outputs
- `candidates_union.csv`: union candidates with `pattern_name, level, parent_name, batch_count`
- `reuse_by_batch/<batch_id>/patterns_support.csv`: copied from Generic batch output
- `reuse_by_batch/<batch_id>/results.csv`: copied from Generic batch output
- `sequence_result.json`: top-level run summary (includes Pass 1 `candidate_count` list per batch)

## Main Files
- `settings.py`: paths, db/exe config, constants
- `karmalego_runtime_common.py`: shared DB/fs/process/parsing helpers
- `prepare_kl_run.py`: SQL + data load + TAK + mediator
- `pass_1.py`: batch runs + candidate union (main function: `run_pass_1`)
- `pass_2.py`: single-run CandidateCount Pass 2 execution (main function: `run_pass_2`)
- `run_kl_2_passes_sequence.py`: mode-based sequence entrypoint

## CandidateCount Pass 2 (Standalone)
Run after Pass 1 union exists in `<output_dir>/candidates_union.csv`.

```powershell
.\.venv\Scripts\python.exe -m kl_2_passes_batch_run.pass_2
```

This runner:
- writes CandidateCount master config to `runtime/pass_2/KarmalegoConfigPath/config_file.json`
- writes runtime appsettings to `runtime/pass_2/appsettings.json` with `VersionType=CandidateCount`
- runs KarmaLego once with `--appsettings runtime/pass_2/appsettings.json`
- provides `reuse_by_batch_root=<output_dir>/reuse_by_batch` to CandidateCount
- builds `<output_dir>/patterns_support_tot_pop.csv` by summing all batch `patterns_support.csv` files
- builds `<output_dir>/results_hz_tot_pop.csv` by concatenating all batch `results_hz.csv` files
- validates aggregated consistency between those two files:
  - same pattern set
  - `supporting_entities_count == count(hz > 0)` per pattern
- copies CandidateCount outputs into `<output_dir>/archive/pass_2/...`
- writes `pass_2_candidatecount_result.json` under `output_dir` (standalone run)

## How to Run
1. Copy `run_config.example.json` to `run_config.json` and edit values.
2. Run:

```powershell
.\.venv\Scripts\python.exe -m kl_2_passes_batch_run.run_kl_2_passes_sequence
```

Or from your existing interpreter if dependencies are installed.



