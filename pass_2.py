from __future__ import annotations

import csv
import json
import shutil
import time
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from kl_2_passes_batch_run import settings  # type: ignore
    from kl_2_passes_batch_run.karmalego_runtime_common import (  # type: ignore
        clean_runtime_work_dirs,
        get_pass_2_runtime_paths,
        read_run_config,
        run_karmalego,
        validate_input_package,
        write_json,
        write_runtime_appsettings,
        write_runtime_config,
    )
else:
    from . import settings
    from .karmalego_runtime_common import (
        clean_runtime_work_dirs,
        get_pass_2_runtime_paths,
        read_run_config,
        run_karmalego,
        validate_input_package,
        write_json,
        write_runtime_appsettings,
        write_runtime_config,
    )


def _aggregate_patterns_support_tot_pop(
    candidate_root: Path,
    batch_ids: list[str],
    output_csv_path: Path,
) -> None:
    support_sum_by_pattern: dict[str, int] = {}
    total_sum_by_pattern: dict[str, int] = {}

    for batch_id in batch_ids:
        batch_csv = candidate_root / batch_id / "patterns_support.csv"
        if not batch_csv.exists():
            continue
        with batch_csv.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                pattern_name = str(row.get("pattern_name") or "").strip()
                if not pattern_name:
                    continue
                try:
                    supporting_entities_count = int(float(str(row.get("supporting_entities_count") or "0").strip()))
                except ValueError:
                    supporting_entities_count = 0
                try:
                    total_entities = int(float(str(row.get("total_entities") or "0").strip()))
                except ValueError:
                    total_entities = 0
                support_sum_by_pattern[pattern_name] = (
                    support_sum_by_pattern.get(pattern_name, 0) + supporting_entities_count
                )
                total_sum_by_pattern[pattern_name] = total_sum_by_pattern.get(pattern_name, 0) + total_entities

    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    with output_csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pattern_name", "supporting_entities_count", "total_entities", "vertical_support"])
        for pattern_name in sorted(support_sum_by_pattern):
            supporting_entities_count = support_sum_by_pattern.get(pattern_name, 0)
            total_entities = total_sum_by_pattern.get(pattern_name, 0)
            vertical_support = 0 if total_entities == 0 else supporting_entities_count / total_entities
            writer.writerow([pattern_name, supporting_entities_count, total_entities, vertical_support])


def _aggregate_results_hz_tot_pop(
    candidate_root: Path,
    batch_ids: list[str],
    output_csv_path: Path,
) -> None:
    output_csv_path.parent.mkdir(parents=True, exist_ok=True)
    wrote_header = False
    expected_headers: list[str] | None = None

    with output_csv_path.open("w", encoding="utf-8", newline="") as out_f:
        writer = csv.writer(out_f)
        for batch_id in batch_ids:
            batch_csv = candidate_root / batch_id / "results_hz.csv"
            if not batch_csv.exists():
                continue
            with batch_csv.open("r", encoding="utf-8", newline="") as in_f:
                reader = csv.DictReader(in_f)
                if reader.fieldnames is None:
                    continue
                current_headers = list(reader.fieldnames)
                if expected_headers is None:
                    expected_headers = current_headers
                elif current_headers != expected_headers:
                    raise ValueError(
                        f"results_hz.csv header mismatch for batch '{batch_id}'. "
                        "Expected the same matrix columns across batches."
                    )
                if not wrote_header:
                    writer.writerow(["batch_id", *current_headers])
                    wrote_header = True
                for row in reader:
                    writer.writerow([batch_id] + [row.get(h, "") for h in current_headers])

        if not wrote_header:
            writer.writerow(["batch_id"])


def _validate_tot_pop_consistency(
    patterns_csv_path: Path,
    results_csv_path: Path,
) -> None:
    if not patterns_csv_path.exists():
        raise FileNotFoundError(f"missing {patterns_csv_path}")
    if not results_csv_path.exists():
        raise FileNotFoundError(f"missing {results_csv_path}")

    support_by_pattern: dict[str, int] = {}
    with patterns_csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"empty csv: {patterns_csv_path}")
        if "pattern_name" not in reader.fieldnames or "supporting_entities_count" not in reader.fieldnames:
            raise ValueError(f"missing required columns in {patterns_csv_path}")
        for row in reader:
            pattern_name = str(row.get("pattern_name") or "").strip()
            if not pattern_name:
                continue
            raw = str(row.get("supporting_entities_count") or "").strip()
            try:
                support_count = int(float(raw))
            except ValueError as exc:
                raise ValueError(f"invalid supporting_entities_count for pattern '{pattern_name}'") from exc
            support_by_pattern[pattern_name] = support_count

    presence_by_pattern: dict[str, int] = {}
    with results_csv_path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames is None:
            raise ValueError(f"empty csv: {results_csv_path}")
        required_base = {"batch_id", "id"}
        if not required_base.issubset(set(reader.fieldnames)):
            raise ValueError(f"missing required columns in {results_csv_path}: expected {sorted(required_base)}")
        pattern_columns = [c for c in reader.fieldnames if c not in required_base]
        if not pattern_columns:
            raise ValueError(f"no pattern columns found in {results_csv_path}")
        presence_by_pattern = {p: 0 for p in pattern_columns}
        for row in reader:
            for pattern in pattern_columns:
                raw = str(row.get(pattern) or "").strip()
                if not raw:
                    continue
                try:
                    value = float(raw)
                except ValueError as exc:
                    raise ValueError(f"invalid hz value for pattern '{pattern}' in {results_csv_path}") from exc
                if value > 0:
                    presence_by_pattern[pattern] += 1

    patterns_set = set(support_by_pattern)
    results_set = set(presence_by_pattern)
    only_in_patterns = sorted(patterns_set - results_set)
    only_in_results = sorted(results_set - patterns_set)
    if only_in_patterns:
        raise ValueError(f"patterns only in patterns_support_tot_pop.csv (sample): {only_in_patterns[:5]}")
    if only_in_results:
        raise ValueError(f"patterns only in results_hz_tot_pop.csv (sample): {only_in_results[:5]}")

    mismatches: list[str] = []
    for pattern in sorted(patterns_set):
        expected = int(support_by_pattern[pattern])
        got = int(presence_by_pattern.get(pattern, -1))
        if expected != got:
            mismatches.append(f"{pattern}: support_count={expected} presence_count={got}")
    if mismatches:
        raise ValueError(f"support/presence mismatches (sample): {mismatches[:5]}")


def run_pass_2(
    input_package_dir: Path,
    output_dir: Path,
    domain_name: str,
    use_fast_count_engine: bool = False,
    batch_configs_path: Path | None = None,
    data_mode: str = "sql",
) -> dict:
    if not domain_name.strip():
        raise ValueError("candidatecount_domain_name must not be empty")

    input_package = validate_input_package(input_package_dir)
    candidate_patterns_path = output_dir / "candidates_union.csv"
    reuse_by_batch_root = output_dir / "reuse_by_batch"
    if not candidate_patterns_path.exists():
        raise FileNotFoundError(f"Pass-1 union file not found: {candidate_patterns_path}")
    if not reuse_by_batch_root.exists():
        raise FileNotFoundError(f"Pass-1 reuse folder not found: {reuse_by_batch_root}")

    runtime_config = {
        "domain_name": domain_name,
        "project_id": int(settings.PROJECT_ID),
        "candidate_patterns_path": str(candidate_patterns_path),
        "batch_configs_path": str(batch_configs_path or (input_package.root / "batch_configs")),
        "reuse_by_batch_root": str(reuse_by_batch_root),
        "use_fast_count_engine": bool(use_fast_count_engine),
    }

    start = time.perf_counter()
    runtime_paths = get_pass_2_runtime_paths()
    clean_runtime_work_dirs(runtime_paths)
    runtime_config_path = write_runtime_config(runtime_config, runtime_paths)
    appsettings_path = write_runtime_appsettings(
        runtime_paths,
        version_type="CandidateCount",
        data_mode=data_mode,
    )
    run_karmalego(appsettings_path=appsettings_path)
    runtime_minutes = round((time.perf_counter() - start) / 60, 3)

    candidate_root = runtime_paths.results_dir
    summary_path = candidate_root / "batch_count_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"CandidateCount summary not found: {summary_path}")

    summary = json.loads(summary_path.read_text(encoding="utf-8-sig"))
    batches: list[dict] = []
    for row in summary:
        batches.append(
            {
                "batch_id": str(row.get("batch_id") or ""),
                "reused_patterns_count": int(row.get("reused_patterns_count") or 0),
                "computed_patterns_count": int(row.get("computed_patterns_count") or 0),
                "reuse_applied": bool(row.get("reuse_applied", False)),
                "status": str(row.get("status") or ""),
            }
        )

    effective_batch_configs_dir = batch_configs_path or (input_package.root / "batch_configs")
    expected_batch_ids = sorted(cfg.stem for cfg in effective_batch_configs_dir.glob("*.json"))
    missing_output_batches: list[str] = []
    missing_hz_output_batches: list[str] = []
    for batch_id in expected_batch_ids:
        support_path = candidate_root / batch_id / "patterns_support.csv"
        if not support_path.exists():
            missing_output_batches.append(batch_id)
        hz_path = candidate_root / batch_id / "results_hz.csv"
        if not hz_path.exists():
            missing_hz_output_batches.append(batch_id)

    tot_pop_csv_path = output_dir / "patterns_support_tot_pop.csv"
    _aggregate_patterns_support_tot_pop(
        candidate_root=candidate_root,
        batch_ids=expected_batch_ids,
        output_csv_path=tot_pop_csv_path,
    )
    hz_tot_pop_csv_path = output_dir / "results_hz_tot_pop.csv"
    _aggregate_results_hz_tot_pop(
        candidate_root=candidate_root,
        batch_ids=expected_batch_ids,
        output_csv_path=hz_tot_pop_csv_path,
    )
    _validate_tot_pop_consistency(
        patterns_csv_path=tot_pop_csv_path,
        results_csv_path=hz_tot_pop_csv_path,
    )

    failed_batches_in_summary = sorted(
        str(row.get("batch_id") or "")
        for row in batches
        if str(row.get("status", "")).strip().lower() == "failed"
    )

    summary_path.unlink(missing_ok=True)

    copied_root = output_dir / "archive" / "pass_2"
    copied_root.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(candidate_root, copied_root, dirs_exist_ok=True)
    (copied_root / "batch_count_summary.json").unlink(missing_ok=True)

    return {
        "status": "success"
        if not missing_output_batches and not missing_hz_output_batches and not failed_batches_in_summary
        else "failed",
        "runtime_minutes": runtime_minutes,
        "domain_name": domain_name,
        "runtime_config_path": str(runtime_config_path),
        "appsettings_path": str(appsettings_path),
        "candidatecount_root": str(candidate_root),
        "expected_batches": len(expected_batch_ids),
        "missing_output_batches": missing_output_batches,
        "missing_hz_output_batches": missing_hz_output_batches,
        "failed_batches_in_summary": failed_batches_in_summary,
        "batches": batches,
        "patterns_support_tot_pop_path": str(tot_pop_csv_path),
        "results_hz_tot_pop_path": str(hz_tot_pop_csv_path),
    }


def run_from_config(config_path: Path | None = None) -> dict:
    config = read_run_config(config_path)
    input_package_dir = Path(str(config["input_package_dir"]))
    output_dir = Path(str(config["output_dir"]))
    domain_name = str(config["candidatecount_domain_name"])
    use_fast_count_engine = bool(config.get("use_fast_count_engine", False))
    result = run_pass_2(
        input_package_dir=input_package_dir,
        output_dir=output_dir,
        domain_name=domain_name,
        use_fast_count_engine=use_fast_count_engine,
    )
    write_json(result, output_dir / "pass_2_candidatecount_result.json")
    return result


if __name__ == "__main__":
    run_from_config()

