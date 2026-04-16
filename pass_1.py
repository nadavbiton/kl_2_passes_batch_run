from __future__ import annotations

import csv
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from kl_2_passes_batch_run.karmalego_runtime_common import (  # type: ignore
        archive_runtime_outputs,
        clean_runtime_work_dirs,
        copy_batch_config_to_runtime,
        ensure_dir,
        extract_pattern_metadata_from_results,
        get_pass_1_worker_runtime_paths,
        run_karmalego,
        validate_input_package,
        write_runtime_appsettings,
        write_json,
    )
else:
    from .karmalego_runtime_common import (
        archive_runtime_outputs,
        clean_runtime_work_dirs,
        copy_batch_config_to_runtime,
        ensure_dir,
        extract_pattern_metadata_from_results,
        get_pass_1_worker_runtime_paths,
        run_karmalego,
        validate_input_package,
        write_runtime_appsettings,
        write_json,
    )


def _write_candidates_union_csv(
    pattern_levels: dict[str, int | None],
    pattern_parents: dict[str, str],
    pattern_relation_pairs: dict[str, str],
    pattern_components: dict[str, str],
    pattern_batch_counts: dict[str, int],
    output_path: Path,
) -> None:
    ensure_dir(output_path.parent)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "pattern_name",
                "level",
                "parent_name",
                "batch_count",
                "relation_pairs_json",
                "components_json",
            ]
        )
        for name in sorted(pattern_levels):
            level = pattern_levels.get(name)
            writer.writerow(
                [
                    name,
                    "" if level is None else level,
                    pattern_parents.get(name, ""),
                    pattern_batch_counts.get(name, 0),
                    pattern_relation_pairs.get(name, ""),
                    pattern_components.get(name, ""),
                ]
            )


def _extract_batch_support_map(results_root: Path) -> dict[str, float]:
    support_by_pattern: dict[str, float] = {}
    for file_path in results_root.rglob("patterns_support.csv"):
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("pattern_name") or "").strip()
                if not name:
                    continue
                # Prefer absolute entity count; fallback to vertical support ratio.
                raw_value = (row.get("supporting_entities_count") or "").strip()
                if not raw_value:
                    raw_value = (row.get("vertical_support") or "").strip()
                if not raw_value:
                    continue
                try:
                    value = float(raw_value)
                except ValueError:
                    continue
                prev = support_by_pattern.get(name)
                if prev is None or value > prev:
                    support_by_pattern[name] = value
    return support_by_pattern


def _extract_batch_hz_map(results_root: Path) -> dict[str, dict[str, float]]:
    """
    Extract sparse HZ map from generic results.csv files:
      pattern_name -> {entity_id: hz}
    """
    hz_by_pattern: dict[str, dict[str, float]] = {}
    ignore_cols = {"id", "class_name", "value"}

    for file_path in results_root.rglob("results.csv"):
        with file_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                continue
            pattern_cols = [c for c in reader.fieldnames if c and c.strip() and c.strip() not in ignore_cols]
            for row in reader:
                entity_id = (row.get("id") or "").strip()
                if not entity_id:
                    continue
                for pattern_name in pattern_cols:
                    raw = (row.get(pattern_name) or "").strip()
                    if not raw:
                        continue
                    try:
                        hz = float(raw)
                    except ValueError:
                        continue
                    if hz <= 0:
                        continue
                    per_pattern = hz_by_pattern.setdefault(pattern_name, {})
                    prev = per_pattern.get(entity_id)
                    if prev is None or hz > prev:
                        per_pattern[entity_id] = hz

    return {k: hz_by_pattern[k] for k in sorted(hz_by_pattern)}


def run_pass_1(
    input_package_dir: Path,
    output_dir: Path,
    max_parallel: int = 1,
    emit_karmalego_output: bool = False,
) -> dict:
    input_package = validate_input_package(input_package_dir)
    ensure_dir(output_dir)
    archive_root = output_dir / "archive" / "pass_1"

    union_levels: dict[str, int | None] = {}
    union_parents: dict[str, str] = {}
    union_relation_pairs: dict[str, str] = {}
    union_components: dict[str, str] = {}
    pattern_batch_counts: dict[str, int] = {}
    batch_pattern_support: dict[str, dict[str, float]] = {}
    batch_pattern_hz: dict[str, dict[str, dict[str, float]]] = {}
    pass_1_summary: list[dict] = []

    max_parallel = max(1, int(max_parallel))
    total_batches = len(input_package.batch_configs)
    print(f"[Run] Starting batch execution: {total_batches} batches | max_parallel={max_parallel}")

    print_lock = threading.Lock()
    worker_map_lock = threading.Lock()
    thread_to_worker_id: dict[int, int] = {}

    def _get_worker_id() -> int:
        thread_id = threading.get_ident()
        with worker_map_lock:
            worker_id = thread_to_worker_id.get(thread_id)
            if worker_id is None:
                worker_id = len(thread_to_worker_id) + 1
                thread_to_worker_id[thread_id] = worker_id
            return worker_id

    def _run_one_batch(index: int, config_path: Path) -> dict:
        batch_id = config_path.stem
        worker_id = _get_worker_id()
        start = time.perf_counter()
        status = "success"
        error = ""
        batch_candidates: set[str] = set()
        batch_metadata: dict[str, dict[str, int | str | None]] = {}
        batch_support_map: dict[str, float] = {}
        batch_hz_map: dict[str, dict[str, float]] = {}
        runtime_paths = get_pass_1_worker_runtime_paths(worker_id)

        with print_lock:
            print(f"[Run] START batch {index}/{total_batches}: {batch_id} | worker=w{worker_id}")
        try:
            clean_runtime_work_dirs(runtime_paths)
            copy_batch_config_to_runtime(config_path, runtime_paths)
            appsettings_path = write_runtime_appsettings(runtime_paths, version_type="Generic")
            run_karmalego(appsettings_path=appsettings_path, emit_output=emit_karmalego_output)
            archive_path = archive_runtime_outputs(
                archive_root=archive_root,
                batch_id=batch_id,
                results_source=runtime_paths.results_dir,
                logs_source=runtime_paths.logs_dir,
            )
            batch_metadata = extract_pattern_metadata_from_results(archive_path / "results")
            batch_candidates = set(batch_metadata.keys())
            batch_support_map = _extract_batch_support_map(archive_path / "results")
            batch_hz_map = _extract_batch_hz_map(archive_path / "results")
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            error = str(exc)
        runtime_seconds = round(time.perf_counter() - start, 3)
        runtime_minutes = round(runtime_seconds / 60, 3)
        with print_lock:
            print(
                f"[Run] END   batch {index}/{total_batches}: {batch_id} | "
                f"worker=w{worker_id} | status={status} | candidates={len(batch_candidates)} | time={runtime_minutes}m"
            )
            if error:
                print(f"[Run] ERROR batch {batch_id}: {error}")
        return {
            "index": index,
            "batch_id": batch_id,
            "status": status,
            "error": error,
            "candidate_count": len(batch_candidates),
            "metadata": batch_metadata,
            "support_map": batch_support_map,
            "hz_map": batch_hz_map,
        }

    batch_result_by_index: dict[int, dict] = {}
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = [
            executor.submit(_run_one_batch, index, config_path)
            for index, config_path in enumerate(input_package.batch_configs, start=1)
        ]
        for future in as_completed(futures):
            row = future.result()
            batch_result_by_index[int(row["index"])] = row

    for index in range(1, total_batches + 1):
        row = batch_result_by_index[index]
        batch_id = str(row["batch_id"])
        status = str(row["status"])
        batch_metadata = dict(row["metadata"])
        batch_candidates = set(batch_metadata.keys())
        batch_support_map = dict(row["support_map"])
        batch_hz_map = dict(row["hz_map"])
        for name, md in batch_metadata.items():
            level = md.get("level")
            parent_name = str(md.get("parent_name") or "")
            relation_pairs_json = str(md.get("relation_pairs_json") or "")
            components_json = str(md.get("components_json") or "")
            if name not in union_levels:
                union_levels[name] = level
            else:
                prev = union_levels[name]
                if prev is None and level is not None:
                    union_levels[name] = level
                elif prev is not None and level is not None:
                    union_levels[name] = min(prev, level)
            if name not in union_parents:
                union_parents[name] = parent_name
            elif not union_parents[name] and parent_name:
                union_parents[name] = parent_name
            if name not in union_relation_pairs:
                union_relation_pairs[name] = relation_pairs_json
            elif not union_relation_pairs[name] and relation_pairs_json:
                union_relation_pairs[name] = relation_pairs_json
            if name not in union_components:
                union_components[name] = components_json
            elif not union_components[name] and components_json:
                union_components[name] = components_json
        for name in batch_candidates:
            pattern_batch_counts[name] = pattern_batch_counts.get(name, 0) + 1
        if status == "success":
            batch_pattern_support[batch_id] = dict(sorted(batch_support_map.items()))
            batch_pattern_hz[batch_id] = batch_hz_map
        pass_1_summary.append(
            {
                "batch_id": batch_id,
                "status": status,
                "candidate_count": int(row["candidate_count"]),
            }
        )

    _write_candidates_union_csv(
        union_levels,
        union_parents,
        union_relation_pairs,
        union_components,
        pattern_batch_counts,
        output_dir / "candidates_union.csv",
    )
    write_json(batch_pattern_support, output_dir / "pattern_support_by_batch.json")
    write_json(batch_pattern_hz, output_dir / "pattern_hz_by_batch.json")

    print(
        f"[Run] Completed. success={sum(1 for row in pass_1_summary if row['status']=='success')} "
        f"failed={sum(1 for row in pass_1_summary if row['status']=='failed')} "
        f"union_candidates={len(union_levels)}"
    )

    return {
        "total_batches": total_batches,
        "successful_batches": sum(1 for row in pass_1_summary if row["status"] == "success"),
        "failed_batches": sum(1 for row in pass_1_summary if row["status"] == "failed"),
        "union_candidates": len(union_levels),
        "batch_ids": [str(row.get("batch_id") or "") for row in pass_1_summary],
        "candidate_count": [int(row.get("candidate_count") or 0) for row in pass_1_summary],
        "max_parallel": max_parallel,
        "emit_karmalego_output": bool(emit_karmalego_output),
        "output_dir": str(output_dir),
    }


