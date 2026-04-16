from __future__ import annotations

import argparse
import csv
from pathlib import Path

IGNORE_BASELINE_COLS = {"id", "class_name", "value"}


def _find_single(root: Path, name: str) -> Path:
    matches = list(root.rglob(name))
    if not matches:
        raise FileNotFoundError(f"Could not find '{name}' under: {root}")
    return matches[0]


def _baseline_schema(results_csv: Path) -> tuple[list[str], list[str]]:
    """
    Returns:
      patterns in baseline column order,
      entities in baseline row order.
    """
    entity_order: list[str] = []
    seen_entities: set[str] = set()

    with results_csv.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"No header in baseline file: {results_csv}")

        patterns = [c for c in reader.fieldnames if c and c not in IGNORE_BASELINE_COLS]
        for row in reader:
            entity_id = str(row.get("id") or "").strip()
            if not entity_id or entity_id in seen_entities:
                continue
            seen_entities.add(entity_id)
            entity_order.append(entity_id)

    return patterns, entity_order


def _load_hz_rows_by_entity(hz_tot: Path) -> tuple[dict[str, dict[str, str]], list[str]]:
    """
    Load HZ total-pop rows indexed by entity id.
    Assumes entity ids should be unique for split-batch setup.
    """
    by_entity: dict[str, dict[str, str]] = {}
    fieldnames: list[str] = []

    with hz_tot.open("r", encoding="utf-8", newline="") as src:
        reader = csv.DictReader(src)
        if not reader.fieldnames:
            raise ValueError(f"No header in HZ total file: {hz_tot}")
        fieldnames = list(reader.fieldnames)

        for row in reader:
            entity_id = str(row.get("id") or "").strip()
            if not entity_id:
                continue
            # Keep the first row if duplicates appear unexpectedly.
            if entity_id not in by_entity:
                by_entity[entity_id] = row

    return by_entity, fieldnames


def _filter_case(case_root: Path) -> Path:
    baseline_root = case_root / "baseline_pass1" / "archive" / "pass_1"
    pipeline_root = case_root / "pipeline_2pass"

    baseline_results = _find_single(baseline_root, "results.csv")
    hz_tot = pipeline_root / "results_hz_tot_pop.csv"
    if not hz_tot.exists():
        raise FileNotFoundError(f"Missing file: {hz_tot}")

    patterns, baseline_entity_order = _baseline_schema(baseline_results)
    hz_by_entity, hz_fields = _load_hz_rows_by_entity(hz_tot)

    # Any entities present in HZ but not in baseline are appended (stable sorted order).
    extra_entities = sorted([eid for eid in hz_by_entity.keys() if eid not in set(baseline_entity_order)])
    final_entity_order = baseline_entity_order + extra_entities

    out_path = pipeline_root / "results_hz_tot_pop_filtered_to_baseline.csv"
    out_headers = ["batch_id", "id", *patterns]

    with out_path.open("w", encoding="utf-8", newline="") as dst:
        writer = csv.DictWriter(dst, fieldnames=out_headers)
        writer.writeheader()

        for entity_id in final_entity_order:
            src_row = hz_by_entity.get(entity_id, {})
            out_row = {
                "batch_id": src_row.get("batch_id", ""),
                "id": entity_id,
            }
            for p in patterns:
                # missing pattern/entity => 0
                out_row[p] = src_row.get(p, "0") or "0"
            writer.writerow(out_row)

    missing_patterns = [p for p in patterns if p not in hz_fields]
    if missing_patterns:
        print(
            f"[WARN] {case_root.name}: {len(missing_patterns)} baseline patterns missing from HZ total file; filled with 0"
        )

    return out_path


def _iter_cases(suite_root: Path, cases: list[str] | None) -> list[Path]:
    if cases:
        return [suite_root / c for c in cases]
    return sorted([p for p in suite_root.iterdir() if p.is_dir()])


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Filter each case results_hz_tot_pop.csv to baseline pattern/entity order."
    )
    parser.add_argument(
        "--suite-root",
        type=Path,
        default=Path(r"C:\Users\binada\PycharmProjects\kl_2_passes_batch_run\tests\test_outputs"),
        help="Root containing case output dirs (default: tests/test_outputs)",
    )
    parser.add_argument(
        "--case",
        action="append",
        default=None,
        help="Case folder name under suite-root. Can be repeated. If omitted, runs all cases.",
    )
    args = parser.parse_args()

    suite_root = args.suite_root
    if not suite_root.exists():
        raise FileNotFoundError(f"Suite root not found: {suite_root}")

    case_paths = _iter_cases(suite_root, args.case)
    if not case_paths:
        raise RuntimeError(f"No case folders found under: {suite_root}")

    failed = False
    for case in case_paths:
        try:
            out_path = _filter_case(case)
            print(f"[OK] {case.name} -> {out_path}")
        except Exception as exc:  # noqa: BLE001
            failed = True
            print(f"[FAIL] {case.name}: {exc}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
