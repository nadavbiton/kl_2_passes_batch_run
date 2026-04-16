from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from kl_2_passes_batch_run.pass_1 import run_pass_1  # type: ignore
    from kl_2_passes_batch_run.pass_2 import run_pass_2  # type: ignore
    from kl_2_passes_batch_run.prepare_kl_run import run_prepare  # type: ignore
else:
    raise RuntimeError("Run this script as a standalone script.")


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_CONFIG = Path(
    r"C:\Users\binada\PycharmProjects\eeg_kl_batcher\output\config\tail\interpolated\generic\S101_interpolate_after_0_batch_1.json"
)
DEFAULT_SOURCE_DATA = Path(
    r"C:\Users\binada\PycharmProjects\eeg_kl_batcher\input\tail\interpolated\split\S101_interpolate_after_0.csv.gz"
)
DEFAULT_TAKS_DIR = PROJECT_ROOT / "tests" / "test_inputs" / "taks_s101_s105"
DEFAULT_WORK_ROOT = PROJECT_ROOT / "tests" / "test_inputs" / "s101_after_0_batch_1"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "tests" / "test_outputs" / "s101_after_0_batch_1"

SUB_BATCH_COUNT = 4
SUB_BATCH_SIZE = 50
MAX_DIFF_PRINT = 20


def _parse_entities(raw: str) -> list[str]:
    return [x.strip() for x in raw.split(",") if x.strip()]


def _entities_to_string(entities: list[str]) -> str:
    return ", ".join(entities)


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _copy_dir_contents(src: Path, dst: Path) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        target = dst / item.name
        if item.is_file():
            shutil.copy2(item, target)
        else:
            shutil.copytree(item, target, dirs_exist_ok=True)


def _build_sub_batches(source_cfg_path: Path) -> tuple[dict, list[list[str]]]:
    cfg = _read_json(source_cfg_path)
    entities = _parse_entities(str(cfg.get("entities") or ""))
    if len(entities) != SUB_BATCH_COUNT * SUB_BATCH_SIZE:
        raise ValueError(
            f"Expected 200 entities, got {len(entities)} in {source_cfg_path}"
        )

    chunks: list[list[str]] = []
    for i in range(SUB_BATCH_COUNT):
        start = i * SUB_BATCH_SIZE
        end = start + SUB_BATCH_SIZE
        chunks.append(entities[start:end])

    counts = [len(x) for x in chunks]
    all_entities = [x for chunk in chunks for x in chunk]
    overlap_free = len(all_entities) == len(set(all_entities))
    same_entities = set(all_entities) == set(entities)

    print("[Test] Sub-batch split summary")
    print(f"[Test] source={source_cfg_path.name}")
    print(f"[Test] counts={counts}")
    print(f"[Test] overlap_check={'PASS' if overlap_free and same_entities else 'FAIL'}")

    if counts != [SUB_BATCH_SIZE] * SUB_BATCH_COUNT:
        raise ValueError(f"Invalid sub-batch sizes: {counts}")
    if not overlap_free:
        raise ValueError("Sub-batch split has overlapping entities")
    if not same_entities:
        raise ValueError("Sub-batch split does not preserve full entity set")

    return cfg, chunks


def _prepare_package_layout(
    package_root: Path,
    source_data_gz: Path,
    taks_dir: Path,
) -> tuple[Path, Path, Path]:
    if package_root.exists():
        shutil.rmtree(package_root)
    data_dir = package_root / "data"
    taks_dst = package_root / "taks"
    batch_cfg_dir = package_root / "batch_configs"
    data_dir.mkdir(parents=True, exist_ok=True)
    taks_dst.mkdir(parents=True, exist_ok=True)
    batch_cfg_dir.mkdir(parents=True, exist_ok=True)

    shutil.copy2(source_data_gz, data_dir / source_data_gz.name)
    _copy_dir_contents(taks_dir, taks_dst)
    return data_dir, taks_dst, batch_cfg_dir


def _find_single_csv(root: Path, name: str) -> Path:
    paths = list(root.rglob(name))
    if not paths:
        raise FileNotFoundError(f"Could not find '{name}' under {root}")
    if len(paths) > 1:
        print(f"[Test] WARNING multiple '{name}' found, using first: {paths[0]}")
    return paths[0]


def _read_support_csv(path: Path) -> dict[str, int]:
    out: dict[str, int] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pattern = str(row.get("pattern_name") or "").strip()
            if not pattern:
                continue
            raw = str(row.get("supporting_entities_count") or "0").strip()
            try:
                out[pattern] = int(float(raw))
            except ValueError:
                out[pattern] = 0
    return out


def _read_baseline_hz_from_results_csv(path: Path) -> tuple[list[str], dict[str, dict[str, float]]]:
    matrix: dict[str, dict[str, float]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], matrix
        ignore = {"id", "class_name", "value"}
        patterns = [c for c in reader.fieldnames if c and c not in ignore]
        for row in reader:
            entity_id = str(row.get("id") or "").strip()
            if not entity_id:
                continue
            per_pattern: dict[str, float] = {}
            for pattern in patterns:
                raw = str(row.get(pattern) or "").strip()
                if not raw:
                    per_pattern[pattern] = 0.0
                    continue
                try:
                    per_pattern[pattern] = float(raw)
                except ValueError:
                    per_pattern[pattern] = 0.0
            matrix[entity_id] = per_pattern
    return patterns, matrix


def _read_pipeline_hz_tot_pop(path: Path) -> tuple[list[str], dict[str, dict[str, float]]]:
    matrix: dict[str, dict[str, float]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return [], matrix
        patterns = [c for c in reader.fieldnames if c not in {"batch_id", "id"}]
        for row in reader:
            entity_id = str(row.get("id") or "").strip()
            if not entity_id:
                continue
            existing = matrix.setdefault(entity_id, {})
            for pattern in patterns:
                raw = str(row.get(pattern) or "").strip()
                try:
                    val = float(raw) if raw else 0.0
                except ValueError:
                    val = 0.0
                existing[pattern] = existing.get(pattern, 0.0) + val
    return patterns, matrix


def _compare_support(
    baseline_support: dict[str, int],
    pipeline_support: dict[str, int],
) -> tuple[bool, list[str]]:
    diffs: list[str] = []
    for pattern in sorted(baseline_support):
        b = baseline_support.get(pattern, 0)
        p = pipeline_support.get(pattern, 0)
        if b != p:
            diffs.append(f"{pattern}: baseline={b}, pipeline={p}")
    return len(diffs) == 0, diffs[:MAX_DIFF_PRINT]


def _compare_hz(
    baseline_hz: dict[str, dict[str, float]],
    pipeline_hz: dict[str, dict[str, float]],
    compare_patterns: list[str],
) -> tuple[bool, list[str]]:
    diffs: list[str] = []
    all_entities = sorted(set(baseline_hz) | set(pipeline_hz))
    for entity_id in all_entities:
        b_row = baseline_hz.get(entity_id, {})
        p_row = pipeline_hz.get(entity_id, {})
        for pattern in compare_patterns:
            b = float(b_row.get(pattern, 0.0))
            p = float(p_row.get(pattern, 0.0))
            if abs(b - p) > 1e-9:
                diffs.append(f"entity={entity_id}, pattern={pattern}, baseline={b}, pipeline={p}")
                if len(diffs) >= MAX_DIFF_PRINT:
                    return False, diffs
    return len(diffs) == 0, diffs


def _check_internal_consistency(
    pipeline_hz: dict[str, dict[str, float]],
    pipeline_support: dict[str, int],
) -> tuple[bool, list[str]]:
    support_from_hz: dict[str, int] = {}
    for row in pipeline_hz.values():
        for pattern, value in row.items():
            if value > 0:
                support_from_hz[pattern] = support_from_hz.get(pattern, 0) + 1

    diffs: list[str] = []
    for pattern, support in sorted(pipeline_support.items()):
        hz_support = support_from_hz.get(pattern, 0)
        if support != hz_support:
            diffs.append(f"{pattern}: support_csv={support}, hz_support={hz_support}")
            if len(diffs) >= MAX_DIFF_PRINT:
                break
    return len(diffs) == 0, diffs


def _prepare_case_inputs(
    source_config: Path,
    source_data_gz: Path,
    taks_dir: Path,
    work_root: Path,
) -> tuple[Path, Path, str]:
    if not source_config.exists():
        raise FileNotFoundError(f"source config not found: {source_config}")
    if not source_data_gz.exists():
        raise FileNotFoundError(f"source data not found: {source_data_gz}")
    if not taks_dir.exists():
        raise FileNotFoundError(f"taks dir not found: {taks_dir}")

    source_cfg, chunks = _build_sub_batches(source_config)
    source_batch_id = source_config.stem

    baseline_pkg = work_root / "baseline_input_package"
    pipeline_pkg = work_root / "pipeline_input_package"
    _, _, baseline_cfg_dir = _prepare_package_layout(baseline_pkg, source_data_gz, taks_dir)
    _, _, pipeline_cfg_dir = _prepare_package_layout(pipeline_pkg, source_data_gz, taks_dir)

    baseline_cfg_path = baseline_cfg_dir / source_config.name
    baseline_cfg_path.write_text(json.dumps(source_cfg, indent=2) + "\n", encoding="utf-8")

    for idx, chunk in enumerate(chunks, start=1):
        sub_cfg = dict(source_cfg)
        sub_cfg["entities"] = _entities_to_string(chunk)
        sub_name = f"{source_batch_id}_sub_{idx}.json"
        (pipeline_cfg_dir / sub_name).write_text(
            json.dumps(sub_cfg, indent=2) + "\n",
            encoding="utf-8",
        )

    print(f"[Test] Prepared inputs under {work_root}")
    return baseline_pkg, pipeline_pkg, source_batch_id


def _compare_outputs(source_batch_id: str, baseline_output: Path, pipeline_output: Path) -> int:
    print("[Test] COMPARE baseline vs pipeline outputs")
    baseline_support_path = _find_single_csv(
        baseline_output / "archive" / "pass_1" / source_batch_id / "results",
        "patterns_support.csv",
    )
    baseline_results_path = _find_single_csv(
        baseline_output / "archive" / "pass_1" / source_batch_id / "results",
        "results.csv",
    )
    pipeline_support_tot_path = pipeline_output / "patterns_support_tot_pop.csv"
    pipeline_hz_tot_path = pipeline_output / "results_hz_tot_pop.csv"
    if not pipeline_support_tot_path.exists():
        raise FileNotFoundError(f"Missing pipeline support total csv: {pipeline_support_tot_path}")
    if not pipeline_hz_tot_path.exists():
        raise FileNotFoundError(f"Missing pipeline hz total csv: {pipeline_hz_tot_path}")

    baseline_support = _read_support_csv(baseline_support_path)
    baseline_patterns = sorted(baseline_support.keys())
    pipeline_support_full = _read_support_csv(pipeline_support_tot_path)
    pipeline_support = {k: v for k, v in pipeline_support_full.items() if k in baseline_support}

    support_ok, support_diffs = _compare_support(baseline_support, pipeline_support)
    print(f"[Test] support comparison: {'PASS' if support_ok else 'FAIL'}")
    for line in support_diffs:
        print(f"  - {line}")

    _, baseline_hz = _read_baseline_hz_from_results_csv(baseline_results_path)
    _, pipeline_hz_full = _read_pipeline_hz_tot_pop(pipeline_hz_tot_path)
    pipeline_hz = {
        eid: {p: row.get(p, 0.0) for p in baseline_patterns}
        for eid, row in pipeline_hz_full.items()
    }

    hz_ok, hz_diffs = _compare_hz(baseline_hz, pipeline_hz, baseline_patterns)
    print(f"[Test] hz matrix comparison: {'PASS' if hz_ok else 'FAIL'}")
    for line in hz_diffs:
        print(f"  - {line}")

    consistency_ok, consistency_diffs = _check_internal_consistency(pipeline_hz_full, pipeline_support_full)
    print(f"[Test] internal consistency (count(hz>0)==support): {'PASS' if consistency_ok else 'FAIL'}")
    for line in consistency_diffs:
        print(f"  - {line}")

    final_ok = support_ok and hz_ok and consistency_ok
    print(f"[Test] FINAL STATUS: {'PASS' if final_ok else 'FAIL'}")
    print(f"[Test] baseline_output={baseline_output}")
    print(f"[Test] pipeline_output={pipeline_output}")
    return 0 if final_ok else 1


def run_validation(
    source_config: Path,
    source_data_gz: Path,
    taks_dir: Path,
    work_root: Path,
    output_root: Path,
    domain_name: str,
    use_fast_count_engine: bool,
    mode: str,
) -> int:
    baseline_pkg, pipeline_pkg, source_batch_id = _prepare_case_inputs(
        source_config=source_config,
        source_data_gz=source_data_gz,
        taks_dir=taks_dir,
        work_root=work_root,
    )

    baseline_output = output_root / "baseline_pass1"
    pipeline_output = output_root / "pipeline_2pass"

    if mode == "full":
        if baseline_output.exists():
            shutil.rmtree(baseline_output)
        if pipeline_output.exists():
            shutil.rmtree(pipeline_output)
        baseline_output.mkdir(parents=True, exist_ok=True)
        pipeline_output.mkdir(parents=True, exist_ok=True)

        print("[Test] PREPARE dataset/knowledge (once)")
        run_prepare(baseline_pkg)

        print("[Test] RUN baseline Pass 1 (big config)")
        baseline_res = run_pass_1(input_package_dir=baseline_pkg, output_dir=baseline_output)
        print(f"[Test] baseline pass_1: {baseline_res.get('successful_batches', 0)}/{baseline_res.get('total_batches', 0)}")

        print("[Test] RUN pipeline Pass 1 (4 sub-batches)")
        pipeline_pass1_res = run_pass_1(input_package_dir=pipeline_pkg, output_dir=pipeline_output)
        print(
            f"[Test] pipeline pass_1: {pipeline_pass1_res.get('successful_batches', 0)}/{pipeline_pass1_res.get('total_batches', 0)}"
        )

        print("[Test] RUN pipeline Pass 2 (CandidateCount fast mode)")
        pipeline_pass2_res = run_pass_2(
            input_package_dir=pipeline_pkg,
            output_dir=pipeline_output,
            domain_name=domain_name,
            use_fast_count_engine=use_fast_count_engine,
        )
        print(f"[Test] pipeline pass_2 status: {pipeline_pass2_res.get('status')}")

    elif mode == "pipeline_and_compare":
        if pipeline_output.exists():
            shutil.rmtree(pipeline_output)
        pipeline_output.mkdir(parents=True, exist_ok=True)

        print("[Test] RUN pipeline Pass 1 (4 sub-batches)")
        pipeline_pass1_res = run_pass_1(input_package_dir=pipeline_pkg, output_dir=pipeline_output)
        print(
            f"[Test] pipeline pass_1: {pipeline_pass1_res.get('successful_batches', 0)}/{pipeline_pass1_res.get('total_batches', 0)}"
        )

        print("[Test] RUN pipeline Pass 2 (CandidateCount fast mode)")
        pipeline_pass2_res = run_pass_2(
            input_package_dir=pipeline_pkg,
            output_dir=pipeline_output,
            domain_name=domain_name,
            use_fast_count_engine=use_fast_count_engine,
        )
        print(f"[Test] pipeline pass_2 status: {pipeline_pass2_res.get('status')}")

    elif mode == "compare_only":
        print("[Test] compare_only mode: using existing outputs")

    else:
        raise ValueError(f"Unsupported mode: {mode}")

    return _compare_outputs(source_batch_id, baseline_output, pipeline_output)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Split-batch validator (Generic big vs 2-pass 4 sub-batches)")
    parser.add_argument("--source-config", type=Path, default=DEFAULT_SOURCE_CONFIG)
    parser.add_argument("--source-data-gz", type=Path, default=DEFAULT_SOURCE_DATA)
    parser.add_argument("--taks-dir", type=Path, default=DEFAULT_TAKS_DIR)
    parser.add_argument("--work-root", type=Path, default=DEFAULT_WORK_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--domain-name", type=str, default="S101")
    parser.add_argument("--mode", choices=["full", "pipeline_and_compare", "compare_only"], default="full")
    parser.add_argument("--use-fast-count-engine", action="store_true", default=True)
    parser.add_argument("--no-fast-count-engine", action="store_false", dest="use_fast_count_engine")
    return parser


def main() -> int:
    args = _build_arg_parser().parse_args()
    return run_validation(
        source_config=args.source_config,
        source_data_gz=args.source_data_gz,
        taks_dir=args.taks_dir,
        work_root=args.work_root,
        output_root=args.output_root,
        domain_name=args.domain_name,
        use_fast_count_engine=bool(args.use_fast_count_engine),
        mode=str(args.mode),
    )


if __name__ == "__main__":
    raise SystemExit(main())
