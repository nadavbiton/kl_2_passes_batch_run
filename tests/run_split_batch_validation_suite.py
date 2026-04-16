from __future__ import annotations

import argparse
import importlib.util
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
TEST_INPUTS = TESTS_ROOT / "test_inputs"
TEST_OUTPUTS = TESTS_ROOT / "test_outputs"


def _load_validation_module():
    module_path = TESTS_ROOT / "run_split_batch_validation.py"
    spec = importlib.util.spec_from_file_location("split_validation", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load validation module from: {module_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


CASES = [
    {
        "case_name": "s101_after_0_batch_1",
        "source_config": TEST_INPUTS / "source_configs" / "S101_interpolate_after_0_batch_1.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S101_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s101",
        "work_root": TEST_INPUTS / "cases" / "s101_after_0_batch_1",
        "output_root": TEST_OUTPUTS / "s101_after_0_batch_1",
        "domain_name": "S101",
    },
    {
        "case_name": "s101_after_0_batch_2",
        "source_config": TEST_INPUTS / "source_configs" / "S101_interpolate_after_0_batch_2.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S101_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s101",
        "work_root": TEST_INPUTS / "cases" / "s101_after_0_batch_2",
        "output_root": TEST_OUTPUTS / "s101_after_0_batch_2",
        "domain_name": "S101",
    },
    {
        "case_name": "s101_after_0_batch_3",
        "source_config": TEST_INPUTS / "source_configs" / "S101_interpolate_after_0_batch_3.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S101_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s101",
        "work_root": TEST_INPUTS / "cases" / "s101_after_0_batch_3",
        "output_root": TEST_OUTPUTS / "s101_after_0_batch_3",
        "domain_name": "S101",
    },
    {
        "case_name": "s105_after_0_batch_1",
        "source_config": TEST_INPUTS / "source_configs" / "S105_interpolate_after_0_batch_1.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S105_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s105",
        "work_root": TEST_INPUTS / "cases" / "s105_after_0_batch_1",
        "output_root": TEST_OUTPUTS / "s105_after_0_batch_1",
        "domain_name": "S105",
    },
    {
        "case_name": "s105_after_0_batch_2",
        "source_config": TEST_INPUTS / "source_configs" / "S105_interpolate_after_0_batch_2.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S105_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s105",
        "work_root": TEST_INPUTS / "cases" / "s105_after_0_batch_2",
        "output_root": TEST_OUTPUTS / "s105_after_0_batch_2",
        "domain_name": "S105",
    },
    {
        "case_name": "s105_after_0_batch_3",
        "source_config": TEST_INPUTS / "source_configs" / "S105_interpolate_after_0_batch_3.json",
        "source_data_gz": TEST_INPUTS / "source_data" / "S105_interpolate_after_0.csv.gz",
        "taks_dir": TEST_INPUTS / "taks_s105",
        "work_root": TEST_INPUTS / "cases" / "s105_after_0_batch_3",
        "output_root": TEST_OUTPUTS / "s105_after_0_batch_3",
        "domain_name": "S105",
    },
]


def run_suite(mode: str, use_fast_count_engine: bool) -> int:
    mod = _load_validation_module()

    # Ensure package import works when script is run from IDE with different cwd.
    os.environ.setdefault("PYTHONPATH", str(PROJECT_ROOT.parent))

    results: list[tuple[str, int]] = []
    for idx, case in enumerate(CASES, start=1):
        print("=" * 90)
        print(f"[Suite] START {idx}/{len(CASES)} case={case['case_name']} mode={mode}")
        try:
            rc = mod.run_validation(
                source_config=Path(case["source_config"]),
                source_data_gz=Path(case["source_data_gz"]),
                taks_dir=Path(case["taks_dir"]),
                work_root=Path(case["work_root"]),
                output_root=Path(case["output_root"]),
                domain_name=str(case["domain_name"]),
                use_fast_count_engine=bool(use_fast_count_engine),
                mode=mode,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[Suite] EXCEPTION case={case['case_name']} error={exc}")
            rc = 1
        results.append((str(case["case_name"]), rc))
        print(f"[Suite] END case={case['case_name']} status={'PASS' if rc == 0 else 'FAIL'}")

    print("\n" + "=" * 90)
    print("[Suite] FINAL SUMMARY")
    print("case_name,status")
    for case_name, rc in results:
        print(f"{case_name},{'PASS' if rc == 0 else 'FAIL'}")

    failed = [name for name, rc in results if rc != 0]
    if failed:
        print(f"[Suite] FAILED_CASES={','.join(failed)}")
        return 1
    print("[Suite] ALL_CASES_PASS")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Run full split-batch validation suite for 6 cases (S101/S105 batch 1-3)")
    parser.add_argument("--mode", choices=["full", "pipeline_and_compare", "compare_only"], default="full")
    parser.add_argument("--use-fast-count-engine", action="store_true", default=True)
    parser.add_argument("--no-fast-count-engine", action="store_false", dest="use_fast_count_engine")
    args = parser.parse_args()
    return run_suite(mode=str(args.mode), use_fast_count_engine=bool(args.use_fast_count_engine))


if __name__ == "__main__":
    raise SystemExit(main())
