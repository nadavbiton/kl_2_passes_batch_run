from __future__ import annotations

import argparse
import importlib.util
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
TESTS_ROOT = PROJECT_ROOT / "tests"
TEST_INPUTS_ROOT = TESTS_ROOT / "test_inputs"
SOURCE_CONFIGS_DIR = TEST_INPUTS_ROOT / "source_configs"
SOURCE_DATA_DIR = TEST_INPUTS_ROOT / "source_data"
CASES_ROOT = TEST_INPUTS_ROOT / "cases"
TAKS_S101_DIR = TEST_INPUTS_ROOT / "taks_s101"
TAKS_S105_DIR = TEST_INPUTS_ROOT / "taks_s105"

EXT_CONFIG_ROOT = Path(r"C:\Users\binada\PycharmProjects\eeg_kl_batcher\output\config\tail\interpolated\generic")
EXT_DATA_ROOT = Path(r"C:\Users\binada\PycharmProjects\eeg_kl_batcher\input\tail\interpolated\split")

CASES = [
    {
        "case_name": "s101_after_0_batch_1",
        "config_name": "S101_interpolate_after_0_batch_1.json",
        "data_name": "S101_interpolate_after_0.csv.gz",
        "domain_name": "S101",
        "taks_dir": TAKS_S101_DIR,
    },
    {
        "case_name": "s101_after_0_batch_2",
        "config_name": "S101_interpolate_after_0_batch_2.json",
        "data_name": "S101_interpolate_after_0.csv.gz",
        "domain_name": "S101",
        "taks_dir": TAKS_S101_DIR,
    },
    {
        "case_name": "s101_after_0_batch_3",
        "config_name": "S101_interpolate_after_0_batch_3.json",
        "data_name": "S101_interpolate_after_0.csv.gz",
        "domain_name": "S101",
        "taks_dir": TAKS_S101_DIR,
    },
    {
        "case_name": "s105_after_0_batch_1",
        "config_name": "S105_interpolate_after_0_batch_1.json",
        "data_name": "S105_interpolate_after_0.csv.gz",
        "domain_name": "S105",
        "taks_dir": TAKS_S105_DIR,
    },
    {
        "case_name": "s105_after_0_batch_2",
        "config_name": "S105_interpolate_after_0_batch_2.json",
        "data_name": "S105_interpolate_after_0.csv.gz",
        "domain_name": "S105",
        "taks_dir": TAKS_S105_DIR,
    },
    {
        "case_name": "s105_after_0_batch_3",
        "config_name": "S105_interpolate_after_0_batch_3.json",
        "data_name": "S105_interpolate_after_0.csv.gz",
        "domain_name": "S105",
        "taks_dir": TAKS_S105_DIR,
    },
]


def _load_validation_module():
    module_path = TESTS_ROOT / "run_split_batch_validation.py"
    spec = importlib.util.spec_from_file_location("split_validation", str(module_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {module_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _copy_file(src: Path, dst: Path) -> None:
    if not src.exists():
        raise FileNotFoundError(f"Missing source file: {src}")
    _ensure_dir(dst.parent)
    shutil.copy2(src, dst)


def prepare_suite_inputs() -> int:
    _ensure_dir(TEST_INPUTS_ROOT)
    _ensure_dir(SOURCE_CONFIGS_DIR)
    _ensure_dir(SOURCE_DATA_DIR)
    _ensure_dir(CASES_ROOT)
    _ensure_dir(TAKS_S101_DIR)
    _ensure_dir(TAKS_S105_DIR)

    mod = _load_validation_module()

    # Copy source configs + data into local test_inputs catalog.
    for case in CASES:
        src_cfg = EXT_CONFIG_ROOT / case["config_name"]
        dst_cfg = SOURCE_CONFIGS_DIR / case["config_name"]
        _copy_file(src_cfg, dst_cfg)

        src_data = EXT_DATA_ROOT / case["data_name"]
        dst_data = SOURCE_DATA_DIR / case["data_name"]
        if not dst_data.exists():
            _copy_file(src_data, dst_data)

    # Build per-case prepared packages (baseline + 4 sub-batches).
    for case in CASES:
        local_cfg = SOURCE_CONFIGS_DIR / case["config_name"]
        local_data = SOURCE_DATA_DIR / case["data_name"]
        taks_dir = Path(case["taks_dir"])
        case_work_root = CASES_ROOT / case["case_name"]

        if not taks_dir.exists():
            raise FileNotFoundError(f"Missing TAKs folder: {taks_dir}")
        tak_count = len(list(taks_dir.glob("*")))
        if tak_count == 0:
            raise RuntimeError(f"TAKs folder is empty: {taks_dir}")

        mod._prepare_case_inputs(  # internal helper by design
            source_config=local_cfg,
            source_data_gz=local_data,
            taks_dir=taks_dir,
            work_root=case_work_root,
        )

        baseline_cfg = case_work_root / "baseline_input_package" / "batch_configs"
        pipeline_cfg = case_work_root / "pipeline_input_package" / "batch_configs"
        pipeline_files = sorted(p.name for p in pipeline_cfg.glob("*.json"))

        print(f"[SuitePrep] case={case['case_name']} domain={case['domain_name']}")
        print(f"[SuitePrep]   source_config={local_cfg}")
        print(f"[SuitePrep]   source_data={local_data}")
        print(f"[SuitePrep]   taks={taks_dir} (count={tak_count})")
        print(f"[SuitePrep]   baseline_cfg_count={len(list(baseline_cfg.glob('*.json')))}")
        print(f"[SuitePrep]   pipeline_cfg_count={len(pipeline_files)}")
        for name in pipeline_files:
            print(f"[SuitePrep]     - {name}")

    print("[SuitePrep] DONE")
    print(f"[SuitePrep] test_inputs_root={TEST_INPUTS_ROOT}")
    print(f"[SuitePrep] source_configs_dir={SOURCE_CONFIGS_DIR}")
    print(f"[SuitePrep] source_data_dir={SOURCE_DATA_DIR}")
    print(f"[SuitePrep] cases_root={CASES_ROOT}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare all split-batch test inputs for S101/S105 after_0 batch 1-3")
    _ = parser.parse_args()
    return prepare_suite_inputs()


if __name__ == "__main__":
    raise SystemExit(main())
