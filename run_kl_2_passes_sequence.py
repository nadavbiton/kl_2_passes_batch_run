from __future__ import annotations

import time
from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from kl_2_passes_batch_run.karmalego_runtime_common import read_run_config, write_json  # type: ignore
    from kl_2_passes_batch_run.prepare_kl_run import run_prepare  # type: ignore
    from kl_2_passes_batch_run.pass_1 import run_pass_1  # type: ignore
    from kl_2_passes_batch_run.pass_2 import run_pass_2  # type: ignore
else:
    from .karmalego_runtime_common import read_run_config, write_json
    from .prepare_kl_run import run_prepare
    from .pass_1 import run_pass_1
    from .pass_2 import run_pass_2


def run_sequence(config_path: Path | None = None) -> dict:
    sequence_start = time.perf_counter()
    config = read_run_config(config_path)
    mode = str(config.get("mode", "full_run")).strip().lower()
    input_package_dir = Path(str(config["input_package_dir"]))
    output_dir = Path(str(config["output_dir"]))
    domain_name = str(config.get("candidatecount_domain_name", "")).strip()
    use_fast_count_engine = bool(config.get("use_fast_count_engine", False))
    pass_1_max_parallel = max(1, int(config.get("pass_1_max_parallel", 1)))
    pass_1_emit_karmalego_output = bool(config.get("pass_1_emit_karmalego_output", False))

    print(f"[Sequence] mode={mode}")
    print(f"[Sequence] input_package_dir={input_package_dir}")
    print(f"[Sequence] output_dir={output_dir}")

    result: dict = {
        "mode": mode,
        "input_package_dir": str(input_package_dir),
        "output_dir": str(output_dir),
    }
    pass_1_result: dict | None = None
    pass_2_result: dict | None = None
    pass_1_runtime_minutes: float | None = None
    pass_2_runtime_minutes: float | None = None

    if mode == "full_run":
        print("[Sequence] Running PREPARE stage...")
        run_prepare(input_package_dir)
        print("[Sequence] PREPARE stage finished.")
        print("[Sequence] START Pass 1")
        pass_1_start = time.perf_counter()
        pass_1_result = run_pass_1(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            max_parallel=pass_1_max_parallel,
            emit_karmalego_output=pass_1_emit_karmalego_output,
        )
        pass_1_runtime_minutes = round((time.perf_counter() - pass_1_start) / 60, 3)
        print("[Sequence] END Pass 1")
        if not domain_name:
            raise ValueError("candidatecount_domain_name is required for mode 'full_run'.")
        print("[Sequence] START Pass 2")
        pass_2_start = time.perf_counter()
        pass_2_result = run_pass_2(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            domain_name=domain_name,
            use_fast_count_engine=use_fast_count_engine,
        )
        pass_2_runtime_minutes = round((time.perf_counter() - pass_2_start) / 60, 3)
        print("[Sequence] END Pass 2")
    elif mode == "run_pass_1_2":
        print("[Sequence] Running Pass 1 + Pass 2...")
        print("[Sequence] START Pass 1")
        pass_1_start = time.perf_counter()
        pass_1_result = run_pass_1(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            max_parallel=pass_1_max_parallel,
            emit_karmalego_output=pass_1_emit_karmalego_output,
        )
        pass_1_runtime_minutes = round((time.perf_counter() - pass_1_start) / 60, 3)
        print("[Sequence] END Pass 1")
        if not domain_name:
            raise ValueError("candidatecount_domain_name is required for mode 'run_pass_1_2'.")
        print("[Sequence] START Pass 2")
        pass_2_start = time.perf_counter()
        pass_2_result = run_pass_2(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            domain_name=domain_name,
            use_fast_count_engine=use_fast_count_engine,
        )
        pass_2_runtime_minutes = round((time.perf_counter() - pass_2_start) / 60, 3)
        print("[Sequence] END Pass 2")
    elif mode == "run_pass_1":
        print("[Sequence] Running Pass 1 only...")
        print("[Sequence] START Pass 1")
        pass_1_start = time.perf_counter()
        pass_1_result = run_pass_1(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            max_parallel=pass_1_max_parallel,
            emit_karmalego_output=pass_1_emit_karmalego_output,
        )
        pass_1_runtime_minutes = round((time.perf_counter() - pass_1_start) / 60, 3)
        print("[Sequence] END Pass 1")
    elif mode == "run_pass_2":
        print("[Sequence] Running Pass 2 only...")
        if not domain_name:
            raise ValueError("candidatecount_domain_name is required for mode 'run_pass_2'.")
        print("[Sequence] START Pass 2")
        pass_2_start = time.perf_counter()
        pass_2_result = run_pass_2(
            input_package_dir=input_package_dir,
            output_dir=output_dir,
            domain_name=domain_name,
            use_fast_count_engine=use_fast_count_engine,
        )
        pass_2_runtime_minutes = round((time.perf_counter() - pass_2_start) / 60, 3)
        print("[Sequence] END Pass 2")
    else:
        raise ValueError(
            "Unsupported mode. Expected one of: 'full_run', 'run_pass_1_2', 'run_pass_1', 'run_pass_2'."
        )

    if pass_1_result is not None:
        result["pass_1_result"] = pass_1_result
        result["pass_1_runtime_minutes"] = pass_1_runtime_minutes
    if pass_2_result is not None:
        result["pass_2_result"] = pass_2_result
        result["pass_2_runtime_minutes"] = pass_2_runtime_minutes
    total_runtime_minutes = round((time.perf_counter() - sequence_start) / 60, 3)
    result["total_runtime_minutes"] = total_runtime_minutes
    write_json(result, output_dir / "sequence_result.json")
    if pass_1_runtime_minutes is not None:
        print(f"[Sequence] Pass 1 runtime: {pass_1_runtime_minutes} minutes")
    if pass_2_runtime_minutes is not None:
        print(f"[Sequence] Pass 2 runtime: {pass_2_runtime_minutes} minutes")
    print(f"[Sequence] Total runtime: {total_runtime_minutes} minutes")
    print("[Sequence] Done. sequence_result.json written.")
    return result


if __name__ == "__main__":
    run_sequence()

