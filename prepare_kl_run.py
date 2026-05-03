from __future__ import annotations

from pathlib import Path

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from kl_2_passes_batch_run.karmalego_runtime_common import (  # type: ignore
        clean_runtime_work_dirs,
        copy_taks_to_destination,
        get_pass_1_worker_runtime_paths,
        get_pass_2_runtime_paths,
        ensure_runtime_dirs,
        load_csv_to_input_table,
        prepare_tables,
        run_mediator_populate,
        validate_input_package,
    )
else:
    from .karmalego_runtime_common import (
        clean_runtime_work_dirs,
        copy_taks_to_destination,
        get_pass_1_worker_runtime_paths,
        get_pass_2_runtime_paths,
        ensure_runtime_dirs,
        load_csv_to_input_table,
        prepare_tables,
        run_mediator_populate,
        validate_input_package,
    )


def run_prepare(input_package_dir: Path) -> None:
    input_package = validate_input_package(input_package_dir)
    pass_1_runtime = get_pass_1_worker_runtime_paths(1)
    pass_2_runtime = get_pass_2_runtime_paths()
    ensure_runtime_dirs(pass_1_runtime)
    ensure_runtime_dirs(pass_2_runtime)
    clean_runtime_work_dirs(pass_1_runtime)
    clean_runtime_work_dirs(pass_2_runtime)
    prepare_tables()
    copy_taks_to_destination(input_package.taks_dir)
    load_csv_to_input_table(input_package.data_file)
    run_mediator_populate()

