from __future__ import annotations

import csv
import json
import re
import shutil
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import pyodbc

from . import settings


@dataclass(frozen=True)
class InputPackage:
    root: Path
    data_csv_gz: Path
    taks_dir: Path
    batch_configs: list[Path]


@dataclass(frozen=True)
class RuntimePaths:
    root: Path
    config_dir: Path
    results_dir: Path
    logs_dir: Path
    config_file: Path
    appsettings_path: Path


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def empty_dir(path: Path) -> None:
    if not path.exists():
        return
    for item in path.iterdir():
        if item.is_file():
            item.unlink()
        else:
            shutil.rmtree(item)


def write_json(data: dict | list, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def get_pass_1_worker_runtime_paths(worker_id: int) -> RuntimePaths:
    if worker_id < 1:
        raise ValueError("worker_id must be >= 1")
    root = settings.RUNTIME_ROOT / f"w{worker_id}"
    return _build_runtime_paths(root)


def get_pass_2_runtime_paths() -> RuntimePaths:
    return _build_runtime_paths(settings.PASS_2_RUNTIME_ROOT)


def _build_runtime_paths(root: Path) -> RuntimePaths:
    return RuntimePaths(
        root=root,
        config_dir=root / "KarmalegoConfigPath",
        results_dir=root / "ResultsPath",
        logs_dir=root / "LogsPath",
        config_file=root / "KarmalegoConfigPath" / "config_file.json",
        appsettings_path=root / "appsettings.json",
    )


def read_run_config(path: Path | None = None) -> dict:
    config_path = path or settings.DEFAULT_RUN_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(f"Run config not found: {config_path}")
    # utf-8-sig supports both regular UTF-8 and UTF-8 with BOM.
    return json.loads(config_path.read_text(encoding="utf-8-sig"))


def get_connection() -> pyodbc.Connection:
    return pyodbc.connect(settings.DB_CONNECTION_STRING)


def table_is_empty(table_name: str) -> bool:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT TOP 1 1 FROM {table_name};")
        return cur.fetchone() is None


def truncate_table(table_name: str) -> None:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(f"TRUNCATE TABLE {table_name};")
        conn.commit()


def prepare_tables() -> None:
    if not table_is_empty(settings.INPUT_PATIENTS_TABLE):
        print(f"[Prepare] Truncating table: {settings.INPUT_PATIENTS_TABLE}")
        truncate_table(settings.INPUT_PATIENTS_TABLE)
    if not table_is_empty(settings.KNOWLEDGE_TABLE):
        print(f"[Prepare] Truncating table: {settings.KNOWLEDGE_TABLE}")
        truncate_table(settings.KNOWLEDGE_TABLE)


def load_csv_gz_to_input_table(csv_gz_path: Path) -> None:
    print(f"[Prepare] Loading dataset: {csv_gz_path}")
    total_rows = 0
    for chunk_idx, chunk in enumerate(
        pd.read_csv(
            csv_gz_path,
            compression="gzip",
            chunksize=settings.CSV_CHUNK_SIZE,
            dtype={"StartTime": "string", "EndTime": "string"},
        ),
        start=1,
    ):
        with get_connection() as conn:
            cursor = conn.cursor()
            cols = list(chunk.columns)
            col_list = ", ".join(f"[{c}]" for c in cols)
            placeholders = ", ".join("?" for _ in cols)
            sql = f"INSERT INTO {settings.INPUT_PATIENTS_TABLE} ({col_list}) VALUES ({placeholders})"
            cursor.fast_executemany = True
            cursor.executemany(sql, chunk.itertuples(index=False, name=None))
            conn.commit()
        total_rows += len(chunk)
        print(f"[Prepare] Chunk {chunk_idx} loaded (rows={len(chunk)}, total={total_rows})")


def copy_taks_to_destination(taks_source_dir: Path) -> None:
    print(f"[Prepare] Copying TAKs from: {taks_source_dir}")
    ensure_dir(settings.TAKS_DEST_FOLDER)
    empty_dir(settings.TAKS_DEST_FOLDER)
    copied = 0
    for item in taks_source_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, settings.TAKS_DEST_FOLDER / item.name)
            copied += 1
    print(f"[Prepare] TAK files copied: {copied}")


def run_mediator_populate() -> None:
    print("[Prepare] Running Mediator PopulateKnowledgeTable...")
    cmd = [str(settings.MEDIATOR_API_EXE), "Admin", "PopulateKnowledgeTable", settings.PROJECT_ID]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            "Mediator populate failed.\n"
            f"Exit: {result.returncode}\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    print("[Prepare] Mediator populate finished.")


def ensure_runtime_dirs(runtime_paths: RuntimePaths) -> None:
    ensure_dir(runtime_paths.root)
    ensure_dir(runtime_paths.config_dir)
    ensure_dir(runtime_paths.results_dir)
    ensure_dir(runtime_paths.logs_dir)


def clean_runtime_work_dirs(runtime_paths: RuntimePaths) -> None:
    ensure_runtime_dirs(runtime_paths)
    empty_dir(runtime_paths.config_dir)
    empty_dir(runtime_paths.results_dir)
    empty_dir(runtime_paths.logs_dir)


def copy_batch_config_to_runtime(config_path: Path, runtime_paths: RuntimePaths) -> Path:
    ensure_runtime_dirs(runtime_paths)
    empty_dir(runtime_paths.config_dir)
    shutil.copy2(config_path, runtime_paths.config_file)
    return runtime_paths.config_file


def write_runtime_config(config: dict, runtime_paths: RuntimePaths) -> Path:
    ensure_runtime_dirs(runtime_paths)
    empty_dir(runtime_paths.config_dir)
    write_json(config, runtime_paths.config_file)
    return runtime_paths.config_file


def _strip_json_comments(text: str) -> str:
    """Allow parsing appsettings files that include // or /* */ comments."""
    result: list[str] = []
    i = 0
    in_string = False
    in_line_comment = False
    in_block_comment = False
    escape = False

    while i < len(text):
        ch = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                result.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if in_string:
            result.append(ch)
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            i += 1
            continue

        if ch == '"':
            in_string = True
            result.append(ch)
            i += 1
            continue

        if ch == "/" and nxt == "/":
            in_line_comment = True
            i += 2
            continue

        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        result.append(ch)
        i += 1

    return "".join(result)


def get_karmalego_appsettings_path() -> Path:
    return settings.KARMALEGO_EXE.parent / "appsettings.json"


def _load_appsettings_json(appsettings_path: Path) -> dict:
    if not appsettings_path.exists():
        raise FileNotFoundError(f"KarmaLego appsettings not found: {appsettings_path}")
    raw = appsettings_path.read_text(encoding="utf-8-sig")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # Some appsettings files are JSON-with-comments. Strip comments and retry.
        data = json.loads(_strip_json_comments(raw))
    return data


def _set_karmalego_appsettings_values(
    data: dict,
    *,
    config_path: Path | None = None,
    results_path: Path | None = None,
    logs_path: Path | None = None,
    version_type: str | None = None,
) -> None:
    app_settings = data.setdefault("AppSettings", {})
    if not isinstance(app_settings, dict):
        raise RuntimeError("AppSettings section is not an object in appsettings.json")
    if config_path is not None:
        app_settings["KarmalegoConfigPath"] = str(config_path)
    if results_path is not None:
        app_settings["ResultsPath"] = str(results_path)
    if logs_path is not None:
        app_settings["LogsPath"] = str(logs_path)
    if version_type is not None:
        app_settings["VersionType"] = version_type


def write_runtime_appsettings(runtime_paths: RuntimePaths, version_type: str) -> Path:
    ensure_runtime_dirs(runtime_paths)
    data = _load_appsettings_json(get_karmalego_appsettings_path())
    _set_karmalego_appsettings_values(
        data,
        config_path=runtime_paths.config_file,
        results_path=runtime_paths.results_dir,
        logs_path=runtime_paths.logs_dir,
        version_type=version_type,
    )
    runtime_paths.appsettings_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return runtime_paths.appsettings_path


def set_karmalego_version_type(version_type: str, appsettings_path: Path | None = None) -> Path:
    target_path = appsettings_path or get_karmalego_appsettings_path()
    data = _load_appsettings_json(target_path)
    _set_karmalego_appsettings_values(data, version_type=version_type)
    target_path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    return target_path


def run_karmalego(
    stream_output: bool = False,
    appsettings_path: Path | None = None,
    emit_output: bool = True,
) -> None:
    if not settings.KARMALEGO_EXE.exists():
        raise FileNotFoundError(f"KarmaLego EXE not found: {settings.KARMALEGO_EXE}")
    cmd = [str(settings.KARMALEGO_EXE)]
    if appsettings_path is not None:
        cmd.extend(["--appsettings", str(appsettings_path)])
    if stream_output:
        collected_output: list[str] = []
        if emit_output:
            print("[Python->KarmaLego output]")
        with subprocess.Popen(
            cmd,
            cwd=str(settings.KARMALEGO_EXE.parent),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            bufsize=1,
        ) as proc:
            assert proc.stdout is not None
            for line in proc.stdout:
                text = line.rstrip("\n")
                collected_output.append(text)
                if emit_output:
                    print(text)
            exit_code = proc.wait()
        stdout = "\n".join(collected_output)
        stderr = ""
    else:
        result = subprocess.run(
            cmd,
            cwd=str(settings.KARMALEGO_EXE.parent),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
        )
        stdout = result.stdout or ""
        stderr = result.stderr or ""
        exit_code = result.returncode
        if emit_output and stdout.strip():
            print("[Python->KarmaLego stdout]")
            print(stdout.rstrip())
        if emit_output and stderr.strip():
            print("[Python->KarmaLego stderr]")
            print(stderr.rstrip())
    known_error_markers = (
        "An error occurred during execution",
        "An error occurred while running KarmaLego",
        "does not exist:",
    )
    if any(marker in stdout for marker in known_error_markers):
        raise RuntimeError(
            "KarmaLego reported an error in stdout.\n"
            f"Exit: {exit_code}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    if any(marker in stdout for marker in settings.KARMALEGO_SUCCESS_MARKERS):
        return
    if exit_code != 0:
        raise RuntimeError(
            "KarmaLego failed.\n"
            f"Exit: {exit_code}\nSTDOUT:\n{stdout}\nSTDERR:\n{stderr}"
        )
    if emit_output:
        print("[Run] KarmaLego finished (zero exit code).")


def _copy_tree_contents(src: Path, dst: Path) -> None:
    ensure_dir(dst)
    if not src.exists():
        return
    for item in src.iterdir():
        if item.is_file():
            shutil.copy2(item, dst / item.name)
        else:
            shutil.copytree(item, dst / item.name, dirs_exist_ok=True)


def archive_runtime_outputs(
    archive_root: Path,
    batch_id: str,
    results_source: Path,
    logs_source: Path,
) -> Path:
    batch_root = archive_root / batch_id
    results_dst = batch_root / "results"
    logs_dst = batch_root / "logs"
    _copy_tree_contents(results_source, results_dst)
    _copy_tree_contents(logs_source, logs_dst)
    return batch_root


def _extract_from_patterns_support(file_path: Path) -> set[str]:
    names: set[str] = set()
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("pattern_name") or "").strip()
            if name:
                names.add(name)
    return names


def _extract_metadata_from_patterns_support(file_path: Path) -> dict[str, dict[str, int | str | None]]:
    metadata: dict[str, dict[str, int | str | None]] = {}
    with file_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("pattern_name") or "").strip()
            if not name:
                continue
            level_raw = (row.get("level") or "").strip()
            parent_name = (row.get("parent_name") or "").strip()
            relation_pairs_json = (row.get("relation_pairs_json") or "").strip()
            components_json = (row.get("components_json") or "").strip()
            level: int | None = None
            if level_raw:
                try:
                    level = int(float(level_raw))
                except ValueError:
                    level = None

            if name not in metadata:
                metadata[name] = {
                    "level": level,
                    "parent_name": parent_name,
                    "relation_pairs_json": relation_pairs_json,
                    "components_json": components_json,
                }
                continue
            prev = metadata[name]
            prev_level = prev.get("level")
            prev_parent = str(prev.get("parent_name") or "")
            prev_relation_pairs = str(prev.get("relation_pairs_json") or "")
            prev_components = str(prev.get("components_json") or "")
            resolved_level = prev_level
            if prev_level is None and level is not None:
                resolved_level = level
            elif prev_level is not None and level is not None:
                resolved_level = min(prev_level, level)

            resolved_parent = prev_parent or parent_name
            resolved_relation_pairs = prev_relation_pairs or relation_pairs_json
            resolved_components = prev_components or components_json
            metadata[name] = {
                "level": resolved_level,
                "parent_name": resolved_parent,
                "relation_pairs_json": resolved_relation_pairs,
                "components_json": resolved_components,
            }
    return metadata


def _extract_from_results_csv_header(file_path: Path) -> set[str]:
    text = file_path.read_text(encoding="utf-8")
    first_line = text.splitlines()[0] if text.splitlines() else ""
    cols = [c.strip() for c in first_line.split(",")]
    ignore = {"id", "class_name", "value"}
    return {c for c in cols if c and c not in ignore}


def extract_pattern_names_from_results(results_root: Path) -> set[str]:
    names: set[str] = set()
    support_files = list(results_root.rglob("patterns_support.csv"))
    for file_path in support_files:
        names |= _extract_from_patterns_support(file_path)
    if names:
        return names

    results_csv_files = list(results_root.rglob("results.csv"))
    for file_path in results_csv_files:
        names |= _extract_from_results_csv_header(file_path)
    return names


def extract_pattern_metadata_from_results(results_root: Path) -> dict[str, dict[str, int | str | None]]:
    metadata: dict[str, dict[str, int | str | None]] = {}

    support_files = list(results_root.rglob("patterns_support.csv"))
    for file_path in support_files:
        file_metadata = _extract_metadata_from_patterns_support(file_path)
        for name, row in file_metadata.items():
            level = row.get("level")
            parent_name = str(row.get("parent_name") or "")
            relation_pairs_json = str(row.get("relation_pairs_json") or "")
            components_json = str(row.get("components_json") or "")
            if name not in metadata:
                metadata[name] = {
                    "level": level,
                    "parent_name": parent_name,
                    "relation_pairs_json": relation_pairs_json,
                    "components_json": components_json,
                }
                continue
            prev = metadata[name]
            prev_level = prev.get("level")
            prev_parent = str(prev.get("parent_name") or "")
            prev_relation_pairs = str(prev.get("relation_pairs_json") or "")
            prev_components = str(prev.get("components_json") or "")
            resolved_level = prev_level
            if prev_level is None and level is not None:
                resolved_level = level
            elif prev_level is not None and level is not None:
                resolved_level = min(prev_level, level)
            resolved_parent = prev_parent or parent_name
            resolved_relation_pairs = prev_relation_pairs or relation_pairs_json
            resolved_components = prev_components or components_json
            metadata[name] = {
                "level": resolved_level,
                "parent_name": resolved_parent,
                "relation_pairs_json": resolved_relation_pairs,
                "components_json": resolved_components,
            }

    if metadata:
        return metadata

    results_csv_files = list(results_root.rglob("results.csv"))
    for file_path in results_csv_files:
        for name in _extract_from_results_csv_header(file_path):
            metadata.setdefault(
                name,
                {
                    "level": None,
                    "parent_name": "",
                    "relation_pairs_json": "",
                    "components_json": "",
                },
            )
    return metadata


def parse_batch_number(name: str) -> int:
    match = re.search(r"_batch_(\d+)", name)
    return int(match.group(1)) if match else 10**9


def validate_input_package(input_package_dir: Path) -> InputPackage:
    if not input_package_dir.exists():
        raise FileNotFoundError(f"Input package dir not found: {input_package_dir}")

    data_dir = input_package_dir / "data"
    taks_dir = input_package_dir / "taks"
    batch_configs_dir = input_package_dir / "batch_configs"
    if not data_dir.exists():
        raise FileNotFoundError(f"Missing data dir: {data_dir}")
    if not taks_dir.exists():
        raise FileNotFoundError(f"Missing taks dir: {taks_dir}")
    if not batch_configs_dir.exists():
        raise FileNotFoundError(f"Missing batch_configs dir: {batch_configs_dir}")

    data_files = sorted(data_dir.glob("*.csv.gz"))
    if len(data_files) != 1:
        raise RuntimeError(f"Expected exactly 1 data file (*.csv.gz) in {data_dir}, found {len(data_files)}")

    batch_configs = sorted(batch_configs_dir.glob("*.json"), key=lambda p: (parse_batch_number(p.stem), p.name))
    if not batch_configs:
        raise RuntimeError(f"No batch config json files found in {batch_configs_dir}")

    return InputPackage(
        root=input_package_dir,
        data_csv_gz=data_files[0],
        taks_dir=taks_dir,
        batch_configs=batch_configs,
    )
