#!/usr/bin/env python3
"""Internal implementation for launch_app.cmd."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_APP_SCRIPT = SCRIPT_DIR / "xgd_web_app.py"
DEFAULT_CONFIG_CANDIDATES = (
    SCRIPT_DIR / "app_data" / "launcher_config.json",
    SCRIPT_DIR / "launcher_config.json",
)


def log(message: str) -> None:
    print(f"[launch_app] {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Launch xgd_web_app.py by using 'conda run'. "
            "Conda env details can be passed directly or read from a JSON config."
        )
    )
    parser.add_argument(
        "--config",
        help=(
            "Optional JSON config path. If omitted, launcher tries defaults: "
            "app_data/launcher_config.json then launcher_config.json."
        ),
    )
    parser.add_argument(
        "--conda-env",
        help="Conda environment name or path. Overrides config values when provided.",
    )
    parser.add_argument(
        "--conda-exe",
        help="Conda executable to use (default: config value or 'conda').",
    )
    parser.add_argument(
        "--app-script",
        default=str(DEFAULT_APP_SCRIPT),
        help=f"App script to run (default: {DEFAULT_APP_SCRIPT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the final command without launching the app.",
    )
    parser.add_argument(
        "app_args",
        nargs=argparse.REMAINDER,
        help="Arguments passed through to xgd_web_app.py. Use '--' before app args.",
    )
    return parser.parse_args()


def resolve_path(path_value: str | Path, base_dir: Path | None = None) -> Path:
    raw = Path(path_value).expanduser()
    if raw.is_absolute():
        return raw.resolve()

    candidates: list[Path] = []
    if base_dir is not None:
        candidates.append((base_dir / raw).resolve())
    candidates.append((Path.cwd() / raw).resolve())
    candidates.append((SCRIPT_DIR / raw).resolve())

    for candidate in candidates:
        if candidate.exists():
            return candidate

    if candidates:
        return candidates[0]
    return raw.resolve()


def load_json_config(path_value: str) -> tuple[dict[str, Any], Path]:
    config_path = resolve_path(path_value)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError("Config root must be a JSON object.")
    return data, config_path


def find_default_config_path() -> Path | None:
    for candidate in DEFAULT_CONFIG_CANDIDATES:
        if candidate.exists():
            return candidate
    return None


def first_non_empty(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def get_launcher_config(config_data: dict[str, Any]) -> dict[str, Any]:
    launcher = config_data.get("launcher")
    if isinstance(launcher, dict):
        return launcher
    return config_data


def parse_app_args(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        return [str(item) for item in raw_value]
    if isinstance(raw_value, str):
        return shlex.split(raw_value, posix=(os.name != "nt"))
    raise ValueError("Config key 'app_args' must be a list or string.")


def parse_env_vars(raw_value: Any) -> dict[str, str]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError("Config key 'env_vars' must be an object of key/value pairs.")
    out: dict[str, str] = {}
    for key, value in raw_value.items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def strip_remainder_marker(args: list[str]) -> list[str]:
    if args and args[0] == "--":
        return args[1:]
    return args


def looks_like_path(value: str) -> bool:
    text = str(value).strip()
    if not text:
        return False
    if any(sep in text for sep in ("/", "\\")):
        return True
    if text.startswith("."):
        return True
    if len(text) > 1 and text[1] == ":":
        return True
    return Path(text).exists()


def resolve_conda_exe(explicit_conda_exe: Any, config_dir: Path | None = None) -> str:
    explicit_text = str(explicit_conda_exe).strip() if explicit_conda_exe is not None else ""
    if explicit_text:
        if looks_like_path(explicit_text):
            explicit_path = resolve_path(explicit_text, base_dir=config_dir)
            if not explicit_path.exists():
                raise FileNotFoundError(f"Conda executable not found: {explicit_path}")
            return str(explicit_path)

        resolved = shutil.which(explicit_text)
        if not resolved:
            raise FileNotFoundError(
                f"Conda executable '{explicit_text}' not found in PATH."
            )
        return resolved

    conda_exe_env = os.environ.get("CONDA_EXE")
    if conda_exe_env:
        env_path = Path(conda_exe_env).expanduser()
        if env_path.exists():
            return str(env_path.resolve())

    from_path = shutil.which("conda")
    if from_path:
        return from_path

    home = Path.home()
    candidates: list[Path] = []
    if os.name == "nt":
        program_data = Path(os.environ.get("ProgramData", ""))
        local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
        roots = [
            home / "miniconda3",
            home / "anaconda3",
            home / "mambaforge",
            home / "miniforge3",
            local_app_data / "miniconda3",
            local_app_data / "anaconda3",
            local_app_data / "mambaforge",
            local_app_data / "miniforge3",
            program_data / "miniconda3",
            program_data / "anaconda3",
            program_data / "mambaforge",
            program_data / "miniforge3",
        ]
        for root in roots:
            candidates.extend(
                [
                    root / "Scripts" / "conda.exe",
                    root / "condabin" / "conda.bat",
                ]
            )
    else:
        roots = [
            home / "miniconda3",
            home / "anaconda3",
            home / "mambaforge",
            home / "miniforge3",
            Path("/opt/conda"),
        ]
        for root in roots:
            candidates.append(root / "bin" / "conda")

    for candidate in candidates:
        if candidate.exists():
            return str(candidate.resolve())

    raise FileNotFoundError(
        "Could not locate a conda executable. Set '--conda-exe', set CONDA_EXE, "
        "or add conda to PATH."
    )


def resolve_conda_env(conda_env: Any, config_dir: Path | None = None) -> str:
    env_text = str(conda_env).strip()
    if not env_text:
        raise ValueError("Conda env cannot be empty.")
    if not looks_like_path(env_text):
        return env_text
    env_path = resolve_path(env_text, base_dir=config_dir)
    return str(env_path)


def get_conda_root_from_exe(conda_exe: str) -> Path | None:
    conda_path = Path(conda_exe).expanduser()
    if not conda_path.exists():
        return None

    parent = conda_path.parent
    parent_name = parent.name.lower()
    if parent_name in {"scripts", "condabin", "bin"}:
        root = parent.parent
    else:
        root = parent
    if root.exists():
        return root.resolve()
    return None


def resolve_env_python(conda_env: str, conda_exe: str) -> Path | None:
    if looks_like_path(conda_env):
        env_root = Path(conda_env).expanduser()
    else:
        conda_root = get_conda_root_from_exe(conda_exe)
        if conda_root is None:
            return None
        env_root = conda_root / "envs" / conda_env

    if os.name == "nt":
        python_path = env_root / "python.exe"
    else:
        python_path = env_root / "bin" / "python"
    if python_path.exists():
        return python_path.resolve()
    return None


def build_conda_command(
    conda_exe: str,
    conda_env: str,
    app_script: Path,
    app_args: list[str],
) -> list[str]:
    command = [conda_exe, "run", "--no-capture-output"]
    if looks_like_path(conda_env):
        command.extend(["-p", conda_env])
    else:
        command.extend(["-n", conda_env])
    command.extend(["python", "-u", str(app_script)])
    command.extend(app_args)
    return command


def format_command(command: list[str]) -> str:
    if os.name == "nt":
        return subprocess.list2cmdline(command)
    return shlex.join(command)


def stop_child_process(proc: subprocess.Popen[Any], timeout_seconds: float = 8.0) -> None:
    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout_seconds)
        return
    except Exception:
        pass

    if proc.poll() is not None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=3.0)
        return
    except Exception:
        pass

    if proc.poll() is not None:
        return
    try:
        proc.kill()
        proc.wait(timeout=3.0)
    except Exception:
        pass


def run_app_command(command: list[str], run_env: dict[str, str]) -> int:
    process = subprocess.Popen(
        command,
        cwd=str(SCRIPT_DIR),
        env=run_env,
    )
    try:
        return int(process.wait())
    except KeyboardInterrupt:
        log("Stop requested (Ctrl+C). Stopping app...")
        # Give the child a brief moment to handle Ctrl+C itself.
        for _ in range(30):
            if process.poll() is not None:
                return 130
            time.sleep(0.1)
        stop_child_process(process)
        return 130


def main() -> int:
    log("Stage 1/4: Loading launcher settings...")
    args = parse_args()

    config_data: dict[str, Any] = {}
    config_dir: Path | None = None
    config_path: Path | None = None
    if args.config:
        config_data, config_path = load_json_config(args.config)
    else:
        default_config = find_default_config_path()
        if default_config is not None:
            config_data, config_path = load_json_config(str(default_config))

    if config_path is not None:
        config_dir = config_path.parent
        log(f"Using config file: {config_path}")
    else:
        log("No config file found; using CLI values and defaults.")

    log("Stage 2/4: Resolving conda environment...")
    launcher_config = get_launcher_config(config_data)
    conda_config = launcher_config.get("conda") if isinstance(launcher_config.get("conda"), dict) else {}

    conda_env = first_non_empty(
        args.conda_env,
        launcher_config.get("conda_env_path"),
        conda_config.get("path"),
        launcher_config.get("conda_env"),
        launcher_config.get("conda_env_name"),
        conda_config.get("env"),
        conda_config.get("name"),
    )
    if not conda_env:
        raise ValueError(
            "No conda env provided. Set '--conda-env' or include one in config "
            "(launcher.conda_env / launcher.conda.env / launcher.conda.path). "
            "Default config location: app_data/launcher_config.json"
        )
    conda_env = resolve_conda_env(conda_env, config_dir=config_dir)
    log(f"Conda env resolved: {conda_env}")

    explicit_conda_exe = first_non_empty(
        args.conda_exe,
        launcher_config.get("conda_exe"),
        conda_config.get("exe"),
    )
    conda_exe = resolve_conda_exe(explicit_conda_exe, config_dir=config_dir)
    log(f"Conda executable: {conda_exe}")

    log("Stage 3/4: Preparing app command...")
    app_script = resolve_path(args.app_script, base_dir=config_dir)
    if not app_script.exists():
        raise FileNotFoundError(f"App script not found: {app_script}")

    config_app_args = parse_app_args(launcher_config.get("app_args"))
    cli_app_args = strip_remainder_marker(list(args.app_args))
    final_app_args = cli_app_args if cli_app_args else config_app_args

    raw_env_vars = launcher_config.get("env_vars")
    if raw_env_vars is None:
        raw_env_vars = launcher_config.get("env")
    env_vars = parse_env_vars(raw_env_vars)

    env_python = resolve_env_python(str(conda_env), conda_exe)
    if env_python is not None:
        command = [str(env_python), "-u", str(app_script), *final_app_args]
        log(f"Launch mode: direct env python ({env_python})")
    else:
        command = build_conda_command(
            conda_exe=conda_exe,
            conda_env=str(conda_env),
            app_script=app_script,
            app_args=final_app_args,
        )
        log("Launch mode: conda run")

    if args.dry_run:
        log("Dry run requested. Final command:")
        print(format_command(command))
        return 0

    run_env = os.environ.copy()
    run_env.update(env_vars)

    log("Stage 4/4: Launching app (live logs below)...")
    log("Tip: press Ctrl+C to stop the app.")
    return run_app_command(command, run_env)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        log("Stopped by user.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"Launcher error: {exc}", file=sys.stderr)
        raise SystemExit(1)
