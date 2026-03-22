from __future__ import annotations

import configparser
import os
from dataclasses import dataclass, field
from importlib import resources
from pathlib import Path
from typing import Dict

from wevra.models import RuntimeBackend

EXAMPLE_FILENAMES = {
    "wevra.ini": "wevra.ini.example",
    "agents.ini": "agents.ini.example",
    ".env": ".env.example",
}


ROLE_DEFAULTS = {
    "coordinator": {"runtime": RuntimeBackend.MOCK, "count": 1},
    "planner": {"runtime": RuntimeBackend.MOCK, "count": 1},
    "investigation": {"runtime": RuntimeBackend.MOCK, "count": 1},
    "analyst": {"runtime": RuntimeBackend.MOCK, "count": 1},
    "tester": {"runtime": RuntimeBackend.MOCK, "count": 1},
    "implementer": {"runtime": RuntimeBackend.MOCK, "count": 4},
    "reviewer": {"runtime": RuntimeBackend.MOCK, "count": 2},
}


CAPABILITY_TO_ROLE = {
    "planner": "planner",
    "implementation": "implementer",
    "rework": "implementer",
    "implementer": "implementer",
    "investigation": "investigation",
    "analyst": "analyst",
    "tester": "tester",
    "reviewer": "reviewer",
}


LOOPBACK_HOST = "127.0.0.1"


@dataclass
class RoleConfig:
    name: str
    runtime: RuntimeBackend
    model: str = ""
    count: int = 1


@dataclass
class AppConfig:
    repo_root: Path
    working_dir: Path
    db_path: Path
    language: str
    runtime_home: Path | None
    agent_timeout_seconds: int
    ui_port: int
    ui_auto_start: bool
    ui_open_browser: bool
    ui_language: str
    notifications: Dict[str, bool] = field(default_factory=dict)
    env: Dict[str, str] = field(default_factory=dict)
    roles: Dict[str, RoleConfig] = field(default_factory=dict)

    def role_for(self, capability: str) -> RoleConfig:
        role_name = CAPABILITY_TO_ROLE.get(capability, capability)
        if role_name in self.roles:
            return self.roles[role_name]
        default = ROLE_DEFAULTS.get("implementer")
        return RoleConfig(
            name=role_name,
            runtime=default["runtime"],
            count=default["count"],
        )


def normalize_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def read_simple_env(path: Path) -> Dict[str, str]:
    if not path.is_file():
        return {}
    data: Dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip().strip('"').strip("'")
    return data


def resolve_optional_config_path(value: str, repo_root: Path) -> Path | None:
    raw = value.strip()
    if not raw:
        return None
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = (repo_root / path).resolve()
    return path


def template_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def read_example_template(filename: str) -> str:
    example_name = EXAMPLE_FILENAMES[filename]
    template_path = template_repo_root() / example_name
    if template_path.is_file():
        return template_path.read_text(encoding="utf-8")
    return (
        resources.files("wevra").joinpath(f"templates/{example_name}").read_text(encoding="utf-8")
    )


def init_repo_config(repo_root: Path) -> Dict[str, str]:
    repo_root = repo_root.resolve()
    created: Dict[str, str] = {}
    for target_name in EXAMPLE_FILENAMES:
        path = repo_root / target_name
        if path.exists():
            continue
        path.write_text(read_example_template(target_name), encoding="utf-8")
        created[path.name] = str(path)
    return created


def load_config(repo_root: Path) -> AppConfig:
    repo_root = repo_root.resolve()
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(repo_root / "wevra.ini", encoding="utf-8")

    agents = configparser.ConfigParser(interpolation=None)
    agents.read(repo_root / "agents.ini", encoding="utf-8")

    env_file = read_simple_env(repo_root / ".env")
    merged_env = dict(env_file)
    merged_env.update({key: value for key, value in os.environ.items() if key not in merged_env})

    db_path_raw = (
        parser.get("runtime", "db_path", fallback=".wevra/wevra.db").strip() or ".wevra/wevra.db"
    )
    runtime_home = resolve_optional_config_path(
        parser.get("runtime", "home", fallback=""),
        repo_root,
    )
    agent_timeout_seconds = max(
        parser.getint("runtime", "agent_timeout_seconds", fallback=1800),
        1,
    )

    working_dir = repo_root

    db_path = Path(db_path_raw)
    if not db_path.is_absolute():
        db_path = (repo_root / db_path).resolve()

    notifications = {
        "question_opened": normalize_bool(
            parser.get("notification", "question_opened", fallback="false")
        ),
        "workflow_completed": normalize_bool(
            parser.get("notification", "workflow_completed", fallback="false")
        ),
    }

    roles: Dict[str, RoleConfig] = {}
    for name, defaults in ROLE_DEFAULTS.items():
        runtime_raw = (
            agents.get(name, "runtime", fallback=str(defaults["runtime"].value)).strip().lower()
        )
        model = agents.get(name, "model", fallback="").strip()
        count = max(agents.getint(name, "count", fallback=defaults["count"]), 1)
        runtime = RuntimeBackend(runtime_raw) if runtime_raw else defaults["runtime"]
        roles[name] = RoleConfig(name=name, runtime=runtime, model=model, count=count)

    if not working_dir.exists():
        working_dir.mkdir(parents=True, exist_ok=True)

    return AppConfig(
        repo_root=repo_root,
        working_dir=working_dir,
        db_path=db_path,
        language=parser.get("runtime", "language", fallback="en").strip() or "en",
        runtime_home=runtime_home,
        agent_timeout_seconds=agent_timeout_seconds,
        ui_port=parser.getint("ui", "port", fallback=43861),
        ui_auto_start=normalize_bool(parser.get("ui", "auto_start", fallback="true")),
        ui_open_browser=normalize_bool(parser.get("ui", "open_browser", fallback="true")),
        ui_language=parser.get("ui", "language", fallback="").strip(),
        notifications=notifications,
        env=merged_env,
        roles=roles,
    )
