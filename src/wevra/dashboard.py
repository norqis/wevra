from __future__ import annotations

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from wevra.config import AppConfig, load_config
from wevra.models import CommandStage, Priority, RuntimeBackend
from wevra.service import (
    answer_question,
    append_instruction,
    get_command,
    list_commands,
    list_events,
    list_instructions,
    list_questions,
    list_reviews,
    list_tasks,
    run_engine,
    submit_command,
)


def iso_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat()


def is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def pid_file_for(settings: AppConfig) -> Path:
    return settings.db_path.parent / "dashboard.pid"


def log_file_for(settings: AppConfig) -> Path:
    return settings.db_path.parent / "dashboard.log"


def dashboard_url(settings: AppConfig) -> str:
    return f"http://{settings.ui_host}:{settings.ui_port}"


def read_static_html() -> str:
    return resources.files("wevra").joinpath("static/index.html").read_text(encoding="utf-8")


def summarize_roles(settings: AppConfig) -> list[dict[str, Any]]:
    items = []
    for name, role in settings.roles.items():
        items.append(
            {
                "name": name,
                "runtime": role.runtime.value,
                "model": role.model,
                "count": role.count,
            }
        )
    items.sort(key=lambda item: item["name"])
    return items


def build_snapshot(repo_root: Path, settings: Optional[AppConfig] = None) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    commands = [command.model_dump(mode="json") for command in list_commands(settings.db_path)]
    tasks = [task.model_dump(mode="json") for task in list_tasks(settings.db_path)]
    questions = [question.model_dump(mode="json") for question in list_questions(settings.db_path)]
    reviews = [review.model_dump(mode="json") for review in list_reviews(settings.db_path)]
    instructions = [instruction.model_dump(mode="json") for instruction in list_instructions(settings.db_path)]
    events = [event.model_dump(mode="json") for event in list_events(settings.db_path)][-80:]

    command_counts: Dict[str, int] = {}
    for command in commands:
        command_counts[command["stage"]] = command_counts.get(command["stage"], 0) + 1

    task_counts: Dict[str, int] = {}
    for task in tasks:
        task_counts[task["state"]] = task_counts.get(task["state"], 0) + 1

    question_counts: Dict[str, int] = {}
    for question in questions:
        question_counts[question["state"]] = question_counts.get(question["state"], 0) + 1

    review_counts: Dict[str, int] = {}
    for review in reviews:
        review_counts[review["decision"]] = review_counts.get(review["decision"], 0) + 1

    active_commands = [
        command for command in commands if command["stage"] not in {CommandStage.DONE.value, CommandStage.FAILED.value}
    ]

    payload = {
        "generated_at": iso_now(),
        "repo_root": str(repo_root),
        "runtime": {
            "db_path": str(settings.db_path),
            "working_dir": str(settings.working_dir),
            "language": settings.language,
            "dashboard_url": dashboard_url(settings),
            "roles": summarize_roles(settings),
        },
        "commands": {
            "counts": command_counts,
            "items": commands,
            "active": active_commands,
        },
        "tasks": {
            "counts": task_counts,
            "items": tasks,
        },
        "questions": {
            "counts": question_counts,
            "items": questions,
            "open": [question for question in questions if question["state"] == "open"],
        },
        "reviews": {
            "counts": review_counts,
            "items": reviews,
        },
        "instructions": {
            "count": len(instructions),
            "items": instructions,
        },
        "events": events,
    }
    checksum_payload = dict(payload)
    checksum_payload.pop("generated_at", None)
    payload["checksum"] = hashlib.sha256(
        json.dumps(checksum_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return payload


class DashboardHandler(BaseHTTPRequestHandler):
    server_version = "WevraDashboard/0.1"

    @property
    def repo_root(self) -> Path:
        return self.server.repo_root  # type: ignore[attr-defined]

    @property
    def settings(self) -> AppConfig:
        return self.server.settings  # type: ignore[attr-defined]

    def log_message(self, format: str, *args) -> None:
        return

    def send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, status: HTTPStatus, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_html(HTTPStatus.OK, read_static_html())
            return
        if parsed.path == "/api/snapshot":
            self.send_json(HTTPStatus.OK, build_snapshot(self.repo_root, self.settings))
            return
        self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            payload = self.read_json_body()
        except json.JSONDecodeError:
            self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid_json"})
            return

        if parsed.path == "/api/commands":
            goal = str(payload.get("goal", "")).strip()
            if not goal:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "goal_required"})
                return
            priority = Priority(str(payload.get("priority", Priority.HIGH.value)))
            backend_raw = str(payload.get("backend", "")).strip().lower()
            backend = RuntimeBackend(backend_raw) if backend_raw else None
            command = submit_command(
                self.settings.db_path,
                goal=goal,
                priority=priority,
                backend=backend,
                settings=self.settings,
                repo_root=self.repo_root,
            )
            self.send_json(HTTPStatus.CREATED, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/append":
            command_id = str(payload.get("command_id", "")).strip()
            body = str(payload.get("body", "")).strip()
            if not command_id or not body:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_and_body_required"})
                return
            instruction, command = append_instruction(self.settings.db_path, command_id=command_id, body=body)
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "instruction": instruction.model_dump(mode="json"),
                    "command": command.model_dump(mode="json"),
                },
            )
            return

        if parsed.path == "/api/commands/run":
            command_id = str(payload.get("command_id", "")).strip() or None
            max_steps = int(payload.get("max_steps", 100) or 100)
            result = run_engine(
                self.settings.db_path,
                command_id=command_id,
                max_steps=max_steps,
                settings=self.settings,
                repo_root=self.repo_root,
            )
            self.send_json(HTTPStatus.OK, {"ok": True, "result": result})
            return

        if parsed.path == "/api/questions/answer":
            question_id = str(payload.get("question_id", "")).strip()
            answer_text = str(payload.get("answer", "")).strip()
            if not question_id or not answer_text:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "question_and_answer_required"})
                return
            answered = answer_question(self.settings.db_path, question_id=question_id, answer=answer_text)
            resumed = run_engine(
                self.settings.db_path,
                command_id=answered.command_id,
                max_steps=int(payload.get("max_steps", 100) or 100),
                settings=self.settings,
                repo_root=self.repo_root,
            )
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "question": answered.model_dump(mode="json"),
                    "result": resumed,
                },
            )
            return

        self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})


def create_server(repo_root: Path, host: str, port: int) -> ThreadingHTTPServer:
    settings = load_config(repo_root)
    server = ThreadingHTTPServer((host, port), DashboardHandler)
    settings.ui_host = host
    settings.ui_port = server.server_address[1]
    server.repo_root = repo_root.resolve()  # type: ignore[attr-defined]
    server.settings = settings  # type: ignore[attr-defined]
    return server


def open_browser(url: str) -> None:
    commands = [
        ["xdg-open", url],
        ["open", url],
        ["cmd", "/c", "start", url],
    ]
    for command in commands:
        try:
            subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return
        except OSError:
            continue


def start_dashboard(repo_root: Path, settings: Optional[AppConfig] = None) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    pid_file = pid_file_for(settings)
    log_file = log_file_for(settings)
    pid_file.parent.mkdir(parents=True, exist_ok=True)

    if pid_file.exists():
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = 0
        if pid and is_pid_running(pid):
            return {"running": True, "pid": pid, "url": dashboard_url(settings)}
        pid_file.unlink(missing_ok=True)

    with log_file.open("a", encoding="utf-8") as handle:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "wevra.dashboard",
                "--repo-root",
                str(repo_root.resolve()),
                "--host",
                settings.ui_host,
                "--port",
                str(settings.ui_port),
            ],
            stdout=handle,
            stderr=handle,
            start_new_session=True,
        )
    pid_file.write_text(str(process.pid), encoding="utf-8")
    time.sleep(0.4)
    if settings.ui_open_browser:
        open_browser(dashboard_url(settings))
    return {"running": True, "pid": process.pid, "url": dashboard_url(settings)}


def stop_dashboard(repo_root: Path, settings: Optional[AppConfig] = None) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    pid_file = pid_file_for(settings)
    if not pid_file.exists():
        return {"running": False, "pid": None, "url": dashboard_url(settings)}
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except ValueError:
        pid = 0
    if pid and is_pid_running(pid):
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.2)
    pid_file.unlink(missing_ok=True)
    return {"running": False, "pid": pid or None, "url": dashboard_url(settings)}


def dashboard_status(repo_root: Path, settings: Optional[AppConfig] = None) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    pid_file = pid_file_for(settings)
    pid = None
    running = False
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text(encoding="utf-8").strip())
        except ValueError:
            pid = None
        running = bool(pid) and is_pid_running(pid)
        if not running:
            pid_file.unlink(missing_ok=True)
            pid = None
    return {"running": running, "pid": pid, "url": dashboard_url(settings)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Wevra dashboard server.")
    parser.add_argument("--repo-root", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=43861)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    server = create_server(repo_root, args.host, args.port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
