from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from wevra.config import AppConfig, LOOPBACK_HOST, load_config
from wevra.db import connect, initialize_database
from wevra.models import (
    ApprovalMode,
    CommandStage,
    JobSplitPreview,
    Priority,
    RuntimeBackend,
    WorkflowMode,
)
from wevra.runtime_registry import runtime_label, runtime_option_payload
from wevra.service import (
    answer_question,
    approve_agent_run,
    approve_agent_runs_batch,
    append_instruction,
    cancel_command_with_repair,
    cancel_command,
    deny_agent_run,
    ignore_command_dependencies,
    list_artifacts,
    list_commands,
    list_events,
    list_instructions,
    list_agent_runs,
    list_questions,
    list_reviews,
    list_tasks,
    request_command_stop,
    retry_operator_issue,
    resume_command,
    generate_job_split_preview,
    submit_command,
    submit_job_split_preview,
    tick_once,
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
    return f"http://{LOOPBACK_HOST}:{settings.ui_port}"


def build_runtime_metadata(
    settings: AppConfig, *, owner: str | None = None, engine_state: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    return {
        "db_path": str(settings.db_path),
        "working_dir": str(settings.working_dir),
        "language": settings.ui_language or settings.language,
        "dashboard_url": dashboard_url(settings),
        "roles": summarize_roles(settings),
        "runtime_options": runtime_option_payload(include_mock=True, include_inherit=True),
        "switchable_runtime_options": runtime_option_payload(
            include_mock=False, include_inherit=False
        ),
        "engine_owner": owner or "manual",
        "engine_state": engine_state or {"status": "running", "last_error": None},
        "listen": {
            "host": LOOPBACK_HOST,
            "port": settings.ui_port,
            "access_mode": "local_only",
        },
    }


def read_static_html() -> str:
    return resources.files("wevra").joinpath("static/index.html").read_text(encoding="utf-8")


def read_packaged_static_bytes(relative_path: str) -> tuple[bytes, str]:
    static_root = resources.files("wevra").joinpath("static")
    current = static_root
    for part in Path(relative_path).parts:
        if part in {"", ".", ".."}:
            raise FileNotFoundError(relative_path)
        current = current.joinpath(part)
    data = current.read_bytes()
    mime_type = mimetypes.guess_type(relative_path)[0] or "application/octet-stream"
    return data, mime_type


def read_repo_image_bytes(repo_root: Path, relative_name: str) -> tuple[bytes, str]:
    if "/" in relative_name or "\\" in relative_name or relative_name in {"", ".", ".."}:
        raise FileNotFoundError(relative_name)
    target = (repo_root / "docs" / "images" / relative_name).resolve()
    docs_root = (repo_root / "docs" / "images").resolve()
    if docs_root not in target.parents:
        raise FileNotFoundError(relative_name)
    if target.exists():
        data = target.read_bytes()
        mime_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        return data, mime_type
    return read_packaged_static_bytes(relative_name)


def summarize_roles(settings: AppConfig) -> list[dict[str, Any]]:
    items = []
    for name, role in settings.roles.items():
        items.append(
            {
                "name": name,
                "runtime": role.runtime.value,
                "runtime_label": runtime_label(role.runtime),
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
    agent_runs = [
        agent_run.model_dump(mode="json") for agent_run in list_agent_runs(settings.db_path)
    ]
    instructions = [
        instruction.model_dump(mode="json") for instruction in list_instructions(settings.db_path)
    ]
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

    agent_run_counts: Dict[str, int] = {}
    for agent_run in agent_runs:
        agent_run_counts[agent_run["state"]] = agent_run_counts.get(agent_run["state"], 0) + 1

    active_commands = [
        command
        for command in commands
        if command["stage"]
        not in {
            CommandStage.DONE.value,
            CommandStage.FAILED.value,
            CommandStage.PAUSED.value,
            CommandStage.CANCELED.value,
        }
    ]

    payload = {
        "generated_at": iso_now(),
        "repo_root": str(repo_root),
        "runtime": build_runtime_metadata(
            settings,
            engine_state={"status": "running", "last_error": None},
        ),
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
        "agent_runs": {
            "counts": agent_run_counts,
            "items": agent_runs,
            "pending": [
                agent_run for agent_run in agent_runs if agent_run["state"] == "pending_approval"
            ],
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


def build_command_detail_tokens(db_path: Path) -> Dict[str, str]:
    with connect(db_path) as conn:
        rows = conn.execute(
            """
            SELECT
                c.id AS command_id,
                MAX(
                    COALESCE(c.updated_at, ''),
                    COALESCE((SELECT MAX(updated_at) FROM tasks WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(updated_at) FROM questions WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(updated_at) FROM reviews WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(updated_at) FROM agent_runs WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(created_at) FROM instructions WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(created_at) FROM artifacts WHERE command_id = c.id), ''),
                    COALESCE((SELECT MAX(created_at) FROM events WHERE stream_type = 'command' AND stream_id = c.id), '')
                ) AS detail_token
            FROM commands c
            """
        ).fetchall()
    return {row["command_id"]: row["detail_token"] for row in rows}


def build_summary_snapshot(
    repo_root: Path,
    settings: Optional[AppConfig] = None,
    *,
    engine_state: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    detail_tokens = build_command_detail_tokens(settings.db_path)
    commands = [command.model_dump(mode="json") for command in list_commands(settings.db_path)]
    open_questions = [
        question.model_dump(mode="json")
        for question in list_questions(settings.db_path, open_only=True)
    ]
    pending_agent_runs = [
        agent_run.model_dump(mode="json")
        for agent_run in list_agent_runs(settings.db_path)
        if agent_run.state.value == "pending_approval"
    ]

    command_counts: Dict[str, int] = {}
    for command in commands:
        command["detail_token"] = detail_tokens.get(command["id"], command["updated_at"])
        command_counts[command["stage"]] = command_counts.get(command["stage"], 0) + 1

    active_commands = [
        command
        for command in commands
        if command["stage"]
        not in {
            CommandStage.DONE.value,
            CommandStage.FAILED.value,
            CommandStage.PAUSED.value,
            CommandStage.CANCELED.value,
        }
    ]

    payload = {
        "generated_at": iso_now(),
        "repo_root": str(repo_root),
        "runtime": build_runtime_metadata(
            settings,
            owner="dashboard_background_loop",
            engine_state=engine_state,
        ),
        "commands": {
            "counts": command_counts,
            "items": commands,
            "active": active_commands,
        },
        "questions": {
            "open": open_questions,
        },
        "agent_runs": {
            "pending": pending_agent_runs,
        },
    }
    checksum_payload = dict(payload)
    checksum_payload.pop("generated_at", None)
    payload["checksum"] = hashlib.sha256(
        json.dumps(checksum_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()
    return payload


def build_command_detail(
    repo_root: Path, command_id: str, settings: Optional[AppConfig] = None
) -> dict[str, Any]:
    settings = settings or load_config(repo_root)
    command = next(
        (item for item in list_commands(settings.db_path) if item.id == command_id),
        None,
    )
    tasks = [task.model_dump(mode="json") for task in list_tasks(settings.db_path, command_id)]
    questions = [
        question.model_dump(mode="json")
        for question in list_questions(settings.db_path, command_id)
    ]
    reviews = [
        review.model_dump(mode="json") for review in list_reviews(settings.db_path, command_id)
    ]
    agent_runs = [
        agent_run.model_dump(mode="json")
        for agent_run in list_agent_runs(settings.db_path, command_id)
    ]
    artifacts = [
        artifact.model_dump(mode="json")
        for artifact in list_artifacts(settings.db_path, command_id)
    ]
    instructions = [
        instruction.model_dump(mode="json")
        for instruction in list_instructions(settings.db_path, command_id)
    ]
    events = [event.model_dump(mode="json") for event in list_events(settings.db_path, command_id)][
        -80:
    ]

    payload = {
        "generated_at": iso_now(),
        "command_id": command_id,
        "command": command.model_dump(mode="json") if command else None,
        "tasks": {"items": tasks},
        "questions": {"items": questions},
        "reviews": {"items": reviews},
        "agent_runs": {"items": agent_runs},
        "artifacts": {"items": artifacts},
        "instructions": {"items": instructions},
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

    @property
    def state_lock(self):
        return self.server.state_lock  # type: ignore[attr-defined]

    @property
    def engine_state_lock(self):
        return self.server.engine_state_lock  # type: ignore[attr-defined]

    def engine_state_snapshot(self) -> dict[str, Any]:
        with self.engine_state_lock:
            return dict(getattr(self.server, "engine_state", {}) or {})

    def log_message(self, format: str, *args) -> None:
        return

    def send_json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
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

    def send_bytes(self, status: HTTPStatus, body: bytes, mime_type: str) -> None:
        self.send_response(status.value)
        self.send_header("Content-Type", mime_type)
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
        if parsed.path.startswith("/static/"):
            try:
                body, mime_type = read_packaged_static_bytes(parsed.path.removeprefix("/static/"))
            except FileNotFoundError:
                self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
                return
            self.send_bytes(HTTPStatus.OK, body, mime_type)
            return
        if parsed.path.startswith("/docs-images/"):
            try:
                body, mime_type = read_repo_image_bytes(
                    self.repo_root, parsed.path.removeprefix("/docs-images/")
                )
            except FileNotFoundError:
                self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})
                return
            self.send_bytes(HTTPStatus.OK, body, mime_type)
            return
        if parsed.path == "/api/snapshot":
            self.send_json(
                HTTPStatus.OK,
                build_summary_snapshot(
                    self.repo_root,
                    self.settings,
                    engine_state=self.engine_state_snapshot(),
                ),
            )
            return
        if (
            parsed.path.startswith("/api/commands/")
            and parsed.path.endswith("/detail")
            and len(parsed.path.strip("/").split("/")) == 4
        ):
            command_id = parsed.path.strip("/").split("/")[2]
            self.send_json(
                HTTPStatus.OK,
                build_command_detail(self.repo_root, command_id, self.settings),
            )
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
            workflow_mode = WorkflowMode(
                str(payload.get("workflow_mode", WorkflowMode.AUTO.value)).strip()
                or WorkflowMode.AUTO.value
            )
            priority = Priority(str(payload.get("priority", Priority.HIGH.value)))
            approval_mode = ApprovalMode(
                str(payload.get("approval_mode", ApprovalMode.AUTO.value)).strip()
                or ApprovalMode.AUTO.value
            )
            backend_raw = str(payload.get("backend", "")).strip().lower()
            backend = RuntimeBackend(backend_raw) if backend_raw else None
            depends_on_command_ids = [
                str(item).strip()
                for item in payload.get("depends_on_command_ids", [])
                if str(item).strip()
            ]
            allow_parallel = bool(payload.get("allow_parallel", False))
            workspace_root_raw = str(payload.get("workspace_root", "")).strip()
            if not workspace_root_raw:
                self.send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "workspace_root_required"},
                )
                return
            runbook_path_raw = str(payload.get("runbook_path", "")).strip()
            workspace_root = Path(workspace_root_raw).expanduser()
            try:
                with self.state_lock:
                    command = submit_command(
                        self.settings.db_path,
                        goal=goal,
                        workflow_mode=workflow_mode,
                        approval_mode=approval_mode,
                        priority=priority,
                        backend=backend,
                        workspace_root=workspace_root,
                        runbook_path=runbook_path_raw or None,
                        depends_on_command_ids=depends_on_command_ids,
                        allow_parallel=allow_parallel,
                        settings=self.settings,
                        repo_root=self.repo_root,
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(
                HTTPStatus.CREATED, {"ok": True, "command": command.model_dump(mode="json")}
            )
            return

        if parsed.path == "/api/commands/split-preview":
            goal = str(payload.get("goal", "")).strip()
            if not goal:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "goal_required"})
                return
            workspace_root_raw = str(payload.get("workspace_root", "")).strip()
            if not workspace_root_raw:
                self.send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "workspace_root_required"},
                )
                return
            backend_raw = str(payload.get("backend", "")).strip().lower()
            backend = RuntimeBackend(backend_raw) if backend_raw else None
            runbook_path_raw = str(payload.get("runbook_path", "")).strip()
            locale = str(payload.get("locale", "")).strip() or None
            workspace_root = Path(workspace_root_raw).expanduser()
            try:
                preview = generate_job_split_preview(
                    goal=goal,
                    workspace_root=workspace_root,
                    runbook_path=runbook_path_raw or None,
                    backend=backend,
                    settings=self.settings,
                    repo_root=self.repo_root,
                    locale=locale,
                )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"ok": True, "preview": preview.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/split-apply":
            preview_payload = payload.get("preview")
            if not isinstance(preview_payload, dict):
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "preview_required"})
                return
            priority = Priority(str(payload.get("priority", Priority.HIGH.value)))
            approval_mode = ApprovalMode(
                str(payload.get("approval_mode", ApprovalMode.AUTO.value)).strip()
                or ApprovalMode.AUTO.value
            )
            backend_raw = str(payload.get("backend", "")).strip().lower()
            backend = RuntimeBackend(backend_raw) if backend_raw else None
            try:
                preview = JobSplitPreview.model_validate(preview_payload)
                with self.state_lock:
                    commands = submit_job_split_preview(
                        self.settings.db_path,
                        preview=preview,
                        approval_mode=approval_mode,
                        priority=priority,
                        backend=backend,
                        settings=self.settings,
                        repo_root=self.repo_root,
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(
                HTTPStatus.CREATED,
                {"ok": True, "commands": [command.model_dump(mode="json") for command in commands]},
            )
            return

        if parsed.path == "/api/commands/append":
            command_id = str(payload.get("command_id", "")).strip()
            body = str(payload.get("body", "")).strip()
            if not command_id or not body:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_and_body_required"}
                )
                return
            with self.state_lock:
                instruction, command = append_instruction(
                    self.settings.db_path, command_id=command_id, body=body
                )
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "instruction": instruction.model_dump(mode="json"),
                    "command": command.model_dump(mode="json"),
                },
            )
            return

        if parsed.path == "/api/commands/stop":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            with self.state_lock:
                command = request_command_stop(self.settings.db_path, command_id=command_id)
            self.send_json(HTTPStatus.OK, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/cancel":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            reason = str(payload.get("reason", "")).strip() or None
            try:
                with self.state_lock:
                    command = cancel_command(
                        self.settings.db_path, command_id=command_id, reason=reason
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/ignore-dependencies":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            try:
                with self.state_lock:
                    command = ignore_command_dependencies(
                        self.settings.db_path, command_id=command_id
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/resume":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            with self.state_lock:
                command = resume_command(self.settings.db_path, command_id=command_id)
            self.send_json(HTTPStatus.OK, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/retry-operator-issue":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            backend_raw = str(payload.get("backend", "")).strip().lower()
            backend = RuntimeBackend(backend_raw) if backend_raw else None
            try:
                with self.state_lock:
                    command = retry_operator_issue(
                        self.settings.db_path,
                        command_id=command_id,
                        backend_override=backend,
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(HTTPStatus.OK, {"ok": True, "command": command.model_dump(mode="json")})
            return

        if parsed.path == "/api/commands/cancel-with-repair":
            command_id = str(payload.get("command_id", "")).strip()
            repair_goal = str(payload.get("repair_goal", "")).strip()
            if not command_id or not repair_goal:
                self.send_json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "command_id_and_repair_goal_required"},
                )
                return
            try:
                with self.state_lock:
                    canceled, repair = cancel_command_with_repair(
                        self.settings.db_path,
                        command_id=command_id,
                        repair_goal=repair_goal,
                        settings=self.settings,
                        repo_root=self.repo_root,
                    )
            except ValueError as exc:
                self.send_json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
                return
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "canceled_command": canceled.model_dump(mode="json"),
                    "repair_command": repair.model_dump(mode="json"),
                },
            )
            return

        if parsed.path == "/api/questions/answer":
            question_id = str(payload.get("question_id", "")).strip()
            answer_text = str(payload.get("answer", "")).strip()
            if not question_id or not answer_text:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "question_and_answer_required"}
                )
                return
            with self.state_lock:
                answered = answer_question(
                    self.settings.db_path, question_id=question_id, answer=answer_text
                )
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "question": answered.model_dump(mode="json"),
                },
            )
            return

        if parsed.path == "/api/agent-runs/approve":
            agent_run_id = str(payload.get("agent_run_id", "")).strip()
            if not agent_run_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "agent_run_id_required"}
                )
                return
            with self.state_lock:
                approved = approve_agent_run(self.settings.db_path, agent_run_id=agent_run_id)
            self.send_json(
                HTTPStatus.OK,
                {"ok": True, "agent_run": approved.model_dump(mode="json")},
            )
            return

        if parsed.path == "/api/agent-runs/approve-batch":
            command_id = str(payload.get("command_id", "")).strip()
            if not command_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "command_id_required"}
                )
                return
            role_name = str(payload.get("role_name", "")).strip() or None
            with self.state_lock:
                approved = approve_agent_runs_batch(
                    self.settings.db_path,
                    command_id=command_id,
                    role_name=role_name,
                )
            self.send_json(
                HTTPStatus.OK,
                {
                    "ok": True,
                    "agent_runs": [agent_run.model_dump(mode="json") for agent_run in approved],
                },
            )
            return

        if parsed.path == "/api/agent-runs/deny":
            agent_run_id = str(payload.get("agent_run_id", "")).strip()
            if not agent_run_id:
                self.send_json(
                    HTTPStatus.BAD_REQUEST, {"ok": False, "error": "agent_run_id_required"}
                )
                return
            reason = str(payload.get("reason", "")).strip() or None
            with self.state_lock:
                denied = deny_agent_run(
                    self.settings.db_path, agent_run_id=agent_run_id, reason=reason
                )
            self.send_json(
                HTTPStatus.OK,
                {"ok": True, "agent_run": denied.model_dump(mode="json")},
            )
            return

        self.send_json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not_found"})


class WevraDashboardServer(ThreadingHTTPServer):
    daemon_threads = True

    def shutdown(self) -> None:
        self.stop_event.set()  # type: ignore[attr-defined]
        super().shutdown()
        engine_thread = getattr(self, "engine_thread", None)
        if engine_thread is not None and engine_thread.is_alive():
            engine_thread.join(timeout=2)


def engine_poll_delay(action: str) -> float:
    if action in {"no_op", "blocked", "approval_required"}:
        return 0.6
    return 0.05


def engine_loop(server: WevraDashboardServer) -> None:
    while not server.stop_event.is_set():
        try:
            with server.state_lock:
                outcome = tick_once(
                    server.settings.db_path,
                    settings=server.settings,
                    repo_root=server.repo_root,
                )
            with server.engine_state_lock:  # type: ignore[attr-defined]
                server.engine_state = {  # type: ignore[attr-defined]
                    "status": "running",
                    "last_error": None,
                    "updated_at": iso_now(),
                    "last_action": outcome.action,
                }
        except Exception as exc:
            with server.engine_state_lock:  # type: ignore[attr-defined]
                server.engine_state = {  # type: ignore[attr-defined]
                    "status": "error",
                    "last_error": str(exc),
                    "updated_at": iso_now(),
                }
            server.stop_event.wait(1.0)
            continue
        server.stop_event.wait(engine_poll_delay(outcome.action))


def create_server(repo_root: Path, port: int) -> ThreadingHTTPServer:
    settings = load_config(repo_root)
    server = WevraDashboardServer((LOOPBACK_HOST, port), DashboardHandler)
    settings.ui_port = server.server_address[1]
    initialize_database(settings.db_path)
    server.repo_root = repo_root.resolve()  # type: ignore[attr-defined]
    server.settings = settings  # type: ignore[attr-defined]
    server.state_lock = threading.RLock()  # type: ignore[attr-defined]
    server.engine_state_lock = threading.Lock()  # type: ignore[attr-defined]
    server.stop_event = threading.Event()  # type: ignore[attr-defined]
    server.engine_state = {  # type: ignore[attr-defined]
        "status": "running",
        "last_error": None,
        "updated_at": iso_now(),
    }
    server.engine_thread = threading.Thread(target=engine_loop, args=(server,), daemon=True)  # type: ignore[attr-defined]
    server.engine_thread.start()  # type: ignore[attr-defined]
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
    parser.add_argument("--port", type=int, default=43861)
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    server = create_server(repo_root, args.port)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
