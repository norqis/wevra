import json
import signal
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib import error, request

import pytest
from typer.testing import CliRunner

import wevra.cli as cli_module
import wevra.dashboard as dashboard_module
import wevra.service as service_module
from wevra import __version__
from wevra.cli import app
from wevra.config import (
    RuntimeBackend,
    init_repo_config,
    load_config,
    normalize_bool,
    read_simple_env,
    resolve_optional_config_path,
)
from wevra.dashboard import (
    build_command_detail,
    build_snapshot,
    build_summary_snapshot,
    create_server,
)
from wevra.db import connect, initialize_database
from wevra.models import (
    ApprovalMode,
    CommandRecord,
    CommandStage,
    PlannerDecision,
    PlannerOutput,
    PlannerTaskSpec,
    Priority,
    QuestionResolutionMode,
    QuestionState,
    TaskRecord,
    TaskState,
    WorkflowMode,
)
from wevra.service import (
    StructuredCliBackend,
    approve_agent_run,
    approve_agent_runs_batch,
    BackendInterface,
    add_mode_control_tasks,
    answer_question,
    append_instruction,
    backend_for,
    deny_agent_run,
    build_context_payload,
    build_final_response,
    build_final_test_spec,
    create_question_record,
    create_task_records,
    effective_mode,
    infer_mode_from_goal,
    infer_mode_from_specs,
    ignore_command_dependencies,
    list_agent_runs,
    list_artifacts,
    list_instructions,
    mode_prompt_guidance,
    mode_requires_review,
    mode_requires_test,
    requested_mode,
    reduce_waiting_question,
    resolve_command_mode,
    resolve_settings,
    select_actionable_command,
    select_ready_batch,
    submit_command,
    tick_once,
    cancel_command,
    workspace_roots_overlap,
)


runner = CliRunner()


def read_json(result):
    assert result.exit_code == 0, result.stdout
    return json.loads(result.stdout)


def submit_job(tmp_path, *args):
    return read_json(runner.invoke(app, ["submit", "--workspace-dir", str(tmp_path), *args]))


def submit_direct(tmp_path, settings, *, goal, workflow_mode, priority, **kwargs):
    return submit_command(
        settings.db_path,
        goal=goal,
        workflow_mode=workflow_mode,
        priority=priority,
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
        **kwargs,
    )


def request_json(url: str, *, method: str = "GET", body: Optional[bytes] = None, headers=None):
    req = request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_config_helpers_parse_env_and_resolve_paths(tmp_path):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n# comment\nDISCORD_WEBHOOK_URL=\"https://discord.example\"\nEMPTY=\nQUOTED='value'\nNOVALUE\n",
        encoding="utf-8",
    )

    assert normalize_bool("Yes") is True
    assert normalize_bool("0") is False
    assert read_simple_env(tmp_path / "missing.env") == {}
    assert read_simple_env(env_path) == {
        "DISCORD_WEBHOOK_URL": "https://discord.example",
        "EMPTY": "",
        "QUOTED": "value",
    }

    assert resolve_optional_config_path("", tmp_path) is None
    assert (
        resolve_optional_config_path("relative/home", tmp_path)
        == (tmp_path / "relative/home").resolve()
    )
    assert (
        resolve_optional_config_path(str(tmp_path / "absolute/home"), tmp_path)
        == (tmp_path / "absolute/home").resolve()
    )


def test_init_repo_config_is_idempotent_and_load_config_uses_repo_root_as_workdir(
    tmp_path, monkeypatch
):
    created = init_repo_config(tmp_path)
    assert set(created) == {"wevra.ini", "agents.ini", ".env"}
    assert init_repo_config(tmp_path) == {}

    (tmp_path / "wevra.ini").write_text(
        """[runtime]
db_path = runtime/app.db
language = ja
agent_timeout_seconds = 321
home = runtime-home

[ui]
auto_start = false
port = 45000
open_browser = false
language = ja

[notification]
question_opened = yes
workflow_completed = on

[discord]
enable = true
webhook_url = DISCORD_WEBHOOK_URL
""",
        encoding="utf-8",
    )
    (tmp_path / "agents.ini").write_text(
        """[planner]
runtime = codex
model = gpt-test

[reviewer]
runtime = claude
model = opus-test
count = 0
""",
        encoding="utf-8",
    )
    (tmp_path / ".env").write_text(
        "DISCORD_WEBHOOK_URL=https://discord.example\n", encoding="utf-8"
    )
    monkeypatch.setenv("ADDITIONAL_SETTING", "from-os")

    settings = load_config(tmp_path)

    assert settings.language == "ja"
    assert settings.runtime_home == (tmp_path / "runtime-home").resolve()
    assert settings.agent_timeout_seconds == 321
    assert settings.working_dir == tmp_path.resolve()
    assert settings.working_dir.is_dir()
    assert settings.db_path == (tmp_path / "runtime/app.db").resolve()
    assert settings.ui_auto_start is False
    assert settings.ui_open_browser is False
    assert settings.ui_port == 45000
    assert settings.ui_language == "ja"
    assert settings.notifications == {"question_opened": True, "workflow_completed": True}
    assert settings.roles["planner"].runtime == RuntimeBackend.CODEX
    assert settings.roles["planner"].model == "gpt-test"
    assert settings.roles["reviewer"].count == 1
    assert settings.env["DISCORD_WEBHOOK_URL"] == "https://discord.example"
    assert settings.env["ADDITIONAL_SETTING"] == "from-os"

    fallback_role = settings.role_for("unknown_capability")
    assert fallback_role.name == "unknown_capability"
    assert fallback_role.count == 4


def test_cli_public_commands_cover_version_status_and_listing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    version = runner.invoke(app, ["version"])
    assert version.exit_code == 0
    assert version.stdout.strip() == __version__

    submitted = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--mode",
                "research",
                "--workspace-dir",
                str(tmp_path / "workspace-a"),
                "investigate the current architecture",
            ],
        )
    )
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    listed = read_json(runner.invoke(app, ["list"]))
    assert listed["commands"][0]["id"] == command_id

    shown = read_json(runner.invoke(app, ["show", command_id]))
    assert shown["workspace_root"] == str((tmp_path / "workspace-a").resolve())

    missing = runner.invoke(app, ["show", "cmd_missing"])
    assert missing.exit_code == 1

    questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
    assert questions["questions"] == []

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert reviews["reviews"] == []

    events = read_json(runner.invoke(app, ["events", "--command-id", command_id]))
    assert any(event["event_type"] == "command_completed" for event in events["events"])


def test_cli_start_stop_status_and_dashboard_subcommands_use_helpers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    init_repo_config(tmp_path)
    config = load_config(tmp_path)
    config.ui_auto_start = True

    started = {"count": 0}
    stopped = {"count": 0}

    monkeypatch.setattr(cli_module, "settings", lambda: config)
    monkeypatch.setattr(
        cli_module,
        "start_dashboard",
        lambda *_: (
            started.update(count=started["count"] + 1)
            or {"running": True, "pid": 111, "url": "http://127.0.0.1:43861"}
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "stop_dashboard",
        lambda *_: (
            stopped.update(count=stopped["count"] + 1)
            or {"running": False, "pid": 111, "url": "http://127.0.0.1:43861"}
        ),
    )
    monkeypatch.setattr(
        cli_module,
        "dashboard_status",
        lambda *_: {"running": False, "pid": None, "url": "http://127.0.0.1:43861"},
    )
    monkeypatch.setattr(
        cli_module,
        "build_snapshot",
        lambda *_: {
            "commands": {"items": [1, 2]},
            "questions": {"open": [1]},
            "tasks": {"items": [1, 2, 3]},
        },
    )

    start_payload = read_json(runner.invoke(app, ["start"]))
    assert start_payload["dashboard"]["running"] is True
    assert started["count"] == 1

    status_payload = read_json(runner.invoke(app, ["status"]))
    assert status_payload["counts"] == {"commands": 2, "open_questions": 1, "tasks": 3}

    dash_start = read_json(runner.invoke(app, ["dashboard", "start"]))
    assert dash_start["running"] is True
    dash_stop = read_json(runner.invoke(app, ["dashboard", "stop"]))
    assert dash_stop["running"] is False
    dash_status = read_json(runner.invoke(app, ["dashboard", "status"]))
    assert dash_status["running"] is False

    stop_payload = read_json(runner.invoke(app, ["stop"]))
    assert stop_payload["dashboard"]["running"] is False
    assert stopped["count"] == 2


def test_dashboard_http_handles_errors_and_serves_snapshot(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    docs_images = tmp_path / "docs" / "images"
    docs_images.mkdir(parents=True, exist_ok=True)
    docs_images.joinpath("probe.png").write_bytes(
        b"\x89PNG\r\n\x1a\n"
        b"\x00\x00\x00\rIHDR"
        b"\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
        b"\x00\x00\x00\x0cIDATx\x9cc`\x00\x00\x00\x02\x00\x01\xe2!\xbc3"
        b"\x00\x00\x00\x00IEND\xaeB`\x82"
    )

    server = create_server(tmp_path, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base_url = f"http://{host}:{port}"

        with request.urlopen(f"{base_url}/") as response:
            html = response.read().decode("utf-8")
        assert "<title>Wevra</title>" in html

        with request.urlopen(f"{base_url}/static/i18n/ja.json") as response:
            locale_payload = json.loads(response.read().decode("utf-8"))
        assert locale_payload["text"]["openSubmitBtn"] == "ジョブを投入"

        with request.urlopen(f"{base_url}/docs-images/probe.png") as response:
            image_bytes = response.read()
            image_mime = response.headers.get_content_type()
        assert image_mime == "image/png"
        assert image_bytes.startswith(b"\x89PNG")

        status, payload = request_json(f"{base_url}/api/snapshot")
        assert status == 200
        assert payload["commands"]["items"] == []

        status, payload = request_json(f"{base_url}/missing")
        assert status == 404
        assert payload["error"] == "not_found"

        status, payload = request_json(
            f"{base_url}/api/commands",
            method="POST",
            body=b"{broken",
            headers={"Content-Type": "application/json"},
        )
        assert status == 400
        assert payload["error"] == "invalid_json"

        cases = [
            ("/api/commands", {}, "goal_required"),
            ("/api/commands", {"goal": "x"}, "workspace_root_required"),
            ("/api/commands/append", {"command_id": "x"}, "command_and_body_required"),
            ("/api/questions/answer", {"question_id": "x"}, "question_and_answer_required"),
            ("/api/agent-runs/approve", {}, "agent_run_id_required"),
            ("/api/agent-runs/approve-batch", {}, "command_id_required"),
            ("/api/agent-runs/deny", {}, "agent_run_id_required"),
            ("/api/unknown", {}, "not_found"),
        ]
        for path, body, expected_error in cases:
            status, payload = request_json(
                f"{base_url}{path}",
                method="POST",
                body=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
            assert payload["error"] == expected_error
            assert status in {400, 404}
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_dashboard_process_helpers_cover_browser_open_and_pid_lifecycle(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.ui_open_browser = True

    open_calls = []
    sleep_calls = []
    popen_calls = []
    kill_calls = []

    class DummyProcess:
        pid = 4321

    def fake_popen(command, stdout=None, stderr=None, start_new_session=None):
        popen_calls.append(command)
        if command[0] == "xdg-open":
            raise OSError("not available")
        return DummyProcess()

    monkeypatch.setattr(dashboard_module.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(dashboard_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr(dashboard_module, "is_pid_running", lambda pid: pid == 4321)
    monkeypatch.setattr(dashboard_module.os, "kill", lambda pid, sig: kill_calls.append((pid, sig)))

    dashboard_module.open_browser("http://example.test")
    assert popen_calls[0][0] == "xdg-open"
    assert popen_calls[1][0] == "open"

    monkeypatch.setattr(dashboard_module, "open_browser", lambda url: open_calls.append(url))

    started = dashboard_module.start_dashboard(tmp_path, settings)
    assert started["running"] is True
    assert started["pid"] == 4321
    assert open_calls == ["http://127.0.0.1:43861"]
    assert dashboard_module.pid_file_for(settings).read_text(encoding="utf-8").strip() == "4321"

    status = dashboard_module.dashboard_status(tmp_path, settings)
    assert status["running"] is True

    stopped = dashboard_module.stop_dashboard(tmp_path, settings)
    assert stopped["running"] is False
    assert kill_calls == [(4321, signal.SIGTERM)]

    pid_file = dashboard_module.pid_file_for(settings)
    pid_file.write_text("not-a-pid", encoding="utf-8")
    stale_status = dashboard_module.dashboard_status(tmp_path, settings)
    assert stale_status["running"] is False
    assert not pid_file.exists()

    pid_file.write_text("9999", encoding="utf-8")
    monkeypatch.setattr(dashboard_module, "is_pid_running", lambda pid: False)
    stopped_again = dashboard_module.stop_dashboard(tmp_path, settings)
    assert stopped_again["pid"] == 9999
    assert not pid_file.exists()

    monkeypatch.setattr(
        dashboard_module.os, "kill", lambda *_: (_ for _ in ()).throw(OSError("dead"))
    )
    assert dashboard_module.is_pid_running(9999) is False


def test_dashboard_process_helpers_cover_no_pid_and_main_entrypoint(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    assert dashboard_module.stop_dashboard(tmp_path, settings)["running"] is False

    stale_pid = dashboard_module.pid_file_for(settings)
    stale_pid.parent.mkdir(parents=True, exist_ok=True)
    stale_pid.write_text("not-a-pid", encoding="utf-8")
    monkeypatch.setattr(dashboard_module.time, "sleep", lambda *_: None)
    monkeypatch.setattr(dashboard_module, "open_browser", lambda *_: None)
    monkeypatch.setattr(
        dashboard_module.subprocess, "Popen", lambda *args, **kwargs: SimpleNamespace(pid=9876)
    )
    assert dashboard_module.start_dashboard(tmp_path, settings)["running"] is True

    closed = []

    class FakeServer:
        def serve_forever(self):
            closed.append("served")

        def server_close(self):
            closed.append("closed")

    monkeypatch.setattr(
        dashboard_module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(repo_root=str(tmp_path), port=43861),
    )
    monkeypatch.setattr(dashboard_module, "create_server", lambda *args, **kwargs: FakeServer())
    dashboard_module.main()
    assert closed == ["served", "closed"]


def test_dashboard_url_stays_on_loopback(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    assert dashboard_module.dashboard_url(settings) == f"http://127.0.0.1:{settings.ui_port}"


def test_build_snapshot_includes_counts_roles_and_active_commands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    submitted = submit_job(tmp_path, "[worker_question] snapshot coverage")
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    snapshot = build_snapshot(tmp_path)
    assert snapshot["runtime"]["working_dir"] == str(tmp_path.resolve())
    assert snapshot["runtime"]["roles"]
    assert snapshot["commands"]["counts"]["waiting_question"] == 1
    assert snapshot["questions"]["counts"]["open"] == 1
    assert snapshot["commands"]["active"][0]["id"] == command_id
    assert any(run["command_id"] == command_id for run in snapshot["agent_runs"]["items"])
    assert any("output_log" in run for run in snapshot["agent_runs"]["items"])
    assert snapshot["checksum"]


def test_dashboard_snapshot_and_agent_approval_endpoints(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    agents_path = tmp_path / "agents.ini"
    agents_path.write_text(
        agents_path.read_text(encoding="utf-8").replace(
            "[planner]\nruntime = mock\n", "[planner]\nruntime = codex\n"
        ),
        encoding="utf-8",
    )

    class CompletingPlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.PLANNING,
                final_response="done after approval",
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: CompletingPlanner())

    server = create_server(tmp_path, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base_url = f"http://{host}:{port}"

        created = request_json(
            f"{base_url}/api/commands",
            method="POST",
            body=json.dumps(
                {
                    "goal": "needs approval",
                    "workflow_mode": "planning",
                    "approval_mode": "manual",
                    "workspace_root": str(tmp_path),
                }
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )[1]["command"]

        detail_status, detail_payload = request_json(
            f"{base_url}/api/commands/{created['id']}/detail"
        )
        assert detail_status == 200
        assert detail_payload["command"]["id"] == created["id"]

        deadline = time.time() + 5
        pending_run = None
        while time.time() < deadline:
            snapshot = request_json(f"{base_url}/api/snapshot")[1]
            assert snapshot["runtime"]["engine_state"]["status"] == "running"
            pending = [
                run
                for run in snapshot["agent_runs"]["pending"]
                if run["command_id"] == created["id"]
            ]
            if pending:
                pending_run = pending[0]
                break
            time.sleep(0.1)
        assert pending_run is not None

        batch_status, batch_payload = request_json(
            f"{base_url}/api/agent-runs/approve-batch",
            method="POST",
            body=json.dumps({"command_id": created["id"], "role_name": "planner"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        assert batch_status == 200
        assert len(batch_payload["agent_runs"]) == 1

        deadline = time.time() + 5
        final_snapshot = None
        while time.time() < deadline:
            final_snapshot = request_json(f"{base_url}/api/snapshot")[1]
            command = next(
                item for item in final_snapshot["commands"]["items"] if item["id"] == created["id"]
            )
            if command["stage"] == "done":
                break
            time.sleep(0.1)
        assert final_snapshot is not None
        assert command["stage"] == "done"

        status, payload = request_json(
            f"{base_url}/api/agent-runs/deny",
            method="POST",
            body=json.dumps({}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        assert status == 400
        assert payload["error"] == "agent_run_id_required"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_build_summary_and_command_detail_split_selected_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    submitted = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--workspace-dir",
                str(tmp_path),
                "[worker_question] split payload coverage",
            ],
        )
    )
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    summary = build_summary_snapshot(tmp_path)
    detail = build_command_detail(tmp_path, command_id)

    assert summary["commands"]["items"][0]["id"] == command_id
    assert summary["commands"]["items"][0]["detail_token"]
    assert summary["questions"]["open"][0]["command_id"] == command_id
    assert "tasks" not in summary
    assert detail["command"]["id"] == command_id
    assert detail["tasks"]["items"]
    assert detail["questions"]["items"][0]["command_id"] == command_id
    assert "output_log" in detail["agent_runs"]["items"][0]


def test_build_command_detail_includes_result_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    submitted = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--mode",
                "planning",
                "--workspace-dir",
                str(tmp_path),
                "detail artifacts planning",
            ],
        )
    )
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    detail = build_command_detail(tmp_path, command_id)

    assert [item["metadata"]["section_key"] for item in detail["artifacts"]["items"]] == [
        "plan",
        "design_direction",
        "task_breakdown",
    ]
    assert detail["artifacts"]["items"][0]["body"]


def test_engine_loop_reports_background_errors_in_summary_snapshot(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    def explode(*args, **kwargs):
        raise RuntimeError("engine loop exploded")

    monkeypatch.setattr(dashboard_module, "tick_once", explode)
    server = create_server(tmp_path, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        payload = None
        deadline = time.time() + 3
        while time.time() < deadline:
            payload = request_json(f"http://{host}:{port}/api/snapshot")[1]
            if payload["runtime"]["engine_state"]["status"] == "error":
                break
            time.sleep(0.1)
        assert payload is not None
        assert payload["runtime"]["engine_state"]["status"] == "error"
        assert "engine loop exploded" in payload["runtime"]["engine_state"]["last_error"]
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_structured_cli_backends_build_commands_and_handle_failures(tmp_path, monkeypatch):
    runtime_home = tmp_path / "runtime-home"

    codex_runs = []

    def fake_stream(self, command, workspace_root, env, **kwargs):
        codex_runs.append((command, workspace_root, env, kwargs))
        if command[0] == "codex":
            output_path = Path(command[command.index("--output-last-message") + 1])
            output_path.write_text(json.dumps({"decision": "complete"}), encoding="utf-8")
            return 0, "", ""
        return (
            0,
            json.dumps({"decision": "approve", "summary": "ok", "findings": []}),
            "",
        )

    monkeypatch.setattr(StructuredCliBackend, "_run_streaming_process", fake_stream)
    codex_backend = StructuredCliBackend(
        RuntimeBackend.CODEX,
        model="gpt-test",
        approval_mode=ApprovalMode.MANUAL,
        timeout_seconds=30,
        runtime_home=runtime_home,
    )
    codex_payload = codex_backend._run_codex("hello", {"type": "object"}, tmp_path)
    codex_command, codex_cwd, codex_env, codex_kwargs = codex_runs[0]
    assert codex_payload == {"decision": "complete"}
    assert codex_command[:2] == ["codex", "exec"]
    assert "--model" in codex_command
    assert "--full-auto" in codex_command
    assert codex_cwd == tmp_path
    assert codex_env["HOME"] == str(runtime_home)
    assert codex_kwargs["agent_run_id"] is None

    claude_backend = StructuredCliBackend(
        RuntimeBackend.CLAUDE,
        model="opus-test",
        approval_mode=ApprovalMode.AUTO,
        timeout_seconds=30,
        runtime_home=runtime_home,
    )
    claude_payload = claude_backend._run_claude("review", {"type": "object"}, tmp_path)
    assert claude_payload["decision"] == "approve"
    claude_command = codex_runs[1][0]
    assert "--dangerously-skip-permissions" in claude_command

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (1, "", "boom"),
    )
    with pytest.raises(RuntimeError, match="boom"):
        codex_backend._run_codex("broken", {"type": "object"}, tmp_path)
    with pytest.raises(RuntimeError, match="boom"):
        claude_backend._run_claude("broken", {"type": "object"}, tmp_path)
    with pytest.raises(RuntimeError, match="Unsupported backend"):
        StructuredCliBackend(
            RuntimeBackend.INHERIT,
            model="",
            approval_mode=ApprovalMode.MANUAL,
            timeout_seconds=30,
        )._run_structured("prompt", {}, str(tmp_path))


def test_manual_agent_runs_capture_logs_and_expose_them_in_detail_payload(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["implementer"].runtime = RuntimeBackend.CODEX
    initialize_database(settings.db_path)
    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: service_module.MockBackend()
    )

    command = submit_direct(
        tmp_path,
        settings,
        goal="manual approval log coverage",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        approval_mode=ApprovalMode.MANUAL,
        priority=Priority.HIGH,
    )

    tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
    tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
    approval = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert approval.action == "approval_required"

    pending_runs = [
        run
        for run in list_agent_runs(settings.db_path, command_id=command.id)
        if run.state.value == "pending_approval"
    ]
    assert pending_runs
    assert "waiting for operator approval" in pending_runs[0].output_log
    assert "prompt:" in pending_runs[0].output_log

    approve_agent_runs_batch(settings.db_path, command.id, role_name="implementer")

    for _ in range(20):
        tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
        current = service_module.get_command(settings.db_path, command.id)
        if current.stage in {CommandStage.DONE, CommandStage.FAILED}:
            break
    else:
        raise AssertionError("command did not finish")

    completed_runs = list_agent_runs(settings.db_path, command_id=command.id)
    assert any("mock worker started" in run.output_log for run in completed_runs)
    assert any("mock worker completed" in run.output_log for run in completed_runs)

    detail = build_command_detail(tmp_path, command.id)
    assert any(
        "output_log" in item and item["output_log"] for item in detail["agent_runs"]["items"]
    )


def test_request_stop_and_resume_round_trip_for_running_command(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="stop and resume coverage",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )

    first = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    second = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert first.action == "planning_started"
    assert second.action == "tasks_planned"

    stopped = service_module.request_command_stop(settings.db_path, command.id)
    assert stopped.stop_requested is True
    assert stopped.resume_stage == CommandStage.RUNNING

    paused = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert paused.action == "command_paused"
    current = service_module.get_command(settings.db_path, command.id)
    assert current.stage == CommandStage.PAUSED
    assert current.resume_stage == CommandStage.RUNNING

    resumed = service_module.resume_command(settings.db_path, command.id)
    assert resumed.stage == CommandStage.RUNNING
    assert resumed.resume_stage is None

    final = service_module.run_engine(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert final["final_command"]["stage"] == CommandStage.DONE.value


def test_service_mode_helpers_and_context_payload_cover_inference_paths(tmp_path):
    command = CommandRecord(
        id="cmd_helpers",
        goal="design a rollout plan",
        stage=CommandStage.PLANNING,
        workflow_mode=WorkflowMode.AUTO,
        effective_mode=None,
        priority=Priority.HIGH,
        backend=RuntimeBackend.INHERIT,
        workspace_root=str(tmp_path),
        created_at="2026-03-20T00:00:00+00:00",
        updated_at="2026-03-20T00:00:00+00:00",
    )
    task = TaskRecord(
        id="task_helpers",
        command_id=command.id,
        task_key="planning_brief",
        kind="analysis",
        capability="analyst",
        title="Produce a plan",
        description="Create a structured plan",
        state=TaskState.DONE,
        plan_order=1,
        depends_on=[],
        input_payload={"write_files": []},
        output_payload={"summary": "done"},
        error=None,
        attempt_count=1,
        assigned_run_id=None,
        created_at="2026-03-20T00:00:00+00:00",
        updated_at="2026-03-20T00:00:00+00:00",
    )

    assert requested_mode(command) == WorkflowMode.AUTO
    assert effective_mode(command) == WorkflowMode.IMPLEMENTATION
    assert mode_requires_review(WorkflowMode.REVIEW) is True
    assert mode_requires_test(WorkflowMode.RESEARCH) is False
    assert infer_mode_from_goal("Please investigate and report") == WorkflowMode.RESEARCH
    assert infer_mode_from_goal("Please review this diff") == WorkflowMode.REVIEW
    assert infer_mode_from_goal("Please design a rollout plan") == WorkflowMode.PLANNING
    assert (
        infer_mode_from_specs(
            [
                PlannerTaskSpec(
                    key="a",
                    kind="implementation",
                    capability="implementer",
                    title="A",
                    description="A",
                )
            ],
            command.goal,
        )
        == WorkflowMode.IMPLEMENTATION
    )
    assert (
        infer_mode_from_specs(
            [
                PlannerTaskSpec(
                    key="b", kind="analysis", capability="analyst", title="B", description="B"
                )
            ],
            "investigate the architecture",
        )
        == WorkflowMode.RESEARCH
    )
    resolved = resolve_command_mode(
        command,
        PlannerOutput(
            decision=PlannerDecision.CREATE_TASKS,
            workflow_mode=WorkflowMode.REVIEW,
            tasks=[],
        ),
    )
    assert resolved == WorkflowMode.REVIEW
    assert "implementation mode" in mode_prompt_guidance(WorkflowMode.IMPLEMENTATION)
    assert "auto mode" in mode_prompt_guidance(WorkflowMode.AUTO)
    planning_guidance = mode_prompt_guidance(WorkflowMode.PLANNING)
    assert "exactly three top-level sections" in planning_guidance
    assert "## Plan" in planning_guidance
    assert "## Design Direction" in planning_guidance
    assert "## Task Breakdown" in planning_guidance

    specs = add_mode_control_tasks(
        WorkflowMode.IMPLEMENTATION,
        [
            PlannerTaskSpec(
                key="impl",
                kind="implementation",
                capability="implementer",
                title="Impl",
                description="Impl",
            )
        ],
    )
    assert specs[-1].capability == "tester"
    assert add_mode_control_tasks(WorkflowMode.REVIEW, specs[:-1]) == specs[:-1]
    assert build_final_test_spec(["impl"]).depends_on == ["impl"]

    context = build_context_payload(command, [task], [], [], [], task=task)
    assert context["command"]["id"] == "cmd_helpers"
    assert context["task"]["id"] == "task_helpers"
    assert "Completed tasks:" in build_final_response(
        command.model_copy(update={"effective_mode": WorkflowMode.PLANNING}), [task], []
    )

    assert (
        resolve_settings(settings=load_config(tmp_path), repo_root=None).repo_root
        == tmp_path.resolve()
    )
    assert resolve_settings(settings=None, repo_root=tmp_path).repo_root == tmp_path.resolve()

    with pytest.raises(NotImplementedError):
        BackendInterface().plan(command, [], [], [], [])
    with pytest.raises(NotImplementedError):
        BackendInterface().execute_task(command, task, [], [], [], [])
    with pytest.raises(NotImplementedError):
        BackendInterface().review(command, [], [], [], [], reviewer_slot=1)


def test_service_task_creation_validation_and_selection_rules(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_direct(
        tmp_path,
        settings,
        goal="implement validation",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )

    with connect(settings.db_path) as conn:
        with pytest.raises(ValueError, match="Duplicate planner task key"):
            create_task_records(
                conn,
                command,
                [
                    PlannerTaskSpec(
                        key="dup",
                        kind="implementation",
                        capability="implementer",
                        title="First",
                        description="first",
                    ),
                    PlannerTaskSpec(
                        key="dup",
                        kind="implementation",
                        capability="implementer",
                        title="Second",
                        description="second",
                    ),
                ],
            )

        with pytest.raises(ValueError, match="Unknown dependency"):
            create_task_records(
                conn,
                command,
                [
                    PlannerTaskSpec(
                        key="bad-dep",
                        kind="implementation",
                        capability="implementer",
                        title="Bad dep",
                        description="bad",
                        depends_on=["missing-key"],
                    )
                ],
            )

    settings.roles["implementer"].count = 2
    tasks = [
        TaskRecord(
            id="task_a",
            command_id=command.id,
            task_key="a",
            kind="implementation",
            capability="implementer",
            title="A",
            description="A",
            state=TaskState.PENDING,
            plan_order=1,
            depends_on=[],
            input_payload={"write_files": ["src/shared.txt"]},
            output_payload=None,
            error=None,
            attempt_count=0,
            assigned_run_id=None,
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        ),
        TaskRecord(
            id="task_b",
            command_id=command.id,
            task_key="b",
            kind="implementation",
            capability="implementer",
            title="B",
            description="B",
            state=TaskState.PENDING,
            plan_order=2,
            depends_on=[],
            input_payload={"write_files": ["src/shared.txt"]},
            output_payload=None,
            error=None,
            attempt_count=0,
            assigned_run_id=None,
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        ),
        TaskRecord(
            id="task_c",
            command_id=command.id,
            task_key="c",
            kind="implementation",
            capability="implementer",
            title="C",
            description="C",
            state=TaskState.PENDING,
            plan_order=3,
            depends_on=[],
            input_payload={"write_files": ["src/other.txt"]},
            output_payload=None,
            error=None,
            attempt_count=0,
            assigned_run_id=None,
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        ),
    ]
    selected = select_ready_batch(tasks, settings)
    assert [task.id for task in selected] == ["task_a", "task_c"]


def test_service_waiting_question_answer_validation_and_terminal_noops(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_direct(
        tmp_path,
        settings,
        goal="waiting question validation",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, question_state = ? WHERE id = ?",
            (CommandStage.WAITING_QUESTION.value, "none", command.id),
        )
        conn.commit()
        waiting_command = service_module.get_command(settings.db_path, command.id)
        no_answered = reduce_waiting_question(conn, waiting_command)
        assert no_answered.action == "no_op"

        question = create_question_record(
            conn,
            waiting_command,
            source="planner",
            resolution_mode=QuestionResolutionMode.REPLAN_COMMAND,
            resume_stage=CommandStage.PLANNING,
            question="Need clarification?",
        )
        conn.commit()

    answered = answer_question(settings.db_path, question.id, "Proceed.")
    assert answered.state == QuestionState.ANSWERED

    with pytest.raises(ValueError, match="Unknown question"):
        answer_question(settings.db_path, "question_missing", "x")
    with pytest.raises(ValueError, match="Question is not open"):
        answer_question(settings.db_path, question.id, "x")

    with connect(settings.db_path) as conn:
        resolved = reduce_waiting_question(
            conn, service_module.get_command(settings.db_path, command.id)
        )
        assert resolved.action == "question_resolved"

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ? WHERE id = ?",
            (CommandStage.DONE.value, command.id),
        )
        conn.commit()

    terminal = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert terminal.action == "no_op"


def test_service_append_instruction_and_list_helpers_validate_errors(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_direct(
        tmp_path,
        settings,
        goal="append validation",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )

    with pytest.raises(ValueError, match="Instruction body must not be blank."):
        append_instruction(settings.db_path, command.id, "   ")
    with pytest.raises(ValueError, match="Unknown command"):
        append_instruction(settings.db_path, "cmd_missing", "append me")

    appended, _ = append_instruction(settings.db_path, command.id, "Follow this up.")
    instructions = list_instructions(settings.db_path, command_id=command.id)
    assert instructions[0].id == appended.id


def test_append_instruction_rejects_terminal_commands(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_direct(
        tmp_path,
        settings,
        goal="append validation complete",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, final_response = ? WHERE id = ?",
            (CommandStage.DONE.value, "done", command.id),
        )
        conn.commit()

    with pytest.raises(ValueError, match="terminal command"):
        append_instruction(settings.db_path, command.id, "Follow this up.")


def test_submit_command_defaults_to_auto_approval_mode(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="default approval mode",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.MEDIUM,
    )

    assert command.approval_mode == ApprovalMode.AUTO
    stored = service_module.get_command(settings.db_path, command.id)
    assert stored.approval_mode == ApprovalMode.AUTO


def test_submit_command_persists_job_dependencies_and_parallel_flag(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    first = submit_direct(
        tmp_path / "workspace-a",
        settings,
        goal="first job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )
    second = submit_command(
        settings.db_path,
        goal="second job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.MEDIUM,
        workspace_root=tmp_path / "workspace-b",
        depends_on_command_ids=[first.id],
        allow_parallel=True,
        settings=settings,
        repo_root=tmp_path,
    )

    stored = service_module.get_command(settings.db_path, second.id)
    assert stored is not None
    assert stored.depends_on == [first.id]
    assert stored.allow_parallel is False
    assert stored.dependency_state == "waiting"


def test_serial_scheduler_keeps_later_jobs_waiting_when_head_is_blocked(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    first = submit_direct(
        tmp_path / "workspace-a",
        settings,
        goal="first blocked job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )
    second = submit_command(
        settings.db_path,
        goal="second queued job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-b",
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.WAITING_APPROVAL.value, service_module.utc_now(), first.id),
        )
        conn.commit()
        assert select_actionable_command(conn, None) is None

    queued = service_module.get_command(settings.db_path, second.id)
    assert queued is not None
    assert queued.stage == CommandStage.QUEUED


def test_parallel_jobs_can_start_when_workspaces_do_not_overlap(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    first = submit_command(
        settings.db_path,
        goal="parallel first",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-a",
        allow_parallel=True,
        settings=settings,
        repo_root=tmp_path,
    )
    second = submit_command(
        settings.db_path,
        goal="parallel second",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-b",
        allow_parallel=True,
        settings=settings,
        repo_root=tmp_path,
    )

    first_tick = tick_once(settings.db_path, settings=settings, repo_root=tmp_path)
    assert first_tick.command.id == first.id
    second_tick = tick_once(settings.db_path, settings=settings, repo_root=tmp_path)
    assert second_tick.command.id == second.id


def test_ignore_dependencies_and_cancel_actions_for_failed_prerequisite(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    dependency = submit_direct(
        tmp_path / "workspace-a",
        settings,
        goal="failing dependency",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )
    blocked = submit_command(
        settings.db_path,
        goal="blocked job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.MEDIUM,
        workspace_root=tmp_path / "workspace-b",
        depends_on_command_ids=[dependency.id],
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, failure_reason = ?, updated_at = ? WHERE id = ?",
            (
                CommandStage.FAILED.value,
                "dependency failed",
                service_module.utc_now(),
                dependency.id,
            ),
        )
        conn.commit()

    failed_view = service_module.get_command(settings.db_path, blocked.id)
    assert failed_view is not None
    assert failed_view.dependency_state == "failed"
    assert failed_view.can_ignore_dependencies is True

    ignored = ignore_command_dependencies(settings.db_path, blocked.id)
    assert ignored.depends_on == []
    assert ignored.dependency_state == "none"

    canceled_source = submit_command(
        settings.db_path,
        goal="cancel me",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.MEDIUM,
        workspace_root=tmp_path / "workspace-c",
        settings=settings,
        repo_root=tmp_path,
    )
    canceled = cancel_command(settings.db_path, canceled_source.id, reason="Not needed anymore.")
    assert canceled.stage == CommandStage.CANCELED
    assert canceled.failure_reason == "Not needed anymore."


def test_workspace_overlap_detects_ancestor_and_exact_matches(tmp_path):
    base = (tmp_path / "project").resolve()
    child = base / "public"
    sibling = (tmp_path / "other").resolve()
    assert workspace_roots_overlap(base, child)
    assert workspace_roots_overlap(child, base)
    assert workspace_roots_overlap(base, base)
    assert not workspace_roots_overlap(base, sibling)


def test_agent_run_approval_and_denial_flow(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["planner"].runtime = RuntimeBackend.CODEX
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="approval path",
        workflow_mode=WorkflowMode.PLANNING,
        approval_mode=ApprovalMode.MANUAL,
        priority=Priority.HIGH,
    )

    tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
    approval = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert approval.action == "approval_required"
    assert approval.command.stage == CommandStage.WAITING_APPROVAL

    agent_runs = list_agent_runs(settings.db_path, command_id=command.id)
    assert len(agent_runs) == 1
    assert agent_runs[0].state.value == "pending_approval"

    approved = approve_agent_run(settings.db_path, agent_runs[0].id)
    assert approved.state.value == "approved"
    resumed = service_module.get_command(settings.db_path, command.id)
    assert resumed.stage == CommandStage.PLANNING

    class CompletingPlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.PLANNING,
                final_response="approved and completed",
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: CompletingPlanner())
    done = tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
    assert done.action == "command_completed"
    assert done.command.stage == CommandStage.DONE

    denied_command = submit_direct(
        tmp_path,
        settings,
        goal="denial path",
        workflow_mode=WorkflowMode.PLANNING,
        approval_mode=ApprovalMode.MANUAL,
        priority=Priority.HIGH,
    )
    tick_once(settings.db_path, command_id=denied_command.id, settings=settings, repo_root=tmp_path)
    denied_outcome = tick_once(
        settings.db_path, command_id=denied_command.id, settings=settings, repo_root=tmp_path
    )
    assert denied_outcome.action == "approval_required"
    denied_run = list_agent_runs(settings.db_path, command_id=denied_command.id)[0]
    denied = deny_agent_run(settings.db_path, denied_run.id, reason="No external runs allowed.")
    assert denied.state.value == "denied"
    failed = service_module.get_command(settings.db_path, denied_command.id)
    assert failed.stage == CommandStage.FAILED
    assert failed.failure_reason == "No external runs allowed."


def test_parallel_implementation_surfaces_multiple_pending_agent_approvals(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["implementer"].runtime = RuntimeBackend.CODEX
    settings.roles["implementer"].count = 2
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="[parallel] implement with multiple approvals",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        approval_mode=ApprovalMode.MANUAL,
        priority=Priority.HIGH,
    )

    tick_once(settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path)
    planned = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert planned.action == "tasks_planned"

    approval = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert approval.action == "approval_required"
    assert approval.command.stage == CommandStage.WAITING_APPROVAL

    pending_runs = [
        run
        for run in list_agent_runs(settings.db_path, command_id=command.id)
        if run.state.value == "pending_approval"
    ]
    assert len(pending_runs) == 2
    assert {run.title for run in pending_runs} == {"Implement part A", "Implement part B"}

    approved = approve_agent_runs_batch(settings.db_path, command.id, role_name="implementer")
    assert len(approved) == 2
    assert all(run.state.value == "approved" for run in approved)


def test_service_dispatch_failure_and_verification_bypass_paths(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_direct(
        tmp_path,
        settings,
        goal="dispatch failure",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )

    with connect(settings.db_path) as conn:
        now = "2026-03-20T00:00:00+00:00"
        conn.execute(
            """
            INSERT INTO tasks (
                id, command_id, task_key, kind, capability, title, description, state, plan_order,
                input_payload, output_payload, error, attempt_count, assigned_run_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "task_blocked",
                command.id,
                "blocked",
                "implementation",
                "implementer",
                "Blocked task",
                "blocked",
                TaskState.BLOCKED.value,
                1,
                json.dumps({"write_files": ["src/blocked.txt"]}),
                None,
                None,
                0,
                None,
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO tasks (
                id, command_id, task_key, kind, capability, title, description, state, plan_order,
                input_payload, output_payload, error, attempt_count, assigned_run_id, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "task_pending",
                command.id,
                "pending",
                "implementation",
                "implementer",
                "Pending task",
                "pending",
                TaskState.PENDING.value,
                2,
                json.dumps({"write_files": ["src/pending.txt"]}),
                None,
                None,
                0,
                None,
                now,
                now,
            ),
        )
        conn.execute(
            "INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?, ?)",
            ("task_pending", "task_blocked"),
        )
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.RUNNING.value, WorkflowMode.IMPLEMENTATION.value, command.id),
        )
        conn.commit()

    outcome = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert outcome.action == "dispatch_failed"
    assert outcome.command.stage == CommandStage.FAILED

    verifying_command = submit_direct(
        tmp_path,
        settings,
        goal="verification bypass",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ?, replan_requested = 1 WHERE id = ?",
            (CommandStage.VERIFYING.value, WorkflowMode.REVIEW.value, verifying_command.id),
        )
        conn.commit()

    verifying = tick_once(
        settings.db_path,
        command_id=verifying_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert verifying.action == "replanning_requested"
    assert verifying.command.stage == CommandStage.REPLANNING


def test_reduce_planning_failure_implicit_gate_and_backend_selection(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["planner"].runtime = RuntimeBackend.CODEX
    settings.roles["reviewer"].runtime = RuntimeBackend.CLAUDE
    initialize_database(settings.db_path)

    structured = backend_for(
        CommandRecord(
            id="cmd_structured",
            goal="structured",
            stage=CommandStage.PLANNING,
            workflow_mode=WorkflowMode.IMPLEMENTATION,
            effective_mode=WorkflowMode.IMPLEMENTATION,
            priority=Priority.HIGH,
            backend=RuntimeBackend.INHERIT,
            workspace_root=str(tmp_path),
            created_at="2026-03-20T00:00:00+00:00",
            updated_at="2026-03-20T00:00:00+00:00",
        ),
        settings,
        "planner",
    )
    assert isinstance(structured, StructuredCliBackend)
    assert (
        backend_for(
            CommandRecord(
                id="cmd_mock",
                goal="mock",
                stage=CommandStage.PLANNING,
                workflow_mode=WorkflowMode.IMPLEMENTATION,
                effective_mode=WorkflowMode.IMPLEMENTATION,
                priority=Priority.HIGH,
                backend=RuntimeBackend.MOCK,
                workspace_root=str(tmp_path),
                created_at="2026-03-20T00:00:00+00:00",
                updated_at="2026-03-20T00:00:00+00:00",
            ),
            settings,
            "planner",
        ).__class__.__name__
        == "MockBackend"
    )

    failing_command = submit_direct(
        tmp_path,
        settings,
        goal="planner failure",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.IMPLEMENTATION.value, failing_command.id),
        )
        conn.commit()

    class RaisingPlanner:
        def plan(self, *args, **kwargs):
            raise RuntimeError("planner exploded")

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: RaisingPlanner())
    failed = tick_once(
        settings.db_path,
        command_id=failing_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert failed.action == "planning_failed"
    assert failed.command.stage == CommandStage.FAILED

    complete_command = submit_direct(
        tmp_path,
        settings,
        goal="implicit tester gate",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.IMPLEMENTATION.value, complete_command.id),
        )
        conn.commit()

    class CompletePlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.IMPLEMENTATION,
                final_response="Planner said complete.",
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: CompletePlanner())
    implicit_gate = tick_once(
        settings.db_path,
        command_id=complete_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert implicit_gate.action == "tasks_planned"
    assert implicit_gate.tasks[0].capability == "tester"

    review_command = submit_direct(
        tmp_path,
        settings,
        goal="review complete",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.REVIEW.value, review_command.id),
        )
        conn.commit()

    class ReviewCompletePlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.REVIEW,
                final_response="Ready for reviewers.",
            )

    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: ReviewCompletePlanner()
    )
    review_gate = tick_once(
        settings.db_path,
        command_id=review_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert review_gate.action == "verification_started"
    assert review_gate.command.stage == CommandStage.VERIFYING

    planning_command = submit_direct(
        tmp_path,
        settings,
        goal="planning final response",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.PLANNING.value, planning_command.id),
        )
        conn.commit()

    class PlanningCompletePlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.PLANNING,
                final_response="Plan is ready.",
            )

    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: PlanningCompletePlanner()
    )
    planned = tick_once(
        settings.db_path,
        command_id=planning_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert planned.action == "command_completed"
    assert planned.command.stage == CommandStage.DONE
    artifacts = list_artifacts(settings.db_path, command_id=planning_command.id)
    assert [artifact.metadata["section_key"] for artifact in artifacts] == [
        "plan",
        "design_direction",
        "task_breakdown",
    ]
    assert artifacts[0].metadata["format"] == "markdown"
    assert "Goal: planning final response" in artifacts[0].body
    assert "Plan is ready." in artifacts[0].body
    assert artifacts[2].body.strip() == "- No tasks were recorded."

    failure_command = submit_direct(
        tmp_path,
        settings,
        goal="planner fail result",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.RESEARCH.value, failure_command.id),
        )
        conn.commit()

    class FailPlanner:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.FAIL,
                workflow_mode=WorkflowMode.RESEARCH,
                failure_reason="explicit planner failure",
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: FailPlanner())
    failed = tick_once(
        settings.db_path,
        command_id=failure_command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert failed.action == "planning_failed"
    assert failed.command.failure_reason == "explicit planner failure"
