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
    list_agent_runs,
    list_instructions,
    mode_prompt_guidance,
    mode_requires_review,
    mode_requires_test,
    requested_mode,
    reduce_waiting_question,
    resolve_command_mode,
    resolve_settings,
    select_ready_batch,
    submit_command,
    tick_once,
)


runner = CliRunner()


def read_json(result):
    assert result.exit_code == 0, result.stdout
    return json.loads(result.stdout)


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


def test_init_repo_config_is_idempotent_and_load_config_creates_workdir(tmp_path, monkeypatch):
    created = init_repo_config(tmp_path)
    assert set(created) == {"wevra.ini", "agents.ini", ".env"}
    assert init_repo_config(tmp_path) == {}

    (tmp_path / "wevra.ini").write_text(
        """[runtime]
working_dir = worktree
db_path = runtime/app.db
language = ja
auto_approve_agent_actions = true
agent_timeout_seconds = 321
home = runtime-home

[ui]
auto_start = false
port = 45000
open_browser = false
language = ja
host = 0.0.0.0

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
    assert settings.auto_approve_agent_actions is True
    assert settings.agent_timeout_seconds == 321
    assert settings.working_dir == (tmp_path / "worktree").resolve()
    assert settings.working_dir.is_dir()
    assert settings.db_path == (tmp_path / "runtime/app.db").resolve()
    assert settings.ui_auto_start is False
    assert settings.ui_open_browser is False
    assert settings.ui_host == "0.0.0.0"
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

    custom_db = tmp_path / "custom.sqlite3"
    init_db = runner.invoke(app, ["init-db", "--db-path", str(custom_db)])
    assert init_db.exit_code == 0
    assert str(custom_db.resolve()) in init_db.stdout

    submitted = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--mode",
                "research",
                "--workspace-root",
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

    server = create_server(tmp_path, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base_url = f"http://{host}:{port}"

        with request.urlopen(f"{base_url}/") as response:
            html = response.read().decode("utf-8")
        assert "Wevra Runtime" in html

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
        lambda self: SimpleNamespace(repo_root=str(tmp_path), host="127.0.0.1", port=43861),
    )
    monkeypatch.setattr(dashboard_module, "create_server", lambda *args, **kwargs: FakeServer())
    dashboard_module.main()
    assert closed == ["served", "closed"]


def test_dashboard_url_uses_localhost_when_binding_all_interfaces(tmp_path):
    init_repo_config(tmp_path)
    (tmp_path / "wevra.ini").write_text(
        (tmp_path / "wevra.ini")
        .read_text(encoding="utf-8")
        .replace("host = 127.0.0.1", "host = 0.0.0.0"),
        encoding="utf-8",
    )
    settings = load_config(tmp_path)
    assert dashboard_module.dashboard_url(settings) == f"http://127.0.0.1:{settings.ui_port}"


def test_build_snapshot_includes_counts_roles_and_active_commands(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    submitted = read_json(runner.invoke(app, ["submit", "[worker_question] snapshot coverage"]))
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    snapshot = build_snapshot(tmp_path)
    assert snapshot["runtime"]["working_dir"] == str(tmp_path.resolve())
    assert snapshot["runtime"]["roles"]
    assert snapshot["commands"]["counts"]["waiting_question"] == 1
    assert snapshot["questions"]["counts"]["open"] == 1
    assert snapshot["commands"]["active"][0]["id"] == command_id
    assert any(run["command_id"] == command_id for run in snapshot["agent_runs"]["items"])
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

    server = create_server(tmp_path, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        base_url = f"http://{host}:{port}"

        created = request_json(
            f"{base_url}/api/commands",
            method="POST",
            body=json.dumps({"goal": "needs approval", "workflow_mode": "planning"}).encode(
                "utf-8"
            ),
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
        runner.invoke(app, ["submit", "[worker_question] split payload coverage"])
    )
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    summary = build_summary_snapshot(tmp_path)
    detail = build_command_detail(tmp_path, command_id)

    assert summary["commands"]["items"][0]["id"] == command_id
    assert summary["questions"]["open"][0]["command_id"] == command_id
    assert "tasks" not in summary
    assert detail["command"]["id"] == command_id
    assert detail["tasks"]["items"]
    assert detail["questions"]["items"][0]["command_id"] == command_id


def test_structured_cli_backends_build_commands_and_handle_failures(tmp_path, monkeypatch):
    runtime_home = tmp_path / "runtime-home"

    codex_runs = []

    def fake_codex_run(command, cwd, env, text, capture_output, check, timeout):
        codex_runs.append((command, cwd, env))
        output_path = Path(command[command.index("--output-last-message") + 1])
        output_path.write_text(json.dumps({"decision": "complete"}), encoding="utf-8")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(service_module.subprocess, "run", fake_codex_run)
    codex_backend = StructuredCliBackend(
        RuntimeBackend.CODEX,
        model="gpt-test",
        auto_approve_agent_actions=False,
        timeout_seconds=30,
        runtime_home=runtime_home,
    )
    codex_payload = codex_backend._run_codex("hello", {"type": "object"}, tmp_path)
    codex_command, codex_cwd, codex_env = codex_runs[0]
    assert codex_payload == {"decision": "complete"}
    assert codex_command[:2] == ["codex", "exec"]
    assert "--model" in codex_command
    assert "--full-auto" in codex_command
    assert codex_cwd == str(tmp_path)
    assert codex_env["HOME"] == str(runtime_home)

    def fake_claude_run(command, cwd, env, text, capture_output, check, timeout):
        assert "--dangerously-skip-permissions" in command
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"decision": "approve", "summary": "ok", "findings": []}),
            stderr="",
        )

    monkeypatch.setattr(service_module.subprocess, "run", fake_claude_run)
    claude_backend = StructuredCliBackend(
        RuntimeBackend.CLAUDE,
        model="opus-test",
        auto_approve_agent_actions=True,
        timeout_seconds=30,
        runtime_home=runtime_home,
    )
    claude_payload = claude_backend._run_claude("review", {"type": "object"}, tmp_path)
    assert claude_payload["decision"] == "approve"

    monkeypatch.setattr(
        service_module.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr="boom"),
    )
    with pytest.raises(RuntimeError, match="boom"):
        codex_backend._run_codex("broken", {"type": "object"}, tmp_path)
    with pytest.raises(RuntimeError, match="boom"):
        claude_backend._run_claude("broken", {"type": "object"}, tmp_path)
    with pytest.raises(RuntimeError, match="Unsupported backend"):
        StructuredCliBackend(
            RuntimeBackend.INHERIT,
            model="",
            auto_approve_agent_actions=False,
            timeout_seconds=30,
        )._run_structured("prompt", {}, str(tmp_path))


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
    command = submit_command(
        settings.db_path,
        goal="implement validation",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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
    command = submit_command(
        settings.db_path,
        goal="waiting question validation",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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
    command = submit_command(
        settings.db_path,
        goal="append validation",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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
    command = submit_command(
        settings.db_path,
        goal="append validation complete",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, final_response = ? WHERE id = ?",
            (CommandStage.DONE.value, "done", command.id),
        )
        conn.commit()

    with pytest.raises(ValueError, match="terminal command"):
        append_instruction(settings.db_path, command.id, "Follow this up.")


def test_agent_run_approval_and_denial_flow(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["planner"].runtime = RuntimeBackend.CODEX
    settings.auto_approve_agent_actions = False
    initialize_database(settings.db_path)

    command = submit_command(
        settings.db_path,
        goal="approval path",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    denied_command = submit_command(
        settings.db_path,
        goal="denial path",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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


def test_service_dispatch_failure_and_verification_bypass_paths(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)
    command = submit_command(
        settings.db_path,
        goal="dispatch failure",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    verifying_command = submit_command(
        settings.db_path,
        goal="verification bypass",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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
    settings.auto_approve_agent_actions = True
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

    failing_command = submit_command(
        settings.db_path,
        goal="planner failure",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    complete_command = submit_command(
        settings.db_path,
        goal="implicit tester gate",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    review_command = submit_command(
        settings.db_path,
        goal="review complete",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    planning_command = submit_command(
        settings.db_path,
        goal="planning final response",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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

    failure_command = submit_command(
        settings.db_path,
        goal="planner fail result",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
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
