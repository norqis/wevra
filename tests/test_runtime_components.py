import json
import signal
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Optional
from urllib import error, request

import pytest
from typer.testing import CliRunner

import wevra.cli as cli_module
import wevra.config as config_module
import wevra.dashboard as dashboard_module
import wevra.service as service_module
from wevra import __version__
from wevra.cli import app
from wevra.config import (
    RuntimeBackend,
    init_repo_config,
    load_config,
    normalize_bool,
    read_example_template,
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
    OperatorIssueKind,
    PlannerDecision,
    PlannerOutput,
    PlannerTaskSpec,
    Priority,
    QuestionResolutionMode,
    QuestionState,
    ReviewDecision,
    ReviewerOutput,
    TaskRecord,
    TaskState,
    WorkerDecision,
    WorkerOutput,
    WorkflowMode,
)
from wevra.runtime_registry import structured_runtime_adapter
from wevra.service import (
    AgentExecutionError,
    advance_frontier_once,
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
    build_review_context_payload,
    build_final_response,
    build_final_test_spec,
    create_question_record,
    create_task_records,
    detect_test_command,
    effective_mode,
    execute_deterministic_test_task,
    infer_mode_from_goal,
    infer_mode_from_specs,
    generate_command_title,
    generate_job_split_preview,
    ignore_command_dependencies,
    is_deterministic_test_gate,
    cancel_command_with_repair,
    list_agent_runs,
    list_artifacts,
    list_instructions,
    list_tasks,
    mode_prompt_guidance,
    mode_requires_review,
    mode_requires_test,
    requested_mode,
    reduce_waiting_question,
    retry_operator_issue,
    resolve_command_mode,
    resolve_settings,
    select_actionable_command,
    select_actionable_commands,
    select_ready_batch,
    _split_preview_prompt,
    submit_command,
    submit_job_split_preview,
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
    status, payload, _ = request_json_with_headers(url, method=method, body=body, headers=headers)
    return status, payload


def request_json_with_headers(
    url: str, *, method: str = "GET", body: Optional[bytes] = None, headers=None
):
    req = request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with request.urlopen(req) as response:
            return response.status, json.loads(response.read().decode("utf-8")), response.headers
    except error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8")), exc.headers


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
    assert (tmp_path / "wevra.ini").read_text(encoding="utf-8") == read_example_template(
        "wevra.ini"
    )
    assert (tmp_path / "agents.ini").read_text(encoding="utf-8") == read_example_template(
        "agents.ini"
    )
    assert (tmp_path / ".env").read_text(encoding="utf-8") == read_example_template(".env")
    assert init_repo_config(tmp_path) == {}

    (tmp_path / "wevra.ini").write_text(
        """[runtime]
db_path = runtime/app.db
language = ja
agent_timeout_seconds = 321
home = runtime-home

[testing]
command = ./custom-test.sh

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

[implementer]
runtime = codex
model = gpt-test
count = 4

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
    assert settings.test_command == "./custom-test.sh"
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
    assert fallback_role.runtime == RuntimeBackend.CODEX
    assert fallback_role.model == "gpt-test"


def test_read_example_template_falls_back_to_packaged_templates(monkeypatch):
    monkeypatch.setattr(config_module, "template_repo_root", lambda: Path("/tmp/does-not-exist"))
    assert read_example_template("wevra.ini").startswith("[runtime]\n")
    assert "db_path = .wevra/wevra.db" in read_example_template("wevra.ini")
    assert "[planner]" in read_example_template("agents.ini")
    assert "DISCORD_WEBHOOK_URL=" in read_example_template(".env")


def test_detect_test_command_prefers_local_pytest_and_respects_configured_command(tmp_path):
    local_pytest = tmp_path / ".venv" / "bin" / "pytest"
    local_pytest.parent.mkdir(parents=True, exist_ok=True)
    local_pytest.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    local_pytest.chmod(0o755)

    assert detect_test_command(tmp_path, None) == [str(local_pytest), "-q"]
    assert detect_test_command(tmp_path, "python -m pytest -q") == [
        "python",
        "-m",
        "pytest",
        "-q",
    ]


def test_execute_deterministic_test_task_runs_configured_command(tmp_path):
    db_path = tmp_path / ".wevra" / "wevra.db"
    initialize_database(db_path)
    script = tmp_path / "fake-test.sh"
    script.write_text("#!/bin/sh\nprintf 'tests ok\\n'\n", encoding="utf-8")
    script.chmod(0o755)
    settings = SimpleNamespace(
        db_path=db_path,
        runtime_home=None,
        agent_timeout_seconds=30,
        test_command=str(script),
    )
    command = CommandRecord(
        id="cmd_test",
        goal="run tests",
        stage=CommandStage.RUNNING,
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        approval_mode=ApprovalMode.AUTO,
        effective_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.MEDIUM,
        backend=RuntimeBackend.INHERIT,
        workspace_root=str(tmp_path),
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    task = TaskRecord(
        id="task_test_gate",
        command_id=command.id,
        task_key="__system_test_gate__",
        kind="test",
        capability="tester",
        title="Run existing feature and unit tests",
        description="Execute the existing test suite against the completed implementation before the final review pass.",
        state=TaskState.RUNNING,
        plan_order=1,
        input_payload={"system_generated": True, "gate": "final_test"},
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )

    assert is_deterministic_test_gate(task) is True
    output = execute_deterministic_test_task(command, task, settings)

    assert output.decision == WorkerDecision.COMPLETE
    assert "Executed existing tests" in (output.summary or "")
    assert output.result["test_command"] == [str(script)]
    assert output.result["stdout_tail"] == ["tests ok"]


def test_submit_command_requires_existing_runbook_path_for_dogfooding(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    with pytest.raises(ValueError, match="runbook_path_required"):
        submit_direct(
            tmp_path,
            settings,
            goal="dogfood this workspace",
            workflow_mode=WorkflowMode.DOGFOODING,
            priority=Priority.HIGH,
        )

    with pytest.raises(ValueError, match="runbook_path_not_found"):
        submit_direct(
            tmp_path,
            settings,
            goal="dogfood this workspace",
            workflow_mode=WorkflowMode.DOGFOODING,
            priority=Priority.HIGH,
            runbook_path=tmp_path / "missing-runbook.md",
        )

    runbook = tmp_path / "RUNBOOK.md"
    runbook.write_text("# Runbook\n\n1. Start the app.\n", encoding="utf-8")

    command = submit_direct(
        tmp_path,
        settings,
        goal="dogfood this workspace",
        workflow_mode=WorkflowMode.DOGFOODING,
        priority=Priority.HIGH,
        runbook_path=runbook,
    )

    assert command.runbook_path == str(runbook.resolve())


def test_generate_job_split_preview_returns_dependency_graph(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    preview = generate_job_split_preview(
        goal="Update docs and implementation for the same change",
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
    )

    assert len(preview.items) == 3
    assert preview.items[0].key == "implement"
    assert preview.items[1].key == "docs"
    assert preview.items[0].title == "Implement the code changes"
    assert "Apply the requested code changes in the main workspace" in preview.items[0].goal
    assert preview.items[1].allow_parallel is False
    assert preview.items[2].depends_on == ["implement", "docs"]
    assert preview.items[1].workspace_root == str((tmp_path / "docs").resolve())
    assert "documentation and usage notes" in preview.items[1].goal
    assert "implementation and documentation updates together" in preview.items[2].goal


def test_generate_job_split_preview_uses_requested_locale(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    preview = generate_job_split_preview(
        goal="README と CLI 表示を整理する",
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
        locale="ja",
    )

    assert preview.summary == "計画、実装、確認の3段階に分けて進めます。"
    assert preview.items[0].title == "実装計画を作る"
    assert "具体的な計画を作成する" in preview.items[0].goal
    assert preview.items[1].title == "変更を実装する"
    assert preview.items[2].title == "変更結果をレビューする"


def test_generate_command_title_falls_back_when_structured_runtime_errors(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    settings.roles["planner"].runtime = RuntimeBackend.CODEX
    settings.roles["planner"].model = "gpt-test"
    monkeypatch.setattr(
        service_module,
        "_structured_command_title",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("cli failed")),
    )

    title = generate_command_title(
        goal="README の CLI 説明を整理して最新状態に合わせる",
        workflow_mode=WorkflowMode.PLANNING,
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
        locale="ja",
    )

    assert title == "README の CLI 説明を整理して最新状態に合わせる"


def test_run_engine_persists_job_contract_and_threads_memory_between_roles(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    guidance = {
        "contract": (
            "## Mission\n- Keep the CLI wording narrow.\n\n"
            "## Guardrails\n- Only touch CLI-facing copy.\n\n"
            "## Do Not Do\n- Do not rename commands.\n\n"
            "## Definition of Done\n- Update the requested text and nothing else.\n"
        ),
        "planner_memory": (
            "## Established Constraints\n- Keep the scope on CLI wording.\n\n"
            "## Confirmed Progress\n- Planning established a single implementation task.\n\n"
            "## Open Cautions\n- Preserve existing command names.\n"
        ),
        "worker_memory": (
            "## Established Constraints\n- Keep the scope on CLI wording.\n\n"
            "## Confirmed Progress\n- Updated the requested CLI text.\n\n"
            "## Open Cautions\n- Review should verify that command names stayed unchanged.\n"
        ),
        "review_memory": (
            "## Established Constraints\n- Keep the scope on CLI wording.\n\n"
            "## Confirmed Progress\n- Implementation and review both completed.\n\n"
            "## Open Cautions\n- No open cautions remain.\n"
        ),
    }
    observed = {"worker": None, "reviewer": None}

    class GuidanceBackend(BackendInterface):
        def plan(
            self,
            command,
            tasks,
            questions,
            reviews,
            instructions,
            job_contract=None,
            job_memory=None,
            **kwargs,
        ):
            assert "CLI wording" in command.goal
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=WorkflowMode.IMPLEMENTATION,
                job_contract=guidance["contract"],
                job_memory=guidance["planner_memory"],
                tasks=[
                    PlannerTaskSpec(
                        key="cli_copy",
                        kind="implementation",
                        capability="implementer",
                        title="Update CLI wording",
                        description="Adjust the requested CLI wording only.",
                        write_files=["README.md"],
                    )
                ],
            )

        def execute_task(
            self,
            command,
            task,
            tasks,
            questions,
            reviews,
            instructions,
            job_contract=None,
            job_memory=None,
            **kwargs,
        ):
            observed["worker"] = {"job_contract": job_contract, "job_memory": job_memory}
            return WorkerOutput(
                decision=WorkerDecision.COMPLETE,
                summary="Updated the CLI wording.",
                job_memory=guidance["worker_memory"],
                result={"summary": "Updated the CLI wording."},
            )

        def review(
            self,
            command,
            tasks,
            questions,
            reviews,
            instructions,
            reviewer_slot,
            job_contract=None,
            job_memory=None,
            **kwargs,
        ):
            observed["reviewer"] = {"job_contract": job_contract, "job_memory": job_memory}
            return ReviewerOutput(
                decision=ReviewDecision.APPROVE,
                summary="Looks good.",
                findings=[],
                job_memory=guidance["review_memory"],
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: GuidanceBackend())

    command = submit_direct(
        tmp_path,
        settings,
        goal="Tighten the CLI wording around pause and resume without renaming commands.",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )

    result = service_module.run_engine(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )

    assert result["final_command"]["stage"] == CommandStage.DONE.value
    assert observed["worker"] == {
        "job_contract": guidance["contract"].strip(),
        "job_memory": guidance["planner_memory"].strip(),
    }
    assert observed["reviewer"] == {
        "job_contract": guidance["contract"].strip(),
        "job_memory": guidance["worker_memory"].strip(),
    }

    artifacts = list_artifacts(settings.db_path, command_id=command.id)
    artifacts_by_kind = {artifact.kind: artifact.body for artifact in artifacts}
    assert artifacts_by_kind["job_contract_markdown"] == guidance["contract"]
    assert artifacts_by_kind["job_memory_markdown"] == guidance["review_memory"]
    assert "Goal:" in artifacts_by_kind["result_markdown"]


def test_structured_backend_prompts_include_job_contract_and_memory(tmp_path):
    command = CommandRecord(
        id="cmd_prompt",
        title="CLI wording",
        goal="Tighten the CLI wording around pause and resume.",
        stage=CommandStage.PLANNING,
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        approval_mode=ApprovalMode.AUTO,
        effective_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
        workspace_root=str(tmp_path),
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    task = TaskRecord(
        id="task_prompt",
        command_id=command.id,
        task_key="cli_copy",
        kind="implementation",
        capability="implementer",
        title="Update CLI wording",
        description="Adjust the requested CLI wording only.",
        state=TaskState.PENDING,
        plan_order=1,
        input_payload={"write_files": ["README.md"]},
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    contract = (
        "## Mission\n- Keep the CLI wording narrow.\n\n"
        "## Guardrails\n- Only touch CLI-facing copy.\n\n"
        "## Do Not Do\n- Do not rename commands.\n\n"
        "## Definition of Done\n- Update the requested text and nothing else.\n"
    )
    memory = (
        "## Established Constraints\n- Keep the scope on CLI wording.\n\n"
        "## Confirmed Progress\n- Planning established the implementation pass.\n\n"
        "## Open Cautions\n- Preserve existing command names.\n"
    )
    planner_capture = {}
    worker_capture = {}
    reviewer_capture = {}

    planner_backend = service_module.StructuredCliBackend(
        RuntimeBackend.CODEX,
        "gpt-test",
        ApprovalMode.AUTO,
        60,
    )
    worker_backend = service_module.StructuredCliBackend(
        RuntimeBackend.CODEX,
        "gpt-test",
        ApprovalMode.AUTO,
        60,
    )
    reviewer_backend = service_module.StructuredCliBackend(
        RuntimeBackend.CLAUDE,
        "opus-test",
        ApprovalMode.AUTO,
        60,
    )

    def fake_plan(prompt, *_args, **_kwargs):
        planner_capture["prompt"] = prompt
        return {
            "decision": "complete",
            "workflow_mode": "planning",
            "job_contract": contract,
            "job_memory": memory,
            "final_response": "## Plan\n- done",
        }

    def fake_worker(prompt, *_args, **_kwargs):
        worker_capture["prompt"] = prompt
        return {
            "decision": "complete",
            "summary": "done",
            "job_memory": memory,
            "result": {},
        }

    def fake_review(prompt, *_args, **_kwargs):
        reviewer_capture["prompt"] = prompt
        return {
            "decision": "approve",
            "summary": "ok",
            "job_memory": memory,
            "findings": [],
        }

    planner_backend._run_structured = fake_plan  # type: ignore[method-assign]
    worker_backend._run_structured = fake_worker  # type: ignore[method-assign]
    reviewer_backend._run_structured = fake_review  # type: ignore[method-assign]

    planner_backend.plan(
        command,
        [task],
        [],
        [],
        [],
        job_contract=contract,
        job_memory=memory,
    )
    worker_backend.execute_task(
        command,
        task,
        [task],
        [],
        [],
        [],
        job_contract=contract,
        job_memory=memory,
    )
    reviewer_backend.review(
        command,
        [task],
        [],
        [],
        [],
        reviewer_slot=1,
        job_contract=contract,
        job_memory=memory,
    )

    assert "Current Job Contract" in planner_capture["prompt"]
    assert "Current Job Memory" in planner_capture["prompt"]
    assert "job_contract must use exactly these H2 sections" in planner_capture["prompt"]
    assert contract.strip() in planner_capture["prompt"]
    assert "Treat the Job Contract as binding scope guidance" in worker_capture["prompt"]
    assert "Return job_memory as a full replacement markdown document" in worker_capture["prompt"]
    assert memory.strip() in worker_capture["prompt"]
    assert "Carry forward still-valid constraints" in reviewer_capture["prompt"]
    assert (
        "Ignore unrelated pre-existing workspace changes outside the requested change"
        in reviewer_capture["prompt"]
    )


def test_structured_backend_review_prompt_has_mode_specific_guidance(tmp_path):
    contract = (
        "## Mission\n- Review the requested files.\n\n"
        "## Guardrails\n- Stay read-only.\n\n"
        "## Do Not Do\n- Do not edit files.\n\n"
        "## Definition of Done\n- Produce concrete findings.\n"
    )
    memory = (
        "## Established Constraints\n- Stay read-only.\n\n"
        "## Confirmed Progress\n- Review prep is complete.\n\n"
        "## Open Cautions\n- Keep findings concrete.\n"
    )
    review_command = CommandRecord(
        id="cmd_review",
        title="Review docs",
        goal="Review README.md and CHANGELOG.md and summarize issues.",
        stage=CommandStage.VERIFYING,
        workflow_mode=WorkflowMode.REVIEW,
        approval_mode=ApprovalMode.AUTO,
        effective_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
        workspace_root=str(tmp_path),
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    dogfood_command = CommandRecord(
        id="cmd_dogfood",
        title="Dogfood onboarding flow",
        goal="Dogfood the onboarding flow in this workspace.",
        stage=CommandStage.VERIFYING,
        workflow_mode=WorkflowMode.DOGFOODING,
        approval_mode=ApprovalMode.AUTO,
        effective_mode=WorkflowMode.DOGFOODING,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
        workspace_root=str(tmp_path),
        runbook_path=str((tmp_path / "RUNBOOK.md").resolve()),
        created_at="2026-01-01T00:00:00+00:00",
        updated_at="2026-01-01T00:00:00+00:00",
    )
    backend = service_module.StructuredCliBackend(
        RuntimeBackend.CODEX,
        "gpt-test",
        ApprovalMode.AUTO,
        60,
    )
    captured_prompts: list[str] = []

    def fake_review(prompt, *_args, **_kwargs):
        captured_prompts.append(prompt)
        return {
            "decision": "approve",
            "summary": "ok",
            "job_memory": memory,
            "findings": [],
        }

    backend._run_structured = fake_review  # type: ignore[method-assign]

    backend.review(
        review_command,
        [],
        [],
        [],
        [],
        reviewer_slot=1,
        job_contract=contract,
        job_memory=memory,
    )
    backend.review(
        dogfood_command,
        [],
        [],
        [],
        [],
        reviewer_slot=1,
        job_contract=contract,
        job_memory=memory,
    )

    assert (
        "Do not request changes merely because the reviewed workspace still has issues"
        in captured_prompts[0]
    )
    assert (
        "The goal is to judge whether the runbook-driven verification loop surfaced and fixed the important issues"
        in captured_prompts[1]
    )


def test_submit_job_split_preview_creates_commands_with_dependencies(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    preview = generate_job_split_preview(
        goal="Update docs and implementation for the same change",
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
    )

    created = submit_job_split_preview(
        settings.db_path,
        preview=preview,
        approval_mode=ApprovalMode.AUTO,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
    )

    assert len(created) == 3
    assert {command.goal for command in created} == {item.goal for item in preview.items}
    docs_command = next(command for command in created if command.workspace_root.endswith("/docs"))
    verify_command = next(
        command
        for command in created
        if "Review the finished implementation and documentation updates together" in command.goal
    )
    assert docs_command.depends_on == []
    assert docs_command.allow_parallel is False
    assert len(verify_command.depends_on) == 2


def test_submit_command_persists_generated_title(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    monkeypatch.setattr(
        service_module,
        "generate_command_title",
        lambda **kwargs: "CLI 表示を整理する",
    )

    command = submit_direct(
        tmp_path,
        settings,
        goal="CLI フラグ名を変更して README の例も更新する",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        locale="ja",
    )

    assert command.title == "CLI 表示を整理する"
    assert command.goal == "CLI フラグ名を変更して README の例も更新する"

    with connect(settings.db_path) as conn:
        row = conn.execute(
            "SELECT title, goal FROM commands WHERE id = ?", (command.id,)
        ).fetchone()
    assert row["title"] == "CLI 表示を整理する"
    assert row["goal"] == "CLI フラグ名を変更して README の例も更新する"


def test_submit_job_split_preview_persists_preview_titles(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)

    preview = generate_job_split_preview(
        goal="README と CLI 表示を整理する",
        workspace_root=tmp_path,
        settings=settings,
        repo_root=tmp_path,
        locale="ja",
    )

    created = submit_job_split_preview(
        settings.db_path,
        preview=preview,
        approval_mode=ApprovalMode.AUTO,
        priority=Priority.HIGH,
        settings=settings,
        repo_root=tmp_path,
    )

    preview_by_goal = {item.goal: item.title for item in preview.items}
    assert {command.title for command in created} == set(preview_by_goal.values())
    for command in created:
        assert command.title == preview_by_goal[command.goal]


def test_split_preview_prompt_requires_concrete_job_goals(tmp_path):
    prompt = _split_preview_prompt(
        "Rename CLI flags and update the README usage examples",
        str(tmp_path),
        None,
        locale="ja",
    )

    assert "Write each goal as a self-contained job brief" in prompt
    assert "Avoid vague titles or goals such as 'prepare the approach'" in prompt
    assert "実装を進める" in prompt
    assert "For planning jobs, state what plan or specification should be produced." in prompt
    assert "Update the README and CLI help text to match the new flag names." in prompt
    assert "Write the summary, titles, goals, and rationale in Japanese." in prompt


def test_codex_schema_preparer_makes_object_schemas_strict():
    prepared = structured_runtime_adapter(RuntimeBackend.CODEX).schema_preparer(
        PlannerOutput.model_json_schema()
    )

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object" or "properties" in node:
                properties = node.get("properties", {})
                assert node.get("additionalProperties") is False
                assert node.get("required") == list(properties.keys())
                assert "default" not in node
                assert "title" not in node
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    walk(prepared)

    input_payload = prepared["$defs"]["PlannerTaskSpec"]["properties"]["input_payload"]
    assert "title" in prepared["$defs"]["PlannerTaskSpec"]["properties"]
    assert input_payload["type"] == "object"
    assert input_payload["additionalProperties"] is False
    assert input_payload["properties"] == {}


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

        status, payload, headers = request_json_with_headers(f"{base_url}/api/snapshot")
        assert status == 200
        assert payload["commands"]["items"] == []
        assert headers["Cache-Control"] == "no-store"

        status, payload = request_json(f"{base_url}/missing")
        assert status == 404
        assert payload["error"] == "not_found"

        status, payload, headers = request_json_with_headers(
            f"{base_url}/api/commands",
            method="POST",
            body=b"{broken",
            headers={"Content-Type": "application/json"},
        )
        assert status == 400
        assert payload["error"] == "invalid_json"
        assert headers["Cache-Control"] == "no-store"

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

        created_status, created_payload, created_headers = request_json_with_headers(
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
        )
        assert created_status == 201
        assert created_headers["Cache-Control"] == "no-store"
        created = created_payload["command"]

        detail_status, detail_payload, detail_headers = request_json_with_headers(
            f"{base_url}/api/commands/{created['id']}/detail"
        )
        assert detail_status == 200
        assert detail_payload["command"]["id"] == created["id"]
        assert detail_headers["Cache-Control"] == "no-store"

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

    monkeypatch.setattr(dashboard_module, "advance_frontier_once", explode)
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
    codex_payload = codex_backend._run_structured("hello", {"type": "object"}, str(tmp_path))
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
    claude_payload = claude_backend._run_structured("review", {"type": "object"}, str(tmp_path))
    assert claude_payload["decision"] == "approve"
    claude_command = codex_runs[1][0]
    assert "--dangerously-skip-permissions" in claude_command

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (1, "", "boom"),
    )
    with pytest.raises(RuntimeError, match="boom"):
        codex_backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    with pytest.raises(RuntimeError, match="boom"):
        claude_backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    with pytest.raises(RuntimeError, match="Unsupported backend"):
        StructuredCliBackend(
            RuntimeBackend.INHERIT,
            model="",
            approval_mode=ApprovalMode.MANUAL,
            timeout_seconds=30,
        )._run_structured("prompt", {}, str(tmp_path))


def test_structured_cli_backends_classify_recoverable_operator_issues(tmp_path, monkeypatch):
    backend = StructuredCliBackend(
        RuntimeBackend.CODEX,
        model="gpt-test",
        approval_mode=ApprovalMode.AUTO,
        timeout_seconds=30,
        runtime_home=tmp_path / "runtime-home",
    )

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (1, "", "429 rate limit exceeded"),
    )
    with pytest.raises(AgentExecutionError) as provider_limit:
        backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    assert provider_limit.value.kind == OperatorIssueKind.PROVIDER_LIMIT

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (1, "", "login required before continuing"),
    )
    with pytest.raises(AgentExecutionError) as auth_required:
        backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    assert auth_required.value.kind == OperatorIssueKind.AUTH_REQUIRED

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (1, "", "confirm in the terminal to continue"),
    )
    with pytest.raises(AgentExecutionError) as interactive_prompt:
        backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    assert interactive_prompt.value.kind == OperatorIssueKind.INTERACTIVE_PROMPT

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (
            1,
            "",
            "Workspace command execution and file mutation are blocked by a sandbox/tooling failure (`bwrap: Unknown option --argv0`).",
        ),
    )
    with pytest.raises(AgentExecutionError) as runtime_environment:
        backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    assert runtime_environment.value.kind == OperatorIssueKind.RUNTIME_ENVIRONMENT

    monkeypatch.setattr(
        StructuredCliBackend,
        "_run_streaming_process",
        lambda self, *args, **kwargs: (_ for _ in ()).throw(
            subprocess.TimeoutExpired(cmd=["codex"], timeout=30)
        ),
    )
    with pytest.raises(AgentExecutionError) as runtime_timeout:
        backend._run_structured("broken", {"type": "object"}, str(tmp_path))
    assert runtime_timeout.value.kind == OperatorIssueKind.RUNTIME_TIMEOUT


def test_run_logged_subprocess_creates_missing_workspace_root(tmp_path):
    missing_root = tmp_path / "nested" / "workspace"

    returncode, stdout_text, stderr_text = service_module.run_logged_subprocess(
        ["/bin/pwd"],
        missing_root,
        dict(service_module.os.environ),
        timeout_seconds=5,
    )

    assert missing_root.is_dir()
    assert returncode == 0
    assert stdout_text.strip() == str(missing_root)
    assert stderr_text == ""


def test_planner_operator_issue_can_retry_with_different_backend(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="planner operator issue",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.PLANNING.value, command.id),
        )
        conn.commit()

    class FlakyPlanner:
        def __init__(self):
            self.calls = 0

        def plan(self, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise AgentExecutionError(
                    OperatorIssueKind.PROVIDER_LIMIT,
                    "provider limit",
                    detail="429 rate limit exceeded",
                )
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.PLANNING,
                final_response=(
                    "## Plan\nContinue planning.\n\n"
                    "## Design Direction\nKeep the current direction.\n\n"
                    "## Task Breakdown\n1. Finish the plan."
                ),
            )

    backend = FlakyPlanner()
    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: backend)

    blocked = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert blocked.action == "operator_attention_required"
    assert blocked.command.stage == CommandStage.WAITING_OPERATOR
    assert blocked.command.operator_issue_kind == OperatorIssueKind.PROVIDER_LIMIT

    resumed = retry_operator_issue(
        settings.db_path, command.id, backend_override=RuntimeBackend.CLAUDE
    )
    assert resumed.stage == CommandStage.PLANNING
    assert resumed.backend == RuntimeBackend.CLAUDE
    assert resumed.resume_hint is not None
    assert "provider usage limit" in resumed.resume_hint

    completed = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert completed.action == "command_completed"
    assert completed.command.stage == CommandStage.DONE


def test_task_operator_issue_requeues_blocked_task_on_retry(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="task operator issue",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.IMPLEMENTATION.value, command.id),
        )
        conn.commit()

    class TaskIssueBackend:
        def __init__(self):
            self.implementer_calls = 0

        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=WorkflowMode.IMPLEMENTATION,
                tasks=[
                    PlannerTaskSpec(
                        key="implement_main",
                        kind="implementation",
                        capability="implementer",
                        title="Implement the main change",
                        description="Edit the workspace to apply the requested change.",
                        depends_on=[],
                        write_files=["src/app.ts"],
                    )
                ],
            )

        def execute_task(self, command, task, *args, **kwargs):
            if task.capability == "implementer":
                self.implementer_calls += 1
                if self.implementer_calls == 1:
                    raise AgentExecutionError(
                        OperatorIssueKind.PROVIDER_LIMIT,
                        "provider limit",
                        detail="usage limit exceeded",
                    )
            return WorkerOutput(
                decision=WorkerDecision.COMPLETE,
                summary=f"{task.capability} finished",
                output_payload={"summary": f"{task.capability} finished"},
            )

        def review(self, *args, **kwargs):
            return ReviewerOutput(
                decision=ReviewDecision.APPROVE,
                summary="review ok",
                findings=[],
            )

    backend = TaskIssueBackend()
    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: backend)

    planned = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert planned.action == "tasks_planned"

    interrupted = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert interrupted.action == "operator_attention_required"
    assert interrupted.command.stage == CommandStage.WAITING_OPERATOR
    tasks = list_tasks(settings.db_path, command_id=command.id)
    blocked_tasks = [
        task for task in tasks if task.operator_issue_kind == OperatorIssueKind.PROVIDER_LIMIT
    ]
    assert blocked_tasks
    assert blocked_tasks[0].state == TaskState.BLOCKED

    resumed = retry_operator_issue(settings.db_path, command.id)
    assert resumed.stage == CommandStage.RUNNING

    resumed_tick = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert resumed_tick.command.stage in {
        CommandStage.RUNNING,
        CommandStage.VERIFYING,
        CommandStage.DONE,
    }
    tasks_after_resume = list_tasks(settings.db_path, command_id=command.id)
    assert any(
        task.state == TaskState.DONE
        for task in tasks_after_resume
        if task.capability == "implementer"
    )


def test_task_structured_failure_reason_becomes_operator_issue(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="repair operator issue",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.IMPLEMENTATION.value, command.id),
        )
        conn.commit()

    class EnvironmentFailureBackend:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=WorkflowMode.IMPLEMENTATION,
                tasks=[
                    PlannerTaskSpec(
                        key="repair_main",
                        kind="implementation",
                        capability="implementer",
                        title="Repair the partial edit",
                        description="Clean up the interrupted README edit.",
                        depends_on=[],
                        write_files=["README.md"],
                    )
                ],
            )

        def execute_task(self, *args, **kwargs):
            return WorkerOutput(
                decision=WorkerDecision.FAIL,
                failure_reason=(
                    "Workspace command execution and file mutation are blocked by a sandbox/tooling "
                    "failure (`bwrap: Unknown option --argv0`)."
                ),
            )

        def review(self, *args, **kwargs):
            return ReviewerOutput(
                decision=ReviewDecision.APPROVE,
                summary="ok",
                findings=[],
            )

    backend = EnvironmentFailureBackend()
    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: backend)

    planned = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert planned.action == "tasks_planned"

    interrupted = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert interrupted.action == "operator_attention_required"
    assert interrupted.command.stage == CommandStage.WAITING_OPERATOR
    assert interrupted.command.operator_issue_kind == OperatorIssueKind.RUNTIME_ENVIRONMENT

    tasks = list_tasks(settings.db_path, command_id=command.id)
    assert tasks[0].operator_issue_kind == OperatorIssueKind.RUNTIME_ENVIRONMENT
    assert tasks[0].state == TaskState.BLOCKED


def test_planner_structured_failure_reason_becomes_operator_issue(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="planner environment issue",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.PLANNING.value, WorkflowMode.PLANNING.value, command.id),
        )
        conn.commit()

    class PlannerFailureBackend:
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.FAIL,
                workflow_mode=WorkflowMode.PLANNING,
                failure_reason=(
                    "Workspace command execution and file mutation are blocked by a sandbox/tooling "
                    "failure (`bwrap: Unknown option --argv0`)."
                ),
            )

    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: PlannerFailureBackend()
    )

    outcome = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert outcome.action == "operator_attention_required"
    assert outcome.command.stage == CommandStage.WAITING_OPERATOR
    assert outcome.command.operator_issue_kind == OperatorIssueKind.RUNTIME_ENVIRONMENT


def test_review_structured_failure_reason_becomes_operator_issue(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="review environment issue",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.VERIFYING.value, WorkflowMode.REVIEW.value, command.id),
        )
        conn.commit()

    class ReviewFailureBackend:
        def review(self, *args, **kwargs):
            return ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary="review blocked",
                failure_reason=(
                    "Workspace command execution and file mutation are blocked by a sandbox/tooling "
                    "failure (`bwrap: Unknown option --argv0`)."
                ),
                findings=[],
            )

    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: ReviewFailureBackend()
    )

    outcome = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert outcome.action == "operator_attention_required"
    assert outcome.command.stage == CommandStage.WAITING_OPERATOR
    assert outcome.command.operator_issue_kind == OperatorIssueKind.RUNTIME_ENVIRONMENT


def test_tick_reconciles_interrupted_running_agent_run_into_operator_issue(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="reconcile interrupted run",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )
    now = service_module.utc_now()
    with connect(settings.db_path) as conn:
        conn.execute(
            """
            INSERT INTO tasks (
                id, command_id, task_key, kind, capability, title, description, state, plan_order,
                input_payload, output_payload, error, operator_issue_kind, attempt_count, assigned_run_id,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "task_running_interrupt",
                command.id,
                "interrupt_task",
                "code_change",
                "implementer",
                "Interrupted task",
                "editing ui",
                TaskState.RUNNING.value,
                1,
                json.dumps({"write_files": ["src/wevra/static/index.html"]}),
                None,
                None,
                None,
                1,
                "agentrun_interrupt",
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO agent_runs (
                id, command_id, task_id, reviewer_slot, role_name, capability, runtime, model,
                run_kind, title, resume_stage, state, approval_required, prompt_excerpt,
                output_summary, output_log, error, process_id, created_at, started_at, finished_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "agentrun_interrupt",
                command.id,
                "task_running_interrupt",
                None,
                "implementer",
                "implementer",
                RuntimeBackend.CODEX.value,
                "gpt-test",
                "task",
                "Interrupted task",
                CommandStage.RUNNING.value,
                "running",
                0,
                "interrupted prompt",
                None,
                "",
                None,
                999999,
                now,
                now,
                None,
                now,
            ),
        )
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ?, updated_at = ? WHERE id = ?",
            (CommandStage.RUNNING.value, WorkflowMode.IMPLEMENTATION.value, now, command.id),
        )
        conn.commit()

    monkeypatch.setattr(service_module, "process_is_alive", lambda _pid: False)
    outcome = tick_once(
        settings.db_path, command_id=command.id, settings=settings, repo_root=tmp_path
    )
    assert outcome.action == "operator_attention_required"
    assert outcome.command.stage == CommandStage.WAITING_OPERATOR
    assert outcome.command.operator_issue_kind == OperatorIssueKind.RUNTIME_INTERRUPTED

    tasks = list_tasks(settings.db_path, command_id=command.id)
    assert tasks[0].state == TaskState.BLOCKED
    assert tasks[0].operator_issue_kind == OperatorIssueKind.RUNTIME_INTERRUPTED


def test_cancel_command_terminates_running_agent_processes(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="cancel kills process group",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.HIGH,
    )
    now = service_module.utc_now()
    with connect(settings.db_path) as conn:
        conn.execute(
            """
            INSERT INTO agent_runs (
                id, command_id, task_id, reviewer_slot, role_name, capability, runtime, model,
                run_kind, title, resume_stage, state, approval_required, prompt_excerpt,
                output_summary, output_log, error, process_id, created_at, started_at, finished_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "agentrun_cancel",
                command.id,
                None,
                None,
                "planner",
                "planner",
                RuntimeBackend.CODEX.value,
                "gpt-test",
                "planner",
                "Planner",
                CommandStage.PLANNING.value,
                "running",
                0,
                "planner prompt",
                None,
                "",
                None,
                424242,
                now,
                now,
                None,
                now,
            ),
        )
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.RUNNING.value, now, command.id),
        )
        conn.commit()

    seen: list[int] = []
    monkeypatch.setattr(service_module, "terminate_process_group", lambda pid: seen.append(pid))
    canceled = cancel_command(settings.db_path, command.id, reason="Stop now.")
    assert canceled.stage == CommandStage.CANCELED
    assert seen == [424242]


def test_cancel_with_repair_creates_high_priority_repair_job(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="repair me",
        workflow_mode=WorkflowMode.IMPLEMENTATION,
        priority=Priority.MEDIUM,
        backend=RuntimeBackend.CODEX,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, operator_issue_kind = ?, operator_issue_detail = ? WHERE id = ?",
            (
                CommandStage.WAITING_OPERATOR.value,
                OperatorIssueKind.PROVIDER_LIMIT.value,
                "usage limit exceeded",
                command.id,
            ),
        )
        conn.commit()

    canceled, repair = cancel_command_with_repair(
        settings.db_path,
        command.id,
        repair_goal="中断したジョブの変更を元に戻す: repair me",
        settings=settings,
        repo_root=tmp_path,
    )
    assert canceled.stage == CommandStage.CANCELED
    assert repair.priority == Priority.HIGH
    assert repair.workflow_mode == WorkflowMode.IMPLEMENTATION
    assert repair.workspace_root == canceled.workspace_root
    assert repair.goal.startswith("中断したジョブの変更を元に戻す")


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
    assert mode_requires_review(WorkflowMode.DOGFOODING) is True
    assert mode_requires_test(WorkflowMode.RESEARCH) is False
    assert mode_requires_test(WorkflowMode.DOGFOODING) is True
    assert infer_mode_from_goal("Please investigate and report") == WorkflowMode.RESEARCH
    assert infer_mode_from_goal("Please review this diff") == WorkflowMode.REVIEW
    assert infer_mode_from_goal("Please design a rollout plan") == WorkflowMode.PLANNING
    assert infer_mode_from_goal("Dogfood the release runbook") == WorkflowMode.DOGFOODING
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
    dogfooding_guidance = mode_prompt_guidance(WorkflowMode.DOGFOODING)
    assert "dogfooding mode" in dogfooding_guidance
    assert "runbook path" in dogfooding_guidance

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
    dogfood_specs = add_mode_control_tasks(
        WorkflowMode.DOGFOODING,
        specs[:-1],
        runbook_path="/tmp/RUNBOOK.md",
    )
    assert dogfood_specs[-1].input_payload["gate"] == "dogfooding_runbook"
    assert dogfood_specs[-1].input_payload["runbook_path"] == "/tmp/RUNBOOK.md"
    assert dogfood_specs[-1].kind == "dogfooding"

    context = build_context_payload(command, [task], [], [], [], task=task)
    assert context["command"]["id"] == "cmd_helpers"
    assert context["task"]["id"] == "task_helpers"
    review_context = build_review_context_payload(command, [task], [], [], [])
    assert review_context["command"]["id"] == "cmd_helpers"
    assert review_context["completed_tasks"][0]["id"] == "task_helpers"
    assert review_context["write_targets"] == []
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


def test_completed_dependency_becomes_actionable_for_followup_job(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    first = submit_direct(
        tmp_path / "workspace-a",
        settings,
        goal="first done job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.HIGH,
    )
    second = submit_command(
        settings.db_path,
        goal="follow-up job",
        workflow_mode=WorkflowMode.RESEARCH,
        priority=Priority.MEDIUM,
        workspace_root=tmp_path / "workspace-b",
        depends_on_command_ids=[first.id],
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.DONE.value, service_module.utc_now(), first.id),
        )
        conn.commit()
        selected = select_actionable_command(conn, None)

    stored = service_module.get_command(settings.db_path, second.id)
    assert stored is not None
    assert stored.dependency_state == "ready"
    assert selected is not None
    assert selected.id == second.id


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


def test_select_actionable_commands_returns_parallel_frontier(tmp_path):
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

    with connect(settings.db_path) as conn:
        actionable = select_actionable_commands(conn, None)

    assert [command.id for command in actionable] == [first.id, second.id]


def test_advance_frontier_once_runs_parallel_planners_concurrently(tmp_path, monkeypatch):
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
    with connect(settings.db_path) as conn:
        now = service_module.utc_now()
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ?, updated_at = ? WHERE id IN (?, ?)",
            (
                CommandStage.PLANNING.value,
                WorkflowMode.PLANNING.value,
                now,
                first.id,
                second.id,
            ),
        )
        conn.commit()

    class ParallelPlanner(BackendInterface):
        def __init__(self):
            self.barrier = threading.Barrier(2)
            self.lock = threading.Lock()
            self.active = 0
            self.max_active = 0

        def plan(self, command, *args, **kwargs):
            with self.lock:
                self.active += 1
                self.max_active = max(self.max_active, self.active)
            try:
                self.barrier.wait(timeout=3)
                time.sleep(0.05)
                return PlannerOutput(
                    decision=PlannerDecision.COMPLETE,
                    workflow_mode=WorkflowMode.PLANNING,
                    final_response=(
                        "## Plan\n- Complete the requested planning pass.\n\n"
                        "## Design Direction\n- Keep the current direction.\n\n"
                        "## Task Breakdown\n1. Finish the plan."
                    ),
                )
            finally:
                with self.lock:
                    self.active -= 1

    backend = ParallelPlanner()
    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: backend)

    outcomes = advance_frontier_once(
        settings.db_path,
        settings=settings,
        repo_root=tmp_path,
    )

    assert backend.max_active == 2
    assert {outcome.command.id for outcome in outcomes if outcome.command} == {first.id, second.id}
    assert {outcome.action for outcome in outcomes} == {"command_completed"}

    with connect(settings.db_path) as conn:
        commands = {
            row["id"]: row["stage"]
            for row in conn.execute(
                "SELECT id, stage FROM commands WHERE id IN (?, ?)",
                (first.id, second.id),
            ).fetchall()
        }
    assert commands == {
        first.id: CommandStage.DONE.value,
        second.id: CommandStage.DONE.value,
    }


def test_run_engine_completes_parallel_jobs_in_one_invocation(tmp_path, monkeypatch):
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

    class CompletePlanner(BackendInterface):
        def plan(self, *args, **kwargs):
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=WorkflowMode.PLANNING,
                final_response=(
                    "## Plan\n- Complete the requested planning pass.\n\n"
                    "## Design Direction\n- Keep the current direction.\n\n"
                    "## Task Breakdown\n1. Finish the plan."
                ),
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: CompletePlanner())

    result = service_module.run_engine(
        settings.db_path,
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        commands = {
            row["id"]: row["stage"]
            for row in conn.execute(
                "SELECT id, stage FROM commands WHERE id IN (?, ?)",
                (first.id, second.id),
            ).fetchall()
        }
    assert commands == {
        first.id: CommandStage.DONE.value,
        second.id: CommandStage.DONE.value,
    }
    assert len(result["steps"]) >= 4


def test_submit_command_rejects_parallel_overlap_with_active_job(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    submit_command(
        settings.db_path,
        goal="active first",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-a",
        settings=settings,
        repo_root=tmp_path,
    )

    with pytest.raises(ValueError, match="parallel_workspace_overlap"):
        submit_command(
            settings.db_path,
            goal="parallel overlap",
            workflow_mode=WorkflowMode.PLANNING,
            priority=Priority.HIGH,
            workspace_root=tmp_path / "workspace-a" / "child",
            allow_parallel=True,
            settings=settings,
            repo_root=tmp_path,
        )


def test_submit_command_allows_parallel_when_overlapping_job_is_done(tmp_path):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    first = submit_command(
        settings.db_path,
        goal="finished first",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-a",
        settings=settings,
        repo_root=tmp_path,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.DONE.value, service_module.utc_now(), first.id),
        )
        conn.commit()

    second = submit_command(
        settings.db_path,
        goal="parallel after done",
        workflow_mode=WorkflowMode.PLANNING,
        priority=Priority.HIGH,
        workspace_root=tmp_path / "workspace-a" / "child",
        allow_parallel=True,
        settings=settings,
        repo_root=tmp_path,
    )

    assert second.allow_parallel is True


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
    artifacts = [
        artifact
        for artifact in list_artifacts(settings.db_path, command_id=planning_command.id)
        if artifact.kind == "result_markdown"
    ]
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


def test_reduce_verifying_returns_canceled_when_job_is_canceled_mid_review(tmp_path, monkeypatch):
    init_repo_config(tmp_path)
    settings = load_config(tmp_path)
    initialize_database(settings.db_path)

    command = submit_direct(
        tmp_path,
        settings,
        goal="cancel during review",
        workflow_mode=WorkflowMode.REVIEW,
        priority=Priority.HIGH,
    )
    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, effective_mode = ? WHERE id = ?",
            (CommandStage.VERIFYING.value, WorkflowMode.REVIEW.value, command.id),
        )
        conn.commit()

    def fake_run_review_batch(*args, **kwargs):
        cancel_command(
            settings.db_path,
            command.id,
            "Operator canceled the job during a final review pass.",
        )
        return [(1, None, None)]

    monkeypatch.setattr(service_module, "run_review_batch", fake_run_review_batch)

    outcome = tick_once(
        settings.db_path,
        command_id=command.id,
        settings=settings,
        repo_root=tmp_path,
    )
    assert outcome.action == "command_canceled"
    assert outcome.command.stage == CommandStage.CANCELED
    assert "final review" in (outcome.note or "")
