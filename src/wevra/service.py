from __future__ import annotations

import importlib.util
import json
import os
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from wevra.config import AppConfig, load_config
from wevra.db import connect, initialize_database
from wevra.models import (
    ApprovalMode,
    AgentRunKind,
    AgentRunRecord,
    AgentRunState,
    ArtifactRecord,
    CommandRecord,
    CommandStage,
    EventRecord,
    InstructionRecord,
    JobTitleOutput,
    JobSplitDraftItem,
    JobSplitDraftOutput,
    JobSplitPreview,
    JobSplitPreviewItem,
    OperatorIssueKind,
    PlannerDecision,
    PlannerOutput,
    PlannerTaskSpec,
    Priority,
    QuestionRecord,
    QuestionResolutionMode,
    QuestionState,
    ReviewDecision,
    ReviewRecord,
    ReviewerOutput,
    RuntimeBackend,
    TaskRecord,
    TaskState,
    TickOutcome,
    WorkerDecision,
    WorkerOutput,
    WorkflowMode,
)
from wevra.runtime_registry import structured_runtime_adapter, runtime_label


PRIORITY_RANK = {
    Priority.HIGH.value: 0,
    Priority.MEDIUM.value: 1,
    Priority.LOW.value: 2,
}

STAGE_RANK = {
    CommandStage.PLANNING.value: 0,
    CommandStage.REPLANNING.value: 1,
    CommandStage.RUNNING.value: 2,
    CommandStage.VERIFYING.value: 3,
    CommandStage.WAITING_APPROVAL.value: 4,
    CommandStage.WAITING_OPERATOR.value: 5,
    CommandStage.WAITING_QUESTION.value: 6,
    CommandStage.PAUSED.value: 7,
    CommandStage.CANCELED.value: 8,
    CommandStage.QUEUED.value: 9,
    CommandStage.DONE.value: 10,
    CommandStage.FAILED.value: 11,
}

TERMINAL_COMMAND_STAGES = {
    CommandStage.DONE.value,
    CommandStage.FAILED.value,
    CommandStage.PAUSED.value,
    CommandStage.CANCELED.value,
}

PARALLEL_STAGE_RANK = {
    CommandStage.QUEUED.value: 0,
    CommandStage.PLANNING.value: 1,
    CommandStage.REPLANNING.value: 2,
    CommandStage.RUNNING.value: 3,
    CommandStage.VERIFYING.value: 4,
    CommandStage.WAITING_APPROVAL.value: 5,
    CommandStage.WAITING_OPERATOR.value: 6,
    CommandStage.WAITING_QUESTION.value: 7,
    CommandStage.PAUSED.value: 8,
    CommandStage.CANCELED.value: 9,
    CommandStage.DONE.value: 10,
    CommandStage.FAILED.value: 11,
}

PROVIDER_LIMIT_PATTERNS = (
    "rate limit",
    "usage limit",
    "quota",
    "too many requests",
    "429",
    "insufficient credits",
    "credit balance",
    "usage cap",
    "limit reached",
)

AUTH_REQUIRED_PATTERNS = (
    "login required",
    "log in",
    "logged in",
    "sign in",
    "authentication",
    "unauthorized",
    "not authorized",
    "api key",
    "auth token",
)

INTERACTIVE_PROMPT_PATTERNS = (
    "yes/no",
    "confirm",
    "requires tty",
    "waiting for input",
    "interactive",
    "prompt",
    "update available",
)

RUNTIME_INTERRUPTED_DETAIL = "The AI process ended before Wevra received a final structured result."

MODE_KEYWORDS = {
    WorkflowMode.RESEARCH: (
        "research",
        "investigate",
        "analysis",
        "report",
        "調査",
        "分析",
        "報告",
    ),
    WorkflowMode.REVIEW: ("review", "audit", "inspect", "pr", "diff", "レビュー", "確認"),
    WorkflowMode.DOGFOODING: (
        "dogfood",
        "dogfooding",
        "runbook",
        "manual walkthrough",
        "manual test",
        "acceptance",
        "運用手順",
        "ドッグフーディング",
        "手順確認",
    ),
    WorkflowMode.PLANNING: ("plan", "design", "spec", "task breakdown", "設計", "計画", "分解"),
}

RESULT_ARTIFACT_KIND = "result_markdown"
RESULT_SECTION_TITLES = {
    "result": {"en": "Result", "ja": "結果"},
    "plan": {"en": "Plan", "ja": "計画"},
    "design_direction": {"en": "Design Direction", "ja": "設計方針"},
    "task_breakdown": {"en": "Task Breakdown", "ja": "タスク分解"},
}
PLANNING_SECTION_ALIASES = {
    "plan": {"plan", "overview", "summary", "計画", "概要"},
    "design_direction": {"design direction", "design", "approach", "設計方針", "設計"},
    "task_breakdown": {"task breakdown", "breakdown", "tasks", "タスク分解", "タスク"},
}

EXPLICIT_PATH_PATTERN = re.compile(r"(?<![\w./-])((?:[A-Za-z0-9_.-]+/)+[A-Za-z0-9_.-]+)")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def resolve_settings(
    settings: Optional[AppConfig] = None, repo_root: Optional[Path] = None
) -> AppConfig:
    if settings is not None:
        return settings
    return load_config((repo_root or Path.cwd()).resolve())


def command_order_key(command: CommandRecord) -> tuple[int, str]:
    return (PRIORITY_RANK[command.priority.value], command.created_at)


def normalized_workspace_root(value: str | Path) -> str:
    return str(Path(value).expanduser().resolve())


def normalized_runbook_path(value: str | Path, workspace_root: str | Path) -> str:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path(workspace_root).expanduser().resolve() / path
    return str(path.resolve())


def workspace_roots_overlap(left: str | Path, right: str | Path) -> bool:
    left_root = normalized_workspace_root(left)
    right_root = normalized_workspace_root(right)
    try:
        common = os.path.commonpath([left_root, right_root])
    except ValueError:
        return False
    return common == left_root or common == right_root


def resolve_workspace_path(base_root: str | Path, workspace_path: str | Path) -> str:
    raw = str(workspace_path or ".").strip() or "."
    path = Path(raw).expanduser()
    if not path.is_absolute():
        path = Path(base_root).expanduser().resolve() / path
    return str(path.resolve())


def normalize_job_split_preview(
    draft: JobSplitDraftOutput, base_workspace_root: str | Path
) -> JobSplitPreview:
    seen: set[str] = set()
    items: List[JobSplitPreviewItem] = []
    for index, item in enumerate(draft.items, start=1):
        key = (item.key or "").strip() or f"job_{index}"
        if key in seen:
            raise ValueError(f"Duplicate split preview key: {key}")
        seen.add(key)
        items.append(
            JobSplitPreviewItem(
                key=key,
                title=(item.title or "").strip() or f"Job {index}",
                goal=(item.goal or "").strip() or (item.title or "").strip() or f"Job {index}",
                workflow_mode=item.workflow_mode,
                workspace_root=resolve_workspace_path(base_workspace_root, item.workspace_path),
                depends_on=[dependency for dependency in item.depends_on if dependency],
                allow_parallel=bool(item.allow_parallel),
                runbook_path=item.runbook_path,
                rationale=item.rationale,
            )
        )
    preview = JobSplitPreview(summary=(draft.summary or "").strip() or None, items=items)
    preview_keys = {item.key for item in preview.items}
    for item in preview.items:
        missing = [dependency for dependency in item.depends_on if dependency not in preview_keys]
        if missing:
            raise ValueError(f"Unknown split preview dependency: {', '.join(missing)}")
        if item.key in item.depends_on:
            raise ValueError(f"Split preview item cannot depend on itself: {item.key}")
    for item in preview.items:
        if item.depends_on:
            item.allow_parallel = False
            continue
        if any(
            other.key != item.key
            and workspace_roots_overlap(item.workspace_root, other.workspace_root)
            for other in preview.items
        ):
            item.allow_parallel = False
    return preview


def requested_mode(command: CommandRecord) -> WorkflowMode:
    return command.workflow_mode


def effective_mode(command: CommandRecord) -> WorkflowMode:
    if command.effective_mode:
        return command.effective_mode
    if command.workflow_mode == WorkflowMode.AUTO:
        return WorkflowMode.IMPLEMENTATION
    return command.workflow_mode


def mode_requires_review(mode: WorkflowMode) -> bool:
    return mode in {WorkflowMode.IMPLEMENTATION, WorkflowMode.REVIEW, WorkflowMode.DOGFOODING}


def mode_requires_test(mode: WorkflowMode) -> bool:
    return mode in {WorkflowMode.IMPLEMENTATION, WorkflowMode.DOGFOODING}


def infer_mode_from_goal(goal: str) -> WorkflowMode:
    lowered = goal.lower()
    for mode, keywords in MODE_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return mode
    return WorkflowMode.IMPLEMENTATION


def infer_mode_from_specs(specs: Sequence[PlannerTaskSpec], goal: str) -> WorkflowMode:
    capabilities = {spec.capability for spec in specs}
    if (
        "implementer" in capabilities
        or "implementation" in capabilities
        or "rework" in capabilities
    ):
        return WorkflowMode.IMPLEMENTATION
    if "tester" in capabilities:
        return WorkflowMode.IMPLEMENTATION
    if capabilities & {"investigation", "analyst"}:
        return infer_mode_from_goal(goal)
    return infer_mode_from_goal(goal)


def resolve_command_mode(command: CommandRecord, output: PlannerOutput) -> WorkflowMode:
    if command.workflow_mode != WorkflowMode.AUTO:
        return command.workflow_mode
    if output.workflow_mode and output.workflow_mode != WorkflowMode.AUTO:
        return output.workflow_mode
    return infer_mode_from_specs(output.tasks, command.goal)


def mode_prompt_guidance(mode: WorkflowMode) -> str:
    if mode == WorkflowMode.IMPLEMENTATION:
        return (
            "This is implementation mode. You may schedule investigation and analyst tasks before implementation, "
            "but do not schedule final tester or reviewer tasks yourself. The engine owns the final test gate and final review gate."
        )
    if mode == WorkflowMode.RESEARCH:
        return (
            "This is research mode. Focus on investigation and analyst tasks that produce a report or conclusion. "
            "Do not assume implementation, testing, or final review are required."
        )
    if mode == WorkflowMode.REVIEW:
        return (
            "This is review mode. Focus on tasks that gather context for review. "
            "The engine owns the final reviewer pass."
        )
    if mode == WorkflowMode.PLANNING:
        return (
            "This is planning mode. Focus on design, decomposition, or spec work. "
            "Do not assume implementation, testing, or final review are required. "
            "When you return COMPLETE, write final_response as markdown with exactly three top-level sections, "
            "in this order: '## Plan', '## Design Direction', and '## Task Breakdown'. "
            "Do not write any prose before the first heading or add extra top-level sections. "
            "Under 'Task Breakdown', use an ordered list with actionable steps."
        )
    if mode == WorkflowMode.DOGFOODING:
        return (
            "This is dogfooding mode. Read the runbook path first, then plan around exercising the real workflow in the target workspace. "
            "Focus on friction, breakage, and fixes needed to make the documented flow succeed. "
            "Do not schedule the final dogfooding validation pass or final reviewer pass yourself. The engine owns both. "
            "Use investigation or analyst tasks only when they directly help understand the runbook or reproduce an issue."
        )
    return (
        "This is auto mode. Infer whether the request is best handled as implementation, research, review, planning, or dogfooding, "
        "and set workflow_mode in the JSON response."
    )


def add_mode_control_tasks(
    mode: WorkflowMode, specs: Sequence[PlannerTaskSpec], *, runbook_path: Optional[str] = None
) -> List[PlannerTaskSpec]:
    planned = list(specs)
    if not mode_requires_test(mode):
        return planned
    if any(spec.capability == "tester" for spec in planned):
        return planned

    dependency_keys = [spec.key for spec in planned]
    if mode == WorkflowMode.DOGFOODING:
        planned.append(build_dogfooding_validation_spec(dependency_keys, runbook_path))
    else:
        planned.append(build_final_test_spec(dependency_keys))
    return planned


def build_final_test_spec(dependency_keys: Sequence[str]) -> PlannerTaskSpec:
    return PlannerTaskSpec(
        key="__system_test_gate__",
        kind="test",
        capability="tester",
        title="Run existing feature and unit tests",
        description="Execute the existing test suite against the completed implementation before the final review pass.",
        depends_on=list(dependency_keys),
        write_files=[],
        input_payload={"system_generated": True, "gate": "final_test"},
    )


def build_dogfooding_validation_spec(
    dependency_keys: Sequence[str], runbook_path: Optional[str]
) -> PlannerTaskSpec:
    runbook_detail = runbook_path or "the configured runbook path"
    return PlannerTaskSpec(
        key="__system_dogfooding_gate__",
        kind="dogfooding",
        capability="tester",
        title="Execute the documented workflow",
        description=(
            "Read the operating runbook and execute the documented workflow against the target workspace. "
            "Confirm whether the flow actually works, capture friction clearly, and fail if the workflow is still broken.\n\n"
            f"Runbook path: {runbook_detail}"
        ),
        depends_on=list(dependency_keys),
        write_files=[],
        input_payload={
            "system_generated": True,
            "gate": "dogfooding_runbook",
            "runbook_path": runbook_path,
        },
    )


def is_deterministic_test_gate(task: TaskRecord) -> bool:
    return task.capability == "tester" and task.input_payload.get("gate") == "final_test"


def append_event(conn, stream_type: str, stream_id: str, event_type: str, payload: dict) -> None:
    now = utc_now()
    conn.execute(
        """
        INSERT INTO events (stream_type, stream_id, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (stream_type, stream_id, event_type, json.dumps(payload, sort_keys=True), now),
    )
    command_id = stream_id if stream_type == "command" else payload.get("command_id")
    if isinstance(command_id, str) and command_id:
        conn.execute("UPDATE commands SET updated_at = ? WHERE id = ?", (now, command_id))


def update_command(conn, command_id: str, **fields) -> None:
    fields["updated_at"] = utc_now()
    assignments = ["version = version + 1"] + [f"{key} = ?" for key in fields]
    values = list(fields.values()) + [command_id]
    conn.execute(
        f"UPDATE commands SET {', '.join(assignments)} WHERE id = ?",
        values,
    )


def update_task(conn, task_id: str, **fields) -> None:
    fields["updated_at"] = utc_now()
    assignments = [f"{key} = ?" for key in fields]
    values = list(fields.values()) + [task_id]
    conn.execute(
        f"UPDATE tasks SET {', '.join(assignments)} WHERE id = ?",
        values,
    )


def update_question(conn, question_id: str, **fields) -> None:
    fields["updated_at"] = utc_now()
    assignments = [f"{key} = ?" for key in fields]
    values = list(fields.values()) + [question_id]
    conn.execute(
        f"UPDATE questions SET {', '.join(assignments)} WHERE id = ?",
        values,
    )


def update_agent_run(conn, agent_run_id: str, **fields) -> None:
    fields["updated_at"] = utc_now()
    assignments = [f"{key} = ?" for key in fields]
    values = list(fields.values()) + [agent_run_id]
    conn.execute(
        f"UPDATE agent_runs SET {', '.join(assignments)} WHERE id = ?",
        values,
    )


def _merge_agent_log(existing: str, text: str) -> str:
    output_log = (existing or "") + text
    if len(output_log) > 120_000:
        output_log = output_log[-120_000:]
    return output_log


def append_agent_run_log_conn(conn, agent_run_id: str, text: str) -> None:
    if not text:
        return
    row = conn.execute(
        "SELECT output_log FROM agent_runs WHERE id = ?",
        (agent_run_id,),
    ).fetchone()
    if row is None:
        return
    update_agent_run(conn, agent_run_id, output_log=_merge_agent_log(row["output_log"] or "", text))


def append_agent_run_log(db_path: Path, agent_run_id: str, text: str) -> None:
    if not text:
        return
    initialize_database(db_path)
    with connect(db_path) as conn:
        append_agent_run_log_conn(conn, agent_run_id, text)
        conn.commit()


def format_agent_log_line(message: str, *, channel: Optional[str] = None) -> str:
    prefix = datetime.now(timezone.utc).astimezone().strftime("%H:%M:%S")
    rendered = message.rstrip("\n")
    if channel:
        return f"[{prefix}] {channel}: {rendered}\n"
    return f"[{prefix}] {rendered}\n"


def command_from_row(row) -> CommandRecord:
    payload = dict(row)
    payload["title"] = (payload.get("title") or payload.get("goal") or "").strip()
    payload["allow_parallel"] = bool(payload.get("allow_parallel", 0))
    payload["replan_requested"] = bool(payload.get("replan_requested", 0))
    payload["stop_requested"] = bool(payload.get("stop_requested", 0))
    payload.setdefault("depends_on", [])
    payload.setdefault("blocking_dependency_ids", [])
    payload.setdefault("dependency_state", "none")
    payload.setdefault("can_ignore_dependencies", False)
    return CommandRecord.model_validate(payload)


def task_from_row(row) -> TaskRecord:
    payload = dict(row)
    payload["input_payload"] = json.loads(payload["input_payload"] or "{}")
    payload["output_payload"] = (
        json.loads(payload["output_payload"]) if payload["output_payload"] else None
    )
    payload.setdefault("depends_on", [])
    return TaskRecord.model_validate(payload)


def question_from_row(row) -> QuestionRecord:
    return QuestionRecord.model_validate(dict(row))


def review_from_row(row) -> ReviewRecord:
    payload = dict(row)
    payload["findings"] = json.loads(payload.pop("findings_json") or "[]")
    return ReviewRecord.model_validate(payload)


def instruction_from_row(row) -> InstructionRecord:
    return InstructionRecord.model_validate(dict(row))


def artifact_from_row(row) -> ArtifactRecord:
    payload = dict(row)
    payload["metadata"] = json.loads(payload.pop("metadata_json") or "{}")
    return ArtifactRecord.model_validate(payload)


def agent_run_from_row(row) -> AgentRunRecord:
    payload = dict(row)
    payload["approval_required"] = bool(payload.get("approval_required", 0))
    return AgentRunRecord.model_validate(payload)


def event_from_row(row) -> EventRecord:
    return EventRecord.model_validate(
        {
            "id": row["id"],
            "stream_type": row["stream_type"],
            "stream_id": row["stream_id"],
            "event_type": row["event_type"],
            "payload": json.loads(row["payload_json"]),
            "created_at": row["created_at"],
        }
    )


def create_agent_run_record(
    conn,
    *,
    command_id: str,
    role_name: str,
    capability: str,
    runtime: RuntimeBackend,
    model: str,
    run_kind: AgentRunKind,
    title: str,
    resume_stage: CommandStage,
    state: AgentRunState,
    approval_required: bool,
    task_id: Optional[str] = None,
    reviewer_slot: Optional[int] = None,
    prompt_excerpt: Optional[str] = None,
) -> AgentRunRecord:
    run_id = new_id("agentrun")
    now = utc_now()
    initial_log = ""
    if prompt_excerpt:
        initial_log += format_agent_log_line(f"prompt: {prompt_excerpt}")
    if state == AgentRunState.PENDING_APPROVAL:
        initial_log += format_agent_log_line("waiting for operator approval")
    elif state == AgentRunState.RUNNING:
        initial_log += format_agent_log_line("run started")
    conn.execute(
        """
        INSERT INTO agent_runs (
            id, command_id, task_id, reviewer_slot, role_name, capability, runtime, model,
            run_kind, title, resume_stage, state, approval_required, prompt_excerpt,
            output_summary, output_log, error, process_id, created_at, started_at, finished_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            command_id,
            task_id,
            reviewer_slot,
            role_name,
            capability,
            runtime.value,
            model,
            run_kind.value,
            title,
            resume_stage.value,
            state.value,
            int(approval_required),
            prompt_excerpt,
            None,
            initial_log,
            None,
            None,
            now,
            now if state == AgentRunState.RUNNING else None,
            None,
            now,
        ),
    )
    append_event(
        conn,
        "agent_run",
        run_id,
        "agent_run_created",
        {
            "command_id": command_id,
            "task_id": task_id,
            "reviewer_slot": reviewer_slot,
            "role_name": role_name,
            "capability": capability,
            "runtime": runtime.value,
            "run_kind": run_kind.value,
            "resume_stage": resume_stage.value,
            "state": state.value,
        },
    )
    row = conn.execute("SELECT * FROM agent_runs WHERE id = ?", (run_id,)).fetchone()
    return agent_run_from_row(row)


def list_agent_runs_for_command(conn, command_id: str) -> List[AgentRunRecord]:
    rows = conn.execute(
        "SELECT * FROM agent_runs WHERE command_id = ? ORDER BY created_at ASC", (command_id,)
    ).fetchall()
    return [agent_run_from_row(row) for row in rows]


def list_artifacts_for_command(conn, command_id: str) -> List[ArtifactRecord]:
    rows = conn.execute(
        "SELECT * FROM artifacts WHERE command_id = ? ORDER BY created_at ASC",
        (command_id,),
    ).fetchall()
    return [artifact_from_row(row) for row in rows]


def list_agent_runs(db_path: Path, command_id: Optional[str] = None) -> List[AgentRunRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        if command_id:
            return list_agent_runs_for_command(conn, command_id)
        rows = conn.execute("SELECT * FROM agent_runs ORDER BY created_at ASC").fetchall()
    return [agent_run_from_row(row) for row in rows]


def get_agent_run_by_id(conn, agent_run_id: str) -> Optional[AgentRunRecord]:
    row = conn.execute("SELECT * FROM agent_runs WHERE id = ?", (agent_run_id,)).fetchone()
    return agent_run_from_row(row) if row else None


def reconcile_orphaned_agent_runs(conn) -> List[str]:
    rows = conn.execute(
        """
        SELECT * FROM agent_runs
        WHERE state = ? AND process_id IS NOT NULL
        ORDER BY created_at ASC
        """,
        (AgentRunState.RUNNING.value,),
    ).fetchall()
    affected_commands: List[str] = []
    for row in rows:
        agent_run = agent_run_from_row(row)
        if process_is_alive(agent_run.process_id):
            continue
        detail = RUNTIME_INTERRUPTED_DETAIL
        issue = AgentExecutionError(
            OperatorIssueKind.RUNTIME_INTERRUPTED,
            operator_issue_message(OperatorIssueKind.RUNTIME_INTERRUPTED),
            detail=detail,
        )
        update_agent_run(
            conn,
            agent_run.id,
            state=AgentRunState.FAILED.value,
            error=detail,
            process_id=None,
            finished_at=utc_now(),
        )
        append_agent_run_log_conn(
            conn,
            agent_run.id,
            format_agent_log_line(detail, channel="error"),
        )
        append_event(
            conn,
            "agent_run",
            agent_run.id,
            "agent_run_interrupted",
            {"command_id": agent_run.command_id, "detail": detail},
        )
        command_row = conn.execute(
            "SELECT * FROM commands WHERE id = ?",
            (agent_run.command_id,),
        ).fetchone()
        if command_row is None:
            continue
        command = command_from_row(command_row)
        if command.stage in {
            CommandStage.DONE,
            CommandStage.FAILED,
            CommandStage.CANCELED,
            CommandStage.PAUSED,
        }:
            continue
        if agent_run.task_id:
            task_row = conn.execute(
                "SELECT * FROM tasks WHERE id = ?",
                (agent_run.task_id,),
            ).fetchone()
            if task_row is not None:
                task = task_from_row(task_row)
                if task.state == TaskState.RUNNING:
                    update_task(
                        conn,
                        task.id,
                        state=TaskState.BLOCKED.value,
                        error=detail,
                        operator_issue_kind=OperatorIssueKind.RUNTIME_INTERRUPTED.value,
                        assigned_run_id=None,
                    )
                    append_event(
                        conn,
                        "task",
                        task.id,
                        "task_interrupted",
                        {
                            "command_id": command.id,
                            "kind": OperatorIssueKind.RUNTIME_INTERRUPTED.value,
                            "detail": detail,
                        },
                    )
            updated_command = set_command_operator_issue(
                conn,
                command,
                issue=issue,
                agent_run=agent_run,
                task_id=agent_run.task_id,
                run_count=command.run_count,
            )
        else:
            updated_command = set_command_operator_issue(
                conn,
                command,
                issue=issue,
                agent_run=agent_run,
                run_count=command.run_count,
            )
        append_event(
            conn,
            "command",
            updated_command.id,
            "operator_attention_required",
            {
                "kind": OperatorIssueKind.RUNTIME_INTERRUPTED.value,
                "agent_run_id": agent_run.id,
            },
        )
        affected_commands.append(updated_command.id)
    return affected_commands


def _find_active_agent_run(
    conn,
    *,
    command_id: str,
    run_kind: AgentRunKind,
    task_id: Optional[str] = None,
    reviewer_slot: Optional[int] = None,
) -> Optional[AgentRunRecord]:
    clauses = ["command_id = ?", "run_kind = ?", "state IN (?, ?, ?)"]
    params: List[object] = [
        command_id,
        run_kind.value,
        AgentRunState.PENDING_APPROVAL.value,
        AgentRunState.APPROVED.value,
        AgentRunState.RUNNING.value,
    ]
    if task_id is None:
        clauses.append("task_id IS NULL")
    else:
        clauses.append("task_id = ?")
        params.append(task_id)
    if reviewer_slot is None:
        clauses.append("reviewer_slot IS NULL")
    else:
        clauses.append("reviewer_slot = ?")
        params.append(reviewer_slot)
    row = conn.execute(
        f"SELECT * FROM agent_runs WHERE {' AND '.join(clauses)} ORDER BY created_at ASC LIMIT 1",
        params,
    ).fetchone()
    return agent_run_from_row(row) if row else None


def ensure_agent_run(
    conn,
    *,
    command: CommandRecord,
    role_name: str,
    capability: str,
    runtime: RuntimeBackend,
    model: str,
    run_kind: AgentRunKind,
    title: str,
    resume_stage: CommandStage,
    approval_required: bool,
    task_id: Optional[str] = None,
    reviewer_slot: Optional[int] = None,
    prompt_excerpt: Optional[str] = None,
) -> AgentRunRecord:
    existing = _find_active_agent_run(
        conn,
        command_id=command.id,
        run_kind=run_kind,
        task_id=task_id,
        reviewer_slot=reviewer_slot,
    )
    if existing is not None:
        return existing
    return create_agent_run_record(
        conn,
        command_id=command.id,
        role_name=role_name,
        capability=capability,
        runtime=runtime,
        model=model,
        run_kind=run_kind,
        title=title,
        resume_stage=resume_stage,
        state=AgentRunState.PENDING_APPROVAL if approval_required else AgentRunState.RUNNING,
        approval_required=approval_required,
        task_id=task_id,
        reviewer_slot=reviewer_slot,
        prompt_excerpt=prompt_excerpt,
    )


def mark_agent_run_running(conn, agent_run_id: str) -> AgentRunRecord:
    now = utc_now()
    update_agent_run(
        conn,
        agent_run_id,
        state=AgentRunState.RUNNING.value,
        started_at=now,
        error=None,
    )
    append_event(conn, "agent_run", agent_run_id, "agent_run_started", {})
    refreshed = get_agent_run_by_id(conn, agent_run_id)
    if refreshed is not None:
        append_agent_run_log_conn(
            conn,
            agent_run_id,
            format_agent_log_line("approval granted, run started"),
        )
    return refreshed


def mark_agent_run_finished(
    conn,
    agent_run_id: str,
    *,
    state: AgentRunState,
    output_summary: Optional[str] = None,
    error: Optional[str] = None,
) -> AgentRunRecord:
    update_agent_run(
        conn,
        agent_run_id,
        state=state.value,
        output_summary=output_summary,
        error=error,
        process_id=None,
        finished_at=utc_now(),
    )
    append_event(
        conn,
        "agent_run",
        agent_run_id,
        "agent_run_finished",
        {"state": state.value, "error": error, "output_summary": output_summary},
    )
    if state == AgentRunState.COMPLETED:
        append_agent_run_log_conn(
            conn,
            agent_run_id,
            format_agent_log_line(output_summary or "run completed"),
        )
    elif state == AgentRunState.FAILED:
        append_agent_run_log_conn(
            conn,
            agent_run_id,
            format_agent_log_line(error or "run failed", channel="error"),
        )
    elif state == AgentRunState.DENIED:
        append_agent_run_log_conn(
            conn,
            agent_run_id,
            format_agent_log_line(error or "run denied", channel="error"),
        )
    return get_agent_run_by_id(conn, agent_run_id)


def list_pending_agent_runs_for_command(conn, command_id: str) -> List[AgentRunRecord]:
    rows = conn.execute(
        """
        SELECT * FROM agent_runs
        WHERE command_id = ? AND state = ?
        ORDER BY created_at ASC
        """,
        (command_id, AgentRunState.PENDING_APPROVAL.value),
    ).fetchall()
    return [agent_run_from_row(row) for row in rows]


def list_approved_agent_runs_for_command(conn, command_id: str) -> List[AgentRunRecord]:
    rows = conn.execute(
        """
        SELECT * FROM agent_runs
        WHERE command_id = ? AND state = ?
        ORDER BY created_at ASC
        """,
        (command_id, AgentRunState.APPROVED.value),
    ).fetchall()
    return [agent_run_from_row(row) for row in rows]


def command_dependency_map(conn) -> Dict[str, List[str]]:
    rows = conn.execute(
        """
        SELECT command_id, depends_on_command_id
        FROM command_dependencies
        ORDER BY command_id ASC, depends_on_command_id ASC
        """
    ).fetchall()
    mapping: Dict[str, List[str]] = {}
    for row in rows:
        mapping.setdefault(row["command_id"], []).append(row["depends_on_command_id"])
    return mapping


def attach_command_dependencies(
    commands: Sequence[CommandRecord],
    dep_map: Dict[str, List[str]],
) -> List[CommandRecord]:
    return [
        command.model_copy(update={"depends_on": dep_map.get(command.id, [])})
        for command in commands
    ]


def enrich_command_dependency_state(commands: Sequence[CommandRecord]) -> List[CommandRecord]:
    by_id = {command.id: command for command in commands}
    enriched: List[CommandRecord] = []
    for command in commands:
        depends_on = list(command.depends_on)
        blocking_ids: List[str] = []
        dependency_state = "none"
        can_ignore = False
        if (
            command.stage
            not in {
                CommandStage.DONE,
                CommandStage.FAILED,
                CommandStage.CANCELED,
            }
            and depends_on
        ):
            waiting_ids: List[str] = []
            failed_ids: List[str] = []
            for dependency_id in depends_on:
                dependency = by_id.get(dependency_id)
                if dependency is None:
                    failed_ids.append(dependency_id)
                    continue
                if dependency.stage == CommandStage.DONE:
                    continue
                if dependency.stage in {CommandStage.FAILED, CommandStage.CANCELED}:
                    failed_ids.append(dependency_id)
                else:
                    waiting_ids.append(dependency_id)
            if failed_ids:
                dependency_state = "failed"
                blocking_ids = failed_ids
                can_ignore = True
            elif waiting_ids:
                dependency_state = "waiting"
                blocking_ids = waiting_ids
            else:
                dependency_state = "ready"
        enriched.append(
            command.model_copy(
                update={
                    "dependency_state": dependency_state,
                    "blocking_dependency_ids": blocking_ids,
                    "can_ignore_dependencies": can_ignore,
                }
            )
        )
    return enriched


def dependency_map_for_command(conn, command_id: str) -> Dict[str, List[str]]:
    rows = conn.execute(
        """
        SELECT task_id, depends_on_task_id
        FROM task_dependencies
        JOIN tasks ON tasks.id = task_dependencies.task_id
        WHERE tasks.command_id = ?
        ORDER BY task_id ASC, depends_on_task_id ASC
        """,
        (command_id,),
    ).fetchall()
    mapping: Dict[str, List[str]] = {}
    for row in rows:
        mapping.setdefault(row["task_id"], []).append(row["depends_on_task_id"])
    return mapping


def attach_dependencies(
    tasks: Sequence[TaskRecord], dep_map: Dict[str, List[str]]
) -> List[TaskRecord]:
    return [task.model_copy(update={"depends_on": dep_map.get(task.id, [])}) for task in tasks]


def list_tasks_for_command(conn, command_id: str) -> List[TaskRecord]:
    rows = conn.execute(
        """
        SELECT * FROM tasks
        WHERE command_id = ?
        ORDER BY plan_order ASC, created_at ASC
        """,
        (command_id,),
    ).fetchall()
    tasks = [task_from_row(row) for row in rows]
    return attach_dependencies(tasks, dependency_map_for_command(conn, command_id))


def list_questions_for_command(conn, command_id: str) -> List[QuestionRecord]:
    rows = conn.execute(
        """
        SELECT * FROM questions
        WHERE command_id = ?
        ORDER BY created_at ASC
        """,
        (command_id,),
    ).fetchall()
    return [question_from_row(row) for row in rows]


def list_reviews_for_command(conn, command_id: str) -> List[ReviewRecord]:
    rows = conn.execute(
        """
        SELECT * FROM reviews
        WHERE command_id = ?
        ORDER BY created_at ASC, reviewer_slot ASC
        """,
        (command_id,),
    ).fetchall()
    return [review_from_row(row) for row in rows]


def list_instructions_for_command(conn, command_id: str) -> List[InstructionRecord]:
    rows = conn.execute(
        """
        SELECT * FROM instructions
        WHERE command_id = ?
        ORDER BY created_at ASC
        """,
        (command_id,),
    ).fetchall()
    return [instruction_from_row(row) for row in rows]


def get_question_by_id(conn, question_id: str) -> Optional[QuestionRecord]:
    row = conn.execute(
        "SELECT * FROM questions WHERE id = ?",
        (question_id,),
    ).fetchone()
    return question_from_row(row) if row else None


def build_context_payload(
    command: CommandRecord,
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
    task: Optional[TaskRecord] = None,
) -> dict:
    payload = {
        "command": command.model_dump(mode="json"),
        "tasks": [item.model_dump(mode="json") for item in tasks],
        "questions": [item.model_dump(mode="json") for item in questions],
        "reviews": [item.model_dump(mode="json") for item in reviews],
        "instructions": [item.model_dump(mode="json") for item in instructions],
    }
    if command.resume_hint:
        payload["resume_hint"] = command.resume_hint
    if task is not None:
        payload["task"] = task.model_dump(mode="json")
    return payload


def build_worker_context_payload(
    command: CommandRecord,
    task: TaskRecord,
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
) -> dict:
    completed_tasks = []
    for item in tasks:
        if item.id == task.id or item.state != TaskState.DONE:
            continue
        summary = None
        if item.output_payload:
            summary = item.output_payload.get("summary")
        completed_tasks.append(
            {
                "id": item.id,
                "title": item.title,
                "capability": item.capability,
                "summary": summary,
            }
        )

    dependency_context = []
    dependency_ids = set(task.depends_on)
    for item in tasks:
        if item.id not in dependency_ids:
            continue
        summary = None
        if item.output_payload:
            summary = item.output_payload.get("summary")
        dependency_context.append(
            {
                "id": item.id,
                "title": item.title,
                "state": item.state.value,
                "summary": summary,
            }
        )

    recent_reviews = [
        {
            "reviewer_slot": review.reviewer_slot,
            "decision": review.decision.value,
            "summary": review.summary,
            "findings": review.findings,
        }
        for review in reviews[-3:]
    ]
    answered_questions = [
        {
            "source": question.source,
            "question": question.question,
            "answer": question.answer,
        }
        for question in questions
        if question.state in {QuestionState.ANSWERED, QuestionState.RESOLVED}
    ][-3:]
    return {
        "command": {
            "id": command.id,
            "goal": command.goal,
            "workflow_mode": command.workflow_mode.value,
            "effective_mode": command.effective_mode.value if command.effective_mode else None,
            "workspace_root": command.workspace_root,
            "runbook_path": command.runbook_path,
        },
        "task": task.model_dump(mode="json"),
        "resume_hint": command.resume_hint,
        "dependencies": dependency_context,
        "completed_tasks": completed_tasks[-8:],
        "answered_questions": answered_questions,
        "recent_reviews": recent_reviews,
        "instructions": [item.model_dump(mode="json") for item in instructions],
    }


def build_review_context_payload(
    command: CommandRecord,
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
) -> dict:
    explicit_paths = extract_explicit_paths(command.goal)
    completed_tasks = []
    write_targets: List[str] = []
    for item in tasks:
        if item.state != TaskState.DONE:
            continue
        task_write_files = [
            path
            for path in item.input_payload.get("write_files", [])
            if isinstance(path, str) and path.strip()
        ]
        for path in task_write_files:
            if path not in write_targets:
                write_targets.append(path)
        summary = None
        if item.output_payload:
            summary = item.output_payload.get("summary")
        completed_tasks.append(
            {
                "id": item.id,
                "title": item.title,
                "capability": item.capability,
                "kind": item.kind,
                "summary": summary,
                "write_files": task_write_files,
            }
        )

    recent_reviews = [
        {
            "reviewer_slot": review.reviewer_slot,
            "decision": review.decision.value,
            "summary": review.summary,
            "findings": review.findings,
        }
        for review in reviews[-3:]
    ]
    answered_questions = [
        {
            "source": question.source,
            "question": question.question,
            "answer": question.answer,
        }
        for question in questions
        if question.state in {QuestionState.ANSWERED, QuestionState.RESOLVED}
    ][-3:]
    return {
        "command": {
            "id": command.id,
            "goal": command.goal,
            "workflow_mode": command.workflow_mode.value,
            "effective_mode": command.effective_mode.value if command.effective_mode else None,
            "workspace_root": command.workspace_root,
            "runbook_path": command.runbook_path,
        },
        "resume_hint": command.resume_hint,
        "explicit_paths": explicit_paths,
        "write_targets": write_targets[:12],
        "completed_tasks": completed_tasks[-8:],
        "answered_questions": answered_questions,
        "recent_reviews": recent_reviews,
        "instructions": [item.model_dump(mode="json") for item in instructions],
    }


def prompt_excerpt(prompt: str, limit: int = 240) -> str:
    compact = " ".join(prompt.strip().split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1] + "…"


def extract_explicit_paths(text: str) -> List[str]:
    seen: List[str] = []
    for match in EXPLICIT_PATH_PATTERN.findall(text or ""):
        if match not in seen:
            seen.append(match)
    return seen[:6]


class AgentExecutionError(RuntimeError):
    def __init__(
        self,
        kind: OperatorIssueKind,
        message: str,
        *,
        detail: str,
    ) -> None:
        super().__init__(message)
        self.kind = kind
        self.detail = detail


def classify_agent_failure_kind(text: str) -> Optional[OperatorIssueKind]:
    lowered = (text or "").lower()
    if any(pattern in lowered for pattern in PROVIDER_LIMIT_PATTERNS):
        return OperatorIssueKind.PROVIDER_LIMIT
    if any(pattern in lowered for pattern in AUTH_REQUIRED_PATTERNS):
        return OperatorIssueKind.AUTH_REQUIRED
    if any(pattern in lowered for pattern in INTERACTIVE_PROMPT_PATTERNS):
        return OperatorIssueKind.INTERACTIVE_PROMPT
    return None


def operator_issue_message(kind: OperatorIssueKind) -> str:
    if kind == OperatorIssueKind.PROVIDER_LIMIT:
        return "The selected AI hit a usage limit before it could finish."
    if kind == OperatorIssueKind.AUTH_REQUIRED:
        return "The selected AI needs sign-in or authentication before it can continue."
    if kind == OperatorIssueKind.INTERACTIVE_PROMPT:
        return "The selected AI is waiting for interactive input and needs operator attention."
    if kind == OperatorIssueKind.RUNTIME_TIMEOUT:
        return "The selected AI timed out before it returned a result."
    if kind == OperatorIssueKind.RUNTIME_INTERRUPTED:
        return "The selected AI stopped before it returned a final result."
    return "The selected AI needs operator attention before this job can continue."


def build_operator_resume_hint(
    kind: OperatorIssueKind,
    *,
    runtime: RuntimeBackend,
    title: str,
    backend_override: Optional[RuntimeBackend] = None,
) -> str:
    selected_runtime_label = runtime_label(backend_override or runtime)
    if kind == OperatorIssueKind.PROVIDER_LIMIT:
        reason = "the previous attempt stopped because the provider usage limit was reached"
    elif kind == OperatorIssueKind.AUTH_REQUIRED:
        reason = "the previous attempt stopped because authentication was required"
    elif kind == OperatorIssueKind.INTERACTIVE_PROMPT:
        reason = "the previous attempt stopped because the CLI was waiting for interactive input"
    elif kind == OperatorIssueKind.RUNTIME_INTERRUPTED:
        reason = "the previous attempt stopped before it returned a final result"
    else:
        reason = "the previous attempt stopped because the CLI timed out"
    return (
        f"This is a retry for '{title}'. The previous attempt stopped because {reason}. "
        f"Continue from the current workspace state using {selected_runtime_label}. "
        "Do not restart from scratch unless the current files are unusable."
    )


def is_recoverable_operator_issue(exc: Exception) -> bool:
    return isinstance(exc, AgentExecutionError)


def process_is_alive(process_id: Optional[int]) -> bool:
    if not process_id or process_id <= 0:
        return False
    proc_stat = Path(f"/proc/{process_id}/stat")
    if proc_stat.exists():
        try:
            state = proc_stat.read_text(encoding="utf-8").split()[2]
        except (IndexError, OSError):
            return False
        return state != "Z"
    try:
        os.kill(process_id, 0)
    except OSError:
        return False
    return True


def terminate_process_group(process_id: Optional[int]) -> None:
    if not process_id or process_id <= 0:
        return
    try:
        os.killpg(process_id, signal.SIGTERM)
    except ProcessLookupError:
        return
    except OSError:
        try:
            os.kill(process_id, signal.SIGTERM)
        except OSError:
            return


def build_runtime_env(runtime_home: Optional[Path]) -> dict[str, str]:
    env = dict(os.environ)
    if runtime_home is not None:
        runtime_home.mkdir(parents=True, exist_ok=True)
        env["HOME"] = str(runtime_home)
    return env


def run_logged_subprocess(
    command: List[str],
    workspace_root: Path,
    env: dict[str, str],
    timeout_seconds: int,
    *,
    agent_run_id: Optional[str] = None,
    db_path: Optional[Path] = None,
) -> Tuple[int, str, str]:
    process = subprocess.Popen(
        command,
        cwd=str(workspace_root),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1,
        start_new_session=True,
    )
    if agent_run_id and db_path:
        with connect(db_path) as conn:
            update_agent_run(conn, agent_run_id, process_id=process.pid)
            append_agent_run_log_conn(
                conn,
                agent_run_id,
                format_agent_log_line(f"pid: {process.pid}"),
            )
            conn.commit()

    stdout_chunks: List[str] = []
    stderr_chunks: List[str] = []

    def consume(stream, target: List[str], channel: str) -> None:
        if stream is None:
            return
        try:
            for line in iter(stream.readline, ""):
                target.append(line)
                if agent_run_id and db_path and line:
                    append_agent_run_log(
                        db_path,
                        agent_run_id,
                        format_agent_log_line(line.rstrip("\n"), channel=channel),
                    )
        finally:
            stream.close()

    stdout_thread = threading.Thread(
        target=consume,
        args=(process.stdout, stdout_chunks, "stdout"),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=consume,
        args=(process.stderr, stderr_chunks, "stderr"),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()
    try:
        returncode = process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        terminate_process_group(process.pid)
        stdout_thread.join(timeout=1)
        stderr_thread.join(timeout=1)
        raise
    stdout_thread.join(timeout=1)
    stderr_thread.join(timeout=1)
    return returncode, "".join(stdout_chunks), "".join(stderr_chunks)


def detect_test_command(workspace_root: Path, configured: Optional[str]) -> Optional[List[str]]:
    if configured:
        return shlex.split(configured)

    local_pytest = workspace_root / ".venv" / "bin" / "pytest"
    if local_pytest.is_file():
        return [str(local_pytest), "-q"]

    interpreter_pytest = Path(sys.executable).resolve().with_name("pytest")
    if interpreter_pytest.is_file():
        return [str(interpreter_pytest), "-q"]

    pytest_bin = shutil.which("pytest")
    if pytest_bin:
        return [pytest_bin, "-q"]

    if importlib.util.find_spec("pytest") is not None:
        return [sys.executable, "-m", "pytest", "-q"]

    return None


def is_pytest_command(command: Sequence[str]) -> bool:
    if not command:
        return False
    joined = " ".join(command).lower()
    return "pytest" in joined


def execute_deterministic_test_task(
    command: CommandRecord,
    task: TaskRecord,
    settings: AppConfig,
    *,
    agent_run_id: Optional[str] = None,
) -> WorkerOutput:
    workspace_root = Path(command.workspace_root)
    test_command = detect_test_command(workspace_root, settings.test_command)
    if not test_command:
        raise RuntimeError(
            "No deterministic test command is configured or detected. "
            "Set [testing] command in wevra.ini."
        )

    if agent_run_id:
        append_agent_run_log(
            settings.db_path,
            agent_run_id,
            format_agent_log_line(f"$ {shlex.join(test_command)}"),
        )
    stdout_text = ""
    stderr_text = ""
    returncode = 0
    try:
        returncode, stdout_text, stderr_text = run_logged_subprocess(
            test_command,
            workspace_root,
            build_runtime_env(settings.runtime_home),
            settings.agent_timeout_seconds,
            agent_run_id=agent_run_id,
            db_path=settings.db_path,
        )
    except subprocess.TimeoutExpired as exc:
        raise AgentExecutionError(
            OperatorIssueKind.RUNTIME_TIMEOUT,
            operator_issue_message(OperatorIssueKind.RUNTIME_TIMEOUT),
            detail=f"Test command timed out after {settings.agent_timeout_seconds} seconds.",
        ) from exc

    combined = (stdout_text.strip() + "\n" + stderr_text.strip()).strip()
    if returncode == 5 and is_pytest_command(test_command):
        summary = f"No existing tests were found for `{shlex.join(test_command)}`."
        return WorkerOutput(
            decision=WorkerDecision.COMPLETE,
            summary=summary,
            result={
                "status": "completed",
                "task_key": task.task_key,
                "task_kind": task.kind,
                "test_command": test_command,
                "stdout_tail": stdout_text.strip().splitlines()[-20:],
                "stderr_tail": stderr_text.strip().splitlines()[-20:],
                "no_tests_collected": True,
            },
        )

    if returncode != 0:
        raise RuntimeError(combined or f"Test command failed with exit code {returncode}.")

    summary = f"Executed existing tests with `{shlex.join(test_command)}`."
    if stdout_text.strip():
        summary = f"{summary} Last output: {stdout_text.strip().splitlines()[-1]}"
    return WorkerOutput(
        decision=WorkerDecision.COMPLETE,
        summary=summary,
        result={
            "status": "completed",
            "task_key": task.task_key,
            "task_kind": task.kind,
            "test_command": test_command,
            "stdout_tail": stdout_text.strip().splitlines()[-20:],
            "stderr_tail": stderr_text.strip().splitlines()[-20:],
        },
    )


class BackendInterface:
    def plan(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> PlannerOutput:
        raise NotImplementedError

    def execute_task(
        self,
        command: CommandRecord,
        task: TaskRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> WorkerOutput:
        raise NotImplementedError

    def review(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        reviewer_slot: int,
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> ReviewerOutput:
        raise NotImplementedError


class MockBackend(BackendInterface):
    def plan(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> PlannerOutput:
        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line("mock planner evaluating the request"),
            )
        goal = " ".join([command.goal, *[instruction.body for instruction in instructions]]).lower()
        current_mode = command.effective_mode or (
            command.workflow_mode
            if command.workflow_mode != WorkflowMode.AUTO
            else infer_mode_from_goal(goal)
        )
        planner_questions = [question for question in questions if question.source == "planner"]
        if "[planner_question]" in goal and not any(
            question.answer for question in planner_questions
        ):
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line("mock planner requested clarification"),
                )
            return PlannerOutput(
                decision=PlannerDecision.ASK_QUESTION,
                workflow_mode=current_mode,
                question="Planner needs clarification before scheduling work.",
            )

        latest_review = reviews[-1] if reviews else None
        if latest_review and latest_review.decision == ReviewDecision.REQUEST_CHANGES:
            if not any(task.input_payload.get("review_id") == latest_review.id for task in tasks):
                if agent_run_id and db_path:
                    append_agent_run_log(
                        db_path,
                        agent_run_id,
                        format_agent_log_line("mock planner generated a review rework task"),
                    )
                return PlannerOutput(
                    decision=PlannerDecision.CREATE_TASKS,
                    workflow_mode=current_mode,
                    tasks=[
                        PlannerTaskSpec(
                            key="review_rework",
                            kind="rework",
                            capability="implementer",
                            title="Address review findings",
                            description=f"{latest_review.summary}\n\n"
                            + "\n".join(latest_review.findings),
                            input_payload={"review_id": latest_review.id},
                            write_files=["src/rework.txt"],
                        )
                    ],
                )

        failed_tasks = [task for task in tasks if task.state == TaskState.FAILED]
        if failed_tasks:
            latest_failed = failed_tasks[-1]
            if not any(task.input_payload.get("retry_of") == latest_failed.id for task in tasks):
                if agent_run_id and db_path:
                    append_agent_run_log(
                        db_path,
                        agent_run_id,
                        format_agent_log_line("mock planner scheduled a retry task"),
                    )
                return PlannerOutput(
                    decision=PlannerDecision.CREATE_TASKS,
                    workflow_mode=current_mode,
                    tasks=[
                        PlannerTaskSpec(
                            key=f"retry_{latest_failed.task_key or latest_failed.id}",
                            kind="rework",
                            capability="implementer",
                            title=f"Recover failed task: {latest_failed.title}",
                            description=latest_failed.error or latest_failed.description,
                            input_payload={"retry_of": latest_failed.id},
                            write_files=latest_failed.input_payload.get("write_files", []),
                        )
                    ],
                )

        active_or_done = [
            task
            for task in tasks
            if task.state
            in {TaskState.PENDING, TaskState.RUNNING, TaskState.BLOCKED, TaskState.DONE}
        ]
        default_specs: List[PlannerTaskSpec]
        if current_mode == WorkflowMode.IMPLEMENTATION and "[parallel]" in goal:
            default_specs = [
                PlannerTaskSpec(
                    key="part_a",
                    kind="implementation",
                    capability="implementer",
                    title="Implement part A",
                    description=command.goal,
                    write_files=["src/part_a.txt"],
                ),
                PlannerTaskSpec(
                    key="part_b",
                    kind="implementation",
                    capability="implementer",
                    title="Implement part B",
                    description=command.goal,
                    write_files=["src/part_b.txt"],
                ),
                PlannerTaskSpec(
                    key="integration",
                    kind="implementation",
                    capability="implementer",
                    title="Integrate parallel work",
                    description="Merge part A and part B into the final result.",
                    depends_on=["part_a", "part_b"],
                    write_files=["src/integration.txt"],
                ),
            ]
        elif current_mode == WorkflowMode.IMPLEMENTATION:
            default_specs = [
                PlannerTaskSpec(
                    key="implementation_main",
                    kind="implementation",
                    capability="implementer",
                    title="Implement the command goal",
                    description=command.goal,
                    write_files=["src/implementation.txt"],
                )
            ]
        elif current_mode == WorkflowMode.RESEARCH:
            default_specs = [
                PlannerTaskSpec(
                    key="research_collect",
                    kind="investigation",
                    capability="investigation",
                    title="Investigate the request",
                    description=command.goal,
                    write_files=[],
                ),
                PlannerTaskSpec(
                    key="research_report",
                    kind="analysis",
                    capability="analyst",
                    title="Summarize findings into a report",
                    description="Turn the investigation results into a clear report for the user.",
                    depends_on=["research_collect"],
                    write_files=[],
                ),
            ]
        elif current_mode == WorkflowMode.REVIEW:
            default_specs = [
                PlannerTaskSpec(
                    key="review_context",
                    kind="analysis",
                    capability="analyst",
                    title="Collect review context",
                    description="Inspect the current workspace and gather the context needed for a final review pass.",
                    write_files=[],
                )
            ]
        elif current_mode == WorkflowMode.DOGFOODING:
            runbook_detail = command.runbook_path or "the provided runbook path"
            default_specs = [
                PlannerTaskSpec(
                    key="dogfood_plan",
                    kind="analysis",
                    capability="analyst",
                    title="Prepare the dogfooding pass",
                    description=(
                        "Read the operating runbook, identify the key workflow checkpoints, and prepare the validation pass "
                        f"for the documented product flow.\n\nRunbook path: {runbook_detail}"
                    ),
                    write_files=[],
                )
            ]
        else:
            default_specs = [
                PlannerTaskSpec(
                    key="planning_brief",
                    kind="analysis",
                    capability="analyst",
                    title="Produce a structured execution plan",
                    description=command.goal,
                    write_files=[],
                )
            ]

        if "[append_extra]" in goal:
            dependency_key = default_specs[-1].key
            default_specs.append(
                PlannerTaskSpec(
                    key="append_followup",
                    kind="followup",
                    capability=default_specs[-1].capability,
                    title="Apply appended instruction",
                    description="Incorporate the latest user-appended instruction into the result.",
                    depends_on=[dependency_key],
                    write_files=[]
                    if current_mode != WorkflowMode.IMPLEMENTATION
                    else ["src/append_followup.txt"],
                )
            )

        if command.stage == CommandStage.REPLANNING and instructions:
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line(
                        "mock planner updated the task graph for appended instructions"
                    ),
                )
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=current_mode,
                tasks=default_specs,
            )

        if not active_or_done:
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line(
                        f"mock planner created {len(default_specs)} task(s) for {current_mode.value}"
                    ),
                )
            if current_mode == WorkflowMode.IMPLEMENTATION and "[parallel]" in goal:
                return PlannerOutput(
                    decision=PlannerDecision.CREATE_TASKS,
                    workflow_mode=current_mode,
                    tasks=default_specs,
                )
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=current_mode,
                tasks=default_specs,
            )

        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line("mock planner determined no further planning is required"),
            )
        if current_mode == WorkflowMode.PLANNING:
            return PlannerOutput(
                decision=PlannerDecision.COMPLETE,
                workflow_mode=current_mode,
                final_response=(
                    "## Plan\n"
                    "- Deliver a structured planning result for the request.\n\n"
                    "## Design Direction\n"
                    "- Keep the implementation deferred.\n"
                    "- Focus on the execution approach and sequencing.\n\n"
                    "## Task Breakdown\n"
                    "1. Produce the planning brief.\n"
                    "2. Return it as the final result."
                ),
            )
        return PlannerOutput(
            decision=PlannerDecision.COMPLETE,
            workflow_mode=current_mode,
            final_response="Planner determined that no further task generation is required.",
        )

    def execute_task(
        self,
        command: CommandRecord,
        task: TaskRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> WorkerOutput:
        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line(f"mock worker started '{task.title}'"),
            )
        goal = " ".join([command.goal, *[instruction.body for instruction in instructions]]).lower()
        task_questions = [question for question in questions if question.task_id == task.id]
        is_primary_worker = task.capability == "implementer"
        if (
            "[worker_question]" in goal
            and is_primary_worker
            and not any(question.answer for question in task_questions)
        ):
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line("mock worker requested clarification"),
                )
            return WorkerOutput(
                decision=WorkerDecision.ASK_QUESTION,
                question=f"Need clarification before continuing task '{task.title}'.",
                resolution_mode=QuestionResolutionMode.RESUME_TASK,
            )

        if (
            "[worker_replan]" in goal
            and is_primary_worker
            and not any(question.answer for question in task_questions)
        ):
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line("mock worker requested replanning"),
                )
            return WorkerOutput(
                decision=WorkerDecision.ASK_QUESTION,
                question=f"Clarification will change the plan for '{task.title}'.",
                resolution_mode=QuestionResolutionMode.REPLAN_COMMAND,
            )

        if (
            "[worker_fail]" in goal
            and is_primary_worker
            and task.input_payload.get("retry_of") is None
            and task.attempt_count <= 1
        ):
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line(
                        "mock worker raised a recoverable failure", channel="error"
                    ),
                )
            return WorkerOutput(
                decision=WorkerDecision.FAIL,
                failure_reason=f"Mock worker failed '{task.title}' and requested replanning.",
            )

        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line(f"mock worker completed '{task.title}'"),
            )
        return WorkerOutput(
            decision=WorkerDecision.COMPLETE,
            summary=f"Completed task '{task.title}'.",
            result={
                "status": "completed",
                "task_key": task.task_key,
                "task_kind": task.kind,
                "write_files": task.input_payload.get("write_files", []),
            },
        )

    def review(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        reviewer_slot: int,
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> ReviewerOutput:
        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line(f"mock reviewer {reviewer_slot} started"),
            )
        goal = " ".join([command.goal, *[instruction.body for instruction in instructions]]).lower()
        if "[review_fail]" in goal:
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line("mock reviewer failed hard", channel="error"),
                )
            return ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary=f"Reviewer {reviewer_slot} failed hard.",
                failure_reason="Review backend encountered a terminal validation failure.",
            )

        if "[review_changes]" in goal and not any(
            review.decision == ReviewDecision.REQUEST_CHANGES for review in reviews
        ):
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line("mock reviewer requested follow-up changes"),
                )
            return ReviewerOutput(
                decision=ReviewDecision.REQUEST_CHANGES,
                summary=f"Reviewer {reviewer_slot} requested a follow-up pass.",
                findings=["Add a second implementation pass before completion."],
            )

        if agent_run_id and db_path:
            append_agent_run_log(
                db_path,
                agent_run_id,
                format_agent_log_line(f"mock reviewer {reviewer_slot} approved the result"),
            )
        return ReviewerOutput(
            decision=ReviewDecision.APPROVE,
            summary=f"Reviewer {reviewer_slot} approved the current result set.",
            findings=[],
        )


class StructuredCliBackend(BackendInterface):
    def __init__(
        self,
        backend: RuntimeBackend,
        model: str,
        approval_mode: ApprovalMode,
        timeout_seconds: int,
        runtime_home: Path | None = None,
    ):
        self.backend = backend
        self.model = model
        self.approval_mode = approval_mode
        self.timeout_seconds = timeout_seconds
        self.runtime_home = runtime_home

    def plan(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> PlannerOutput:
        context = build_context_payload(command, tasks, questions, reviews, instructions)
        planning_mode = "replanning" if command.stage == CommandStage.REPLANNING else "initial_plan"
        requested = requested_mode(command)
        active_mode = effective_mode(command)
        explicit_paths = extract_explicit_paths(command.goal)
        explicit_path_block = ""
        if explicit_paths:
            explicit_path_lines = "\n".join(f"- {path}" for path in explicit_paths)
            explicit_path_block = (
                "The request explicitly names these files or paths. Keep planning anchored to them.\n"
                "Do not inspect unrelated files, and do not scan the wider repository unless one of these paths forces it.\n"
                "Only look at directly related tests if you genuinely need to update them.\n"
                f"{explicit_path_lines}\n"
            )
        prompt = (
            "You are the Wevra planner.\n"
            "The engine owns all state transitions.\n"
            "Return only JSON that matches the provided schema.\n"
            "Every task must include a stable key, explicit depends_on references, and intended write_files.\n"
            "Keep planning lightweight: inspect only the minimum files needed to name concrete write targets.\n"
            "Do not run tests or broad repository sweeps during planning.\n"
            "Prefer a small plan with one to three tasks unless the request clearly requires more.\n"
            "For single-file UI, docs, copy, or test updates, prefer exactly one implementer task unless a second task is strictly necessary.\n"
            "Use only these task capabilities: implementer, investigation, analyst, tester, reviewer.\n"
            "For UI, docs, test-file, or general code changes, use capability 'implementer'.\n"
            f"Planning mode: {planning_mode}.\n"
            f"Requested workflow mode: {requested.value}.\n"
            f"Current effective workflow mode: {active_mode.value if active_mode else 'unresolved'}.\n"
            f"{mode_prompt_guidance(active_mode if requested != WorkflowMode.AUTO else WorkflowMode.AUTO)}\n"
            f"{explicit_path_block}"
            "If this is replanning, preserve completed work as fact, reuse still-valid task keys, and only omit tasks that should be superseded.\n\n"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt,
            PlannerOutput.model_json_schema(),
            command.workspace_root,
            agent_run_id=agent_run_id,
            db_path=db_path,
        )
        return PlannerOutput.model_validate(payload)

    def execute_task(
        self,
        command: CommandRecord,
        task: TaskRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> WorkerOutput:
        context = build_worker_context_payload(
            command, task, tasks, questions, reviews, instructions
        )
        write_files = task.input_payload.get("write_files", [])
        write_targets = [
            f"- {path}" for path in write_files if isinstance(path, str) and path.strip()
        ]
        target_block = ""
        if write_targets:
            target_block = f"Primary write targets:\n{chr(10).join(write_targets)}\n"
        explicit_paths = extract_explicit_paths(command.goal)
        explicit_path_block = ""
        if explicit_paths:
            explicit_path_block = (
                "The original request explicitly named these files or paths. Stay anchored to them unless the task context clearly requires a directly related companion file.\n"
                f"{chr(10).join(f'- {path}' for path in explicit_paths)}\n"
            )
        focus_note = (
            "Focus on the listed write targets and directly related tests only.\n"
            if write_targets
            else "Keep the scope narrow and only touch files directly required for this task.\n"
        )
        prompt = (
            "You are the Wevra worker.\n"
            "Use the workspace to complete the assigned task if changes are required.\n"
            "Do not mutate engine state. Return only JSON matching the schema.\n\n"
            f"{focus_note}"
            f"{explicit_path_block}"
            "Prefer the smallest coherent change that completes the task.\n"
            "Avoid broad repository sweeps.\n"
            "Do not run the full test suite unless the task explicitly requires it.\n"
            f"{target_block}\n"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt,
            WorkerOutput.model_json_schema(),
            command.workspace_root,
            agent_run_id=agent_run_id,
            db_path=db_path,
        )
        return WorkerOutput.model_validate(payload)

    def review(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
        reviewer_slot: int,
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> ReviewerOutput:
        context = build_review_context_payload(command, tasks, questions, reviews, instructions)
        review_targets = [
            *context["explicit_paths"],
            *[path for path in context["write_targets"] if path not in context["explicit_paths"]],
        ]
        target_block = ""
        if review_targets:
            target_block = (
                "Keep the review anchored to these files or paths. Inspect directly related tests only if they are needed to verify the requested change.\n"
                f"{chr(10).join(f'- {path}' for path in review_targets)}\n"
            )
        prompt = (
            f"You are Wevra reviewer #{reviewer_slot}.\n"
            "Inspect the current command context and workspace state.\n"
            "Keep the review narrow and focused on the requested change.\n"
            "Do not perform broad repository sweeps.\n"
            "Validate that the current workspace satisfies the goal and any prior review findings.\n"
            "Return only JSON matching the schema.\n\n"
            f"{target_block}"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt,
            ReviewerOutput.model_json_schema(),
            command.workspace_root,
            agent_run_id=agent_run_id,
            db_path=db_path,
        )
        return ReviewerOutput.model_validate(payload)

    def _run_structured(
        self,
        prompt: str,
        schema: dict,
        workspace_root: str,
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> dict:
        root = Path(workspace_root)
        adapter = structured_runtime_adapter(self.backend)
        prepared_schema = adapter.schema_preparer(schema)
        schema_path: str | None = None
        output_path: str | None = None
        if self.backend == RuntimeBackend.CODEX:
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as schema_file:
                json.dump(prepared_schema, schema_file)
                schema_path = schema_file.name
            with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as output_file:
                output_path = output_file.name
        try:
            command = adapter.command_builder(
                prompt,
                prepared_schema,
                self.model,
                self.approval_mode,
                schema_path,
                output_path,
            )
            env = self._build_runtime_env()
            if agent_run_id and db_path:
                append_agent_run_log(
                    db_path,
                    agent_run_id,
                    format_agent_log_line(f"$ {' '.join(command[:-1])} <prompt>"),
                )
            returncode, stdout_text, stderr_text = self._run_streaming_process(
                command,
                root,
                env,
                agent_run_id=agent_run_id,
                db_path=db_path,
            )
            if returncode != 0:
                detail = (
                    stderr_text.strip()
                    or stdout_text.strip()
                    or f"{adapter.label} structured run failed"
                )
                issue_kind = classify_agent_failure_kind(detail)
                if issue_kind is not None:
                    raise AgentExecutionError(
                        issue_kind,
                        operator_issue_message(issue_kind),
                        detail=detail,
                    )
                raise RuntimeError(detail)
            return adapter.output_loader(stdout_text, output_path)
        except subprocess.TimeoutExpired as exc:
            raise AgentExecutionError(
                OperatorIssueKind.RUNTIME_TIMEOUT,
                operator_issue_message(OperatorIssueKind.RUNTIME_TIMEOUT),
                detail=adapter.timeout_builder(self.timeout_seconds),
            ) from exc
        finally:
            if schema_path:
                Path(schema_path).unlink(missing_ok=True)
            if output_path:
                Path(output_path).unlink(missing_ok=True)

    def _run_streaming_process(
        self,
        command: List[str],
        workspace_root: Path,
        env: dict[str, str],
        *,
        agent_run_id: Optional[str] = None,
        db_path: Optional[Path] = None,
    ) -> Tuple[int, str, str]:
        return run_logged_subprocess(
            command,
            workspace_root,
            env,
            self.timeout_seconds,
            agent_run_id=agent_run_id,
            db_path=db_path,
        )

    def _build_runtime_env(self) -> dict[str, str]:
        return build_runtime_env(self.runtime_home)


def execution_profile(
    command: CommandRecord, settings: AppConfig, capability: str
) -> Tuple[str, RuntimeBackend, str]:
    role = settings.role_for(capability)
    runtime = role.runtime if command.backend == RuntimeBackend.INHERIT else command.backend
    return role.name, runtime, role.model


def approval_required_for_runtime(command: CommandRecord, runtime: RuntimeBackend) -> bool:
    return runtime != RuntimeBackend.MOCK and command.approval_mode == ApprovalMode.MANUAL


def backend_for(command: CommandRecord, settings: AppConfig, capability: str) -> BackendInterface:
    _, runtime, model = execution_profile(command, settings, capability)
    if runtime == RuntimeBackend.MOCK:
        return MockBackend()
    return StructuredCliBackend(
        runtime,
        model,
        command.approval_mode,
        settings.agent_timeout_seconds,
        runtime_home=settings.runtime_home,
    )


def _normalized_output_locale(locale: Optional[str]) -> str:
    return "ja" if str(locale or "").lower().startswith("ja") else "en"


def _fallback_command_title(
    goal: str,
    workflow_mode: WorkflowMode,
    *,
    locale: Optional[str] = None,
) -> str:
    output_locale = _normalized_output_locale(locale)
    compact_goal = " ".join(str(goal or "").strip().split())
    if not compact_goal:
        return "ジョブ" if output_locale == "ja" else "Job"
    first_line = str(goal or "").strip().splitlines()[0].strip()
    if first_line and len(first_line) <= 48:
        return first_line
    if workflow_mode == WorkflowMode.DOGFOODING:
        return "dogfooding"
    if workflow_mode == WorkflowMode.PLANNING:
        return "計画を作る" if output_locale == "ja" else "Create a plan"
    if workflow_mode == WorkflowMode.RESEARCH:
        return "調査する" if output_locale == "ja" else "Research the request"
    if workflow_mode == WorkflowMode.REVIEW:
        return "レビューする" if output_locale == "ja" else "Review the request"
    if workflow_mode == WorkflowMode.IMPLEMENTATION:
        return "実装する" if output_locale == "ja" else "Implement the change"
    return "ジョブ" if output_locale == "ja" else "Job"


def _mock_command_title(
    goal: str,
    workflow_mode: WorkflowMode,
    *,
    locale: Optional[str] = None,
) -> str:
    output_locale = _normalized_output_locale(locale)
    compact_goal = " ".join(str(goal or "").strip().split())
    if not compact_goal:
        return _fallback_command_title(goal, workflow_mode, locale=locale)
    if len(compact_goal) <= 48:
        return compact_goal
    if output_locale == "ja":
        prefixes = {
            WorkflowMode.IMPLEMENTATION: "実装",
            WorkflowMode.RESEARCH: "調査",
            WorkflowMode.REVIEW: "レビュー",
            WorkflowMode.PLANNING: "計画",
            WorkflowMode.DOGFOODING: "dogfooding",
            WorkflowMode.AUTO: "ジョブ",
        }
        return f"{prefixes.get(workflow_mode, 'ジョブ')}: {compact_goal[:22]}"
    prefixes = {
        WorkflowMode.IMPLEMENTATION: "Implement",
        WorkflowMode.RESEARCH: "Research",
        WorkflowMode.REVIEW: "Review",
        WorkflowMode.PLANNING: "Plan",
        WorkflowMode.DOGFOODING: "Dogfood",
        WorkflowMode.AUTO: "Job",
    }
    return f"{prefixes.get(workflow_mode, 'Job')}: {compact_goal[:30]}"


def _job_title_prompt(
    goal: str,
    workflow_mode: WorkflowMode,
    runbook_path: Optional[str],
    *,
    locale: Optional[str] = None,
) -> str:
    output_language = "Japanese" if _normalized_output_locale(locale) == "ja" else "English"
    runbook_block = f"Runbook path: {runbook_path}\n" if runbook_path else ""
    return (
        "You are naming a Wevra job for the dashboard.\n"
        "Return only JSON that matches the schema.\n"
        "Write a concise title for a top-level job.\n"
        "The title must be short, specific, and useful in a job list.\n"
        "Do not repeat the full brief. Do not use vague labels like 'Task', 'Work', or 'Handle request'.\n"
        "Focus on the concrete deliverable or phase.\n"
        "Avoid trailing punctuation. Keep it under 48 characters when possible.\n"
        f"Write the title in {output_language}.\n"
        f"Workflow mode: {workflow_mode.value}\n"
        f"{runbook_block}"
        f"Job brief:\n{goal}"
    )


def _structured_command_title(
    *,
    runtime: RuntimeBackend,
    model: str,
    settings: AppConfig,
    goal: str,
    workflow_mode: WorkflowMode,
    workspace_root: str,
    runbook_path: Optional[str],
    locale: Optional[str],
) -> str:
    backend = StructuredCliBackend(
        runtime,
        model,
        ApprovalMode.AUTO,
        settings.agent_timeout_seconds,
        runtime_home=settings.runtime_home,
    )
    payload = backend._run_structured(  # noqa: SLF001
        _job_title_prompt(goal, workflow_mode, runbook_path, locale=locale),
        JobTitleOutput.model_json_schema(),
        workspace_root,
    )
    title = JobTitleOutput.model_validate(payload).title.strip()
    return title or _fallback_command_title(goal, workflow_mode, locale=locale)


def generate_command_title(
    *,
    goal: str,
    workflow_mode: WorkflowMode,
    workspace_root: str | Path,
    runbook_path: Optional[str | Path] = None,
    backend: Optional[RuntimeBackend] = None,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
    locale: Optional[str] = None,
) -> str:
    resolved_settings = resolve_settings(settings=settings, repo_root=repo_root)
    resolved_root = normalized_workspace_root(workspace_root)
    resolved_runbook_path: Optional[str] = None
    if runbook_path and str(runbook_path).strip():
        resolved_runbook_path = normalized_runbook_path(runbook_path, resolved_root)
    planner_role = resolved_settings.role_for("planner")
    runtime = backend or planner_role.runtime
    model = planner_role.model if runtime == planner_role.runtime else ""
    if runtime == RuntimeBackend.MOCK:
        return _mock_command_title(goal, workflow_mode, locale=locale)
    try:
        return _structured_command_title(
            runtime=runtime,
            model=model,
            settings=resolved_settings,
            goal=goal,
            workflow_mode=workflow_mode,
            workspace_root=resolved_root,
            runbook_path=resolved_runbook_path,
            locale=locale,
        )
    except Exception:
        return _fallback_command_title(goal, workflow_mode, locale=locale)


def _mock_job_split_preview(
    goal: str,
    workspace_root: str,
    runbook_path: Optional[str],
    *,
    locale: Optional[str] = None,
) -> JobSplitPreview:
    output_locale = _normalized_output_locale(locale)
    lowered = goal.lower()
    if "docs" in lowered or "document" in lowered:
        if output_locale == "ja":
            draft = JobSplitDraftOutput(
                summary="実装、文書更新、最終確認の3つに分けて進めます。",
                items=[
                    JobSplitDraftItem(
                        key="implement",
                        title="コード変更を実装する",
                        goal=f"メインの作業ディレクトリで、依頼内容に沿った実装変更を行う: {goal}",
                        workflow_mode=WorkflowMode.IMPLEMENTATION,
                        workspace_path=".",
                        rationale="主となる実装作業。",
                    ),
                    JobSplitDraftItem(
                        key="docs",
                        title="仕様書と案内を更新する",
                        goal=f"依頼内容に合わせて、関連する仕様書や利用案内を更新する: {goal}",
                        workflow_mode=WorkflowMode.PLANNING,
                        workspace_path="docs",
                        allow_parallel=True,
                        rationale="文書更新は別の workspace で独立して進められる。",
                    ),
                    JobSplitDraftItem(
                        key="verify",
                        title="変更結果をレビューする",
                        goal=f"実装変更と文書更新をまとめて確認し、仕上がりをレビューする: {goal}",
                        workflow_mode=WorkflowMode.REVIEW,
                        depends_on=["implement", "docs"],
                        workspace_path=".",
                        rationale="並行作業がそろった後の最終確認。",
                    ),
                ],
            )
        else:
            draft = JobSplitDraftOutput(
                summary="Split the work into implementation, documentation, and final verification.",
                items=[
                    JobSplitDraftItem(
                        key="implement",
                        title="Implement the code changes",
                        goal=f"Apply the requested code changes in the main workspace for: {goal}",
                        workflow_mode=WorkflowMode.IMPLEMENTATION,
                        workspace_path=".",
                        rationale="Primary implementation pass.",
                    ),
                    JobSplitDraftItem(
                        key="docs",
                        title="Update docs and usage notes",
                        goal=(
                            "Update the related documentation and usage notes so they match the requested "
                            f"change: {goal}"
                        ),
                        workflow_mode=WorkflowMode.PLANNING,
                        workspace_path="docs",
                        allow_parallel=True,
                        rationale="Documentation can run independently in a separate workspace path.",
                    ),
                    JobSplitDraftItem(
                        key="verify",
                        title="Review the completed changes",
                        goal=(
                            "Review the finished implementation and documentation updates together for "
                            f"the requested change: {goal}"
                        ),
                        workflow_mode=WorkflowMode.REVIEW,
                        depends_on=["implement", "docs"],
                        workspace_path=".",
                        rationale="Final verification after the parallel work completes.",
                    ),
                ],
            )
    else:
        if output_locale == "ja":
            draft = JobSplitDraftOutput(
                summary="計画、実装、確認の3段階に分けて進めます。",
                items=[
                    JobSplitDraftItem(
                        key="prepare",
                        title="実装計画を作る",
                        goal=f"現状を確認し、この依頼を進めるための具体的な計画を作成する: {goal}",
                        workflow_mode=WorkflowMode.PLANNING,
                        workspace_path=".",
                        rationale="実装前に進め方を固める。",
                    ),
                    JobSplitDraftItem(
                        key="implement",
                        title="変更を実装する",
                        goal=f"作成した計画をもとに、依頼された変更を workspace に反映する: {goal}",
                        workflow_mode=WorkflowMode.IMPLEMENTATION,
                        depends_on=["prepare"],
                        workspace_path=".",
                        rationale="主となる実装作業。",
                        runbook_path=runbook_path,
                    ),
                    JobSplitDraftItem(
                        key="verify",
                        title="変更結果をレビューする",
                        goal=f"実装後の変更内容を確認し、依頼内容どおりに仕上がっているか検証する: {goal}",
                        workflow_mode=WorkflowMode.REVIEW,
                        depends_on=["implement"],
                        workspace_path=".",
                        rationale="実装後の最終確認。",
                    ),
                ],
            )
        else:
            draft = JobSplitDraftOutput(
                summary="Split the request into preparation, execution, and verification jobs.",
                items=[
                    JobSplitDraftItem(
                        key="prepare",
                        title="Create the implementation plan",
                        goal=(
                            "Inspect the current state and produce a concrete implementation plan for: "
                            f"{goal}"
                        ),
                        workflow_mode=WorkflowMode.PLANNING,
                        workspace_path=".",
                        rationale="Establish the plan before making changes.",
                    ),
                    JobSplitDraftItem(
                        key="implement",
                        title="Apply the requested changes",
                        goal=(
                            "Apply the requested change in the workspace based on the approved plan: "
                            f"{goal}"
                        ),
                        workflow_mode=WorkflowMode.IMPLEMENTATION,
                        depends_on=["prepare"],
                        workspace_path=".",
                        rationale="Main implementation pass.",
                        runbook_path=runbook_path,
                    ),
                    JobSplitDraftItem(
                        key="verify",
                        title="Review the completed implementation",
                        goal=(
                            "Review and validate the completed change after implementation for: "
                            f"{goal}"
                        ),
                        workflow_mode=WorkflowMode.REVIEW,
                        depends_on=["implement"],
                        workspace_path=".",
                        rationale="Confirm the final state after implementation.",
                    ),
                ],
            )
    return normalize_job_split_preview(draft, workspace_root)


def _split_preview_prompt(
    goal: str,
    workspace_root: str,
    runbook_path: Optional[str],
    *,
    locale: Optional[str] = None,
) -> str:
    runbook_block = ""
    if runbook_path:
        runbook_block = f"Runbook path: {runbook_path}\n"
    output_language = "Japanese" if _normalized_output_locale(locale) == "ja" else "English"
    return (
        "You are the Wevra job splitter.\n"
        "Create a small set of top-level jobs, not internal tasks.\n"
        "These jobs will be shown separately in the dashboard and may depend on one another.\n"
        "Return only JSON that matches the schema.\n"
        "Prefer 2 to 4 jobs.\n"
        "Use workflow modes from: implementation, research, review, planning, dogfooding.\n"
        "Each item must include a stable key, title, goal, workflow_mode, workspace_path, depends_on, allow_parallel, and optional rationale.\n"
        "Write titles as concise dashboard labels that describe the actual deliverable, artifact, or phase.\n"
        "Write each goal as a self-contained job brief that tells the worker exactly what to produce.\n"
        "Avoid vague titles or goals such as 'prepare the approach', 'implement the change', 'verify the result', or '実装を進める' unless they are qualified with the actual subject of the work.\n"
        "Downstream jobs must mention what they are building on or checking. Example: 'Update the README and CLI help text to match the new flag names.'\n"
        "For planning jobs, state what plan or specification should be produced. For review jobs, state what should be reviewed and why.\n"
        "workspace_path must be relative to the base working directory. Use '.' when the job should run in the base working directory.\n"
        "Only mark allow_parallel=true when the job has no dependencies and can safely run in a workspace path that does not overlap other preview jobs.\n"
        "Do not split into tiny jobs. Split only at natural boundaries that are worth tracking independently.\n"
        "If the request clearly contains setup, implementation, and verification phases, reflect that.\n"
        f"Write the summary, titles, goals, and rationale in {output_language}.\n"
        f"Base working directory: {workspace_root}\n"
        f"{runbook_block}"
        f"Job brief:\n{goal}"
    )


def _structured_job_split_preview(
    *,
    runtime: RuntimeBackend,
    model: str,
    settings: AppConfig,
    goal: str,
    workspace_root: str,
    runbook_path: Optional[str],
    locale: Optional[str],
) -> JobSplitPreview:
    backend = StructuredCliBackend(
        runtime,
        model,
        ApprovalMode.AUTO,
        settings.agent_timeout_seconds,
        runtime_home=settings.runtime_home,
    )
    payload = backend._run_structured(  # noqa: SLF001
        _split_preview_prompt(goal, workspace_root, runbook_path, locale=locale),
        JobSplitDraftOutput.model_json_schema(),
        workspace_root,
    )
    draft = JobSplitDraftOutput.model_validate(payload)
    return normalize_job_split_preview(draft, workspace_root)


def generate_job_split_preview(
    *,
    goal: str,
    workspace_root: str | Path,
    runbook_path: Optional[str | Path] = None,
    backend: Optional[RuntimeBackend] = None,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
    locale: Optional[str] = None,
) -> JobSplitPreview:
    resolved_settings = resolve_settings(settings=settings, repo_root=repo_root)
    resolved_root = normalized_workspace_root(workspace_root)
    resolved_runbook_path: Optional[str] = None
    if runbook_path and str(runbook_path).strip():
        resolved_runbook_path = normalized_runbook_path(runbook_path, resolved_root)
    planner_role = resolved_settings.role_for("planner")
    runtime = backend or planner_role.runtime
    model = planner_role.model if runtime == planner_role.runtime else ""
    if runtime == RuntimeBackend.MOCK:
        return _mock_job_split_preview(
            goal,
            resolved_root,
            resolved_runbook_path,
            locale=locale,
        )
    return _structured_job_split_preview(
        runtime=runtime,
        model=model,
        settings=resolved_settings,
        goal=goal,
        workspace_root=resolved_root,
        runbook_path=resolved_runbook_path,
        locale=locale,
    )


def _topologically_order_preview_items(
    items: Sequence[JobSplitPreviewItem],
) -> List[JobSplitPreviewItem]:
    pending = {item.key: item for item in items}
    resolved: List[JobSplitPreviewItem] = []
    while pending:
        ready = [
            item
            for item in pending.values()
            if all(
                dependency in {resolved_item.key for resolved_item in resolved}
                for dependency in item.depends_on
            )
        ]
        if not ready:
            raise ValueError("job_split_preview_cycle")
        ready.sort(key=lambda item: item.key)
        for item in ready:
            resolved.append(item)
            pending.pop(item.key, None)
    return resolved


def submit_job_split_preview(
    db_path: Path,
    *,
    preview: JobSplitPreview,
    approval_mode: ApprovalMode,
    priority: Priority,
    backend: Optional[RuntimeBackend] = None,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
) -> List[CommandRecord]:
    created: Dict[str, CommandRecord] = {}
    ordered = _topologically_order_preview_items(preview.items)
    for item in ordered:
        command = submit_command(
            db_path,
            title=item.title,
            goal=item.goal,
            workflow_mode=item.workflow_mode,
            priority=priority,
            approval_mode=approval_mode,
            backend=backend,
            workspace_root=item.workspace_root,
            runbook_path=item.runbook_path,
            depends_on_command_ids=[created[dependency].id for dependency in item.depends_on],
            allow_parallel=item.allow_parallel,
            settings=settings,
            repo_root=repo_root,
        )
        created[item.key] = command
    return [created[item.key] for item in ordered]


def build_final_response(
    command: CommandRecord, tasks: List[TaskRecord], reviews: List[ReviewRecord]
) -> str:
    mode = effective_mode(command).value
    lines = [f"Goal: {command.goal}", f"Mode: {mode}", "", "Completed tasks:"]
    if command.runbook_path:
        lines.insert(2, f"Runbook Path: {command.runbook_path}")
    completed_tasks = [task for task in tasks if task.state == TaskState.DONE]
    for task in completed_tasks:
        summary = "completed"
        if task.output_payload and task.output_payload.get("summary"):
            summary = task.output_payload["summary"]
        lines.append(f"- {task.title}: {summary}")
    if reviews:
        lines.append("")
        lines.append("Final reviews:")
        for review in reviews:
            lines.append(f"- reviewer {review.reviewer_slot}: {review.summary}")
    return "\n".join(lines)


def parse_markdown_sections(text: str) -> List[Tuple[str, str]]:
    stripped = (text or "").strip()
    if not stripped:
        return []
    sections: List[Tuple[str, str]] = []
    current_title: Optional[str] = None
    current_lines: List[str] = []
    intro_lines: List[str] = []
    for line in stripped.splitlines():
        match = re.match(r"^\s{0,3}#{1,6}\s+(.+?)\s*$", line)
        if match:
            if current_title is not None:
                sections.append((current_title, "\n".join(current_lines).strip()))
            elif intro_lines:
                sections.append(("Result", "\n".join(intro_lines).strip()))
                intro_lines = []
            current_title = match.group(1).strip()
            current_lines = []
            continue
        if current_title is None:
            intro_lines.append(line)
        else:
            current_lines.append(line)
    if current_title is not None:
        sections.append((current_title, "\n".join(current_lines).strip()))
    elif intro_lines:
        sections.append(("Result", "\n".join(intro_lines).strip()))
    return [(title, body.strip()) for title, body in sections if title or body.strip()]


def normalize_result_section_key(title: str) -> Optional[str]:
    normalized = re.sub(r"\s+", " ", title.strip().lower())
    for key, aliases in PLANNING_SECTION_ALIASES.items():
        if normalized in aliases:
            return key
    return None


def section_title_map(key: str) -> Dict[str, str]:
    return RESULT_SECTION_TITLES.get(key, {"en": key.replace("_", " ").title(), "ja": key})


def build_result_section_body(_title: str, body: str) -> str:
    rendered_body = body.strip() or "- No content."
    return f"{rendered_body}\n"


def build_task_breakdown_markdown(tasks: List[TaskRecord]) -> str:
    ordered_tasks = sorted(tasks, key=lambda task: (task.plan_order, task.created_at))
    if not ordered_tasks:
        return "- No tasks were recorded."
    lines = []
    for index, task in enumerate(ordered_tasks, start=1):
        summary = (
            task.output_payload.get("summary")
            if task.output_payload and task.output_payload.get("summary")
            else task.description
        )
        lines.append(f"{index}. **{task.title}**")
        if summary:
            lines.append(f"   - {summary}")
        if task.depends_on:
            lines.append(f"   - Depends on: {', '.join(task.depends_on)}")
    return "\n".join(lines)


def build_plan_markdown(command: CommandRecord, tasks: List[TaskRecord], fallback_text: str) -> str:
    ordered_tasks = sorted(tasks, key=lambda task: (task.plan_order, task.created_at))
    lines = [f"- Goal: {command.goal}"]
    if ordered_tasks:
        lines.append(f"- Primary planning task: {ordered_tasks[0].title}")
        if len(ordered_tasks) > 1:
            lines.append("- Scope:")
            for task in ordered_tasks[:4]:
                lines.append(f"  - {task.title}")
        else:
            lines.append("- Output: return a structured plan before implementation.")
        return "\n".join(lines)
    stripped = fallback_text.strip()
    if stripped:
        lines.append(f"- Notes: {stripped}")
    else:
        lines.append("- Output: return a structured plan before implementation.")
    return "\n".join(lines)


def build_design_direction_markdown(tasks: List[TaskRecord], fallback_text: str) -> str:
    completed_tasks = [task for task in tasks if task.state == TaskState.DONE]
    lines = []
    for task in completed_tasks:
        summary = (
            task.output_payload.get("summary")
            if task.output_payload and task.output_payload.get("summary")
            else task.title
        )
        lines.append(f"- {summary}")
    if lines:
        return "\n".join(lines)
    stripped = fallback_text.strip()
    return stripped or "- No design direction was recorded."


def build_result_sections(
    command: CommandRecord,
    final_response: str,
    tasks: List[TaskRecord],
    reviews: List[ReviewRecord],
) -> List[Dict[str, str]]:
    mode = effective_mode(command)
    body = (final_response or "").strip() or build_final_response(command, tasks, reviews)
    parsed_sections = parse_markdown_sections(body)

    if mode == WorkflowMode.PLANNING:
        parsed_map: Dict[str, str] = {}
        for title, section_body in parsed_sections:
            key = normalize_result_section_key(title)
            if key and section_body.strip() and key not in parsed_map:
                parsed_map[key] = section_body.strip()
        plan_body = parsed_map.get("plan") or build_plan_markdown(command, tasks, body)
        design_body = parsed_map.get("design_direction") or build_design_direction_markdown(
            tasks, body
        )
        breakdown_body = parsed_map.get("task_breakdown") or build_task_breakdown_markdown(tasks)
        ordered_sections = [
            ("plan", plan_body),
            ("design_direction", design_body),
            ("task_breakdown", breakdown_body),
        ]
        return [
            {
                "key": key,
                "body": build_result_section_body(section_title_map(key)["en"], section_body),
                "title_en": section_title_map(key)["en"],
                "title_ja": section_title_map(key)["ja"],
            }
            for key, section_body in ordered_sections
        ]

    if len(parsed_sections) >= 2:
        sections: List[Dict[str, str]] = []
        for index, (title, section_body) in enumerate(parsed_sections, start=1):
            key = normalize_result_section_key(title) or f"section_{index}"
            titles = section_title_map(key)
            sections.append(
                {
                    "key": key,
                    "body": build_result_section_body(title.strip() or titles["en"], section_body),
                    "title_en": title.strip() or titles["en"],
                    "title_ja": title.strip() or titles["ja"],
                }
            )
        return sections

    titles = section_title_map("result")
    return [
        {
            "key": "result",
            "body": build_result_section_body(titles["en"], body),
            "title_en": titles["en"],
            "title_ja": titles["ja"],
        }
    ]


def replace_result_artifacts(
    conn,
    command: CommandRecord,
    final_response: str,
    tasks: List[TaskRecord],
    reviews: List[ReviewRecord],
) -> List[ArtifactRecord]:
    now = utc_now()
    sections = build_result_sections(command, final_response, tasks, reviews)
    conn.execute(
        "DELETE FROM artifacts WHERE command_id = ? AND kind = ?",
        (command.id, RESULT_ARTIFACT_KIND),
    )
    created: List[ArtifactRecord] = []
    for index, section in enumerate(sections, start=1):
        artifact_id = new_id("artifact")
        uri = f"result://{command.id}/{section['key']}.md"
        metadata = {
            "section_key": section["key"],
            "index": index,
            "title_en": section["title_en"],
            "title_ja": section["title_ja"],
            "format": "markdown",
            "extension": "md",
        }
        conn.execute(
            """
            INSERT INTO artifacts (id, command_id, task_id, kind, uri, metadata_json, body, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                artifact_id,
                command.id,
                None,
                RESULT_ARTIFACT_KIND,
                uri,
                json.dumps(metadata, sort_keys=True),
                section["body"],
                now,
            ),
        )
        row = conn.execute("SELECT * FROM artifacts WHERE id = ?", (artifact_id,)).fetchone()
        created.append(artifact_from_row(row))
    return created


def _resolve_dependency_target(
    dep_ref: str,
    existing_key_map: Dict[str, str],
    created_key_map: Dict[str, str],
) -> Optional[str]:
    if dep_ref in created_key_map:
        return created_key_map[dep_ref]
    if dep_ref in existing_key_map:
        return existing_key_map[dep_ref]
    if dep_ref.startswith("task_"):
        return dep_ref
    return None


def create_task_records(
    conn, command: CommandRecord, task_specs: Iterable[PlannerTaskSpec]
) -> List[TaskRecord]:
    specs = list(task_specs)
    if not specs:
        return []

    existing_tasks = list_tasks_for_command(conn, command.id)
    existing_key_map = {task.task_key: task.id for task in existing_tasks if task.task_key}
    existing_by_key = {task.task_key: task for task in existing_tasks if task.task_key}

    seen_keys: set[str] = set()
    planned_key_map: Dict[str, str] = {}
    planned_ids: List[str] = []
    now = utc_now()

    for offset, spec in enumerate(specs, start=1):
        if not spec.key.strip():
            raise ValueError("Planner task key must not be blank.")
        if spec.key in seen_keys:
            raise ValueError(f"Duplicate planner task key: {spec.key}")
        seen_keys.add(spec.key)

        payload = dict(spec.input_payload)
        payload["write_files"] = spec.write_files
        existing_task = existing_by_key.get(spec.key)
        if existing_task is None:
            task_id = new_id("task")
            conn.execute(
                """
                INSERT INTO tasks (
                    id, command_id, task_key, kind, capability, title, description, state, plan_order,
                    input_payload, output_payload, error, attempt_count, assigned_run_id, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    command.id,
                    spec.key,
                    spec.kind,
                    spec.capability,
                    spec.title,
                    spec.description,
                    TaskState.PENDING.value,
                    offset,
                    json.dumps(payload, sort_keys=True),
                    None,
                    None,
                    0,
                    None,
                    now,
                    now,
                ),
            )
            append_event(
                conn,
                "task",
                task_id,
                "task_created",
                {
                    "command_id": command.id,
                    "task_key": spec.key,
                    "kind": spec.kind,
                    "capability": spec.capability,
                    "title": spec.title,
                },
            )
        else:
            rerun_completed_gate = (
                command.stage == CommandStage.REPLANNING and existing_task.capability == "tester"
            )
            update_fields = {
                "kind": spec.kind,
                "capability": spec.capability,
                "title": spec.title,
                "description": spec.description,
                "plan_order": offset,
                "input_payload": json.dumps(payload, sort_keys=True),
                "assigned_run_id": None,
                "operator_issue_kind": None,
            }
            if existing_task.state != TaskState.DONE or rerun_completed_gate:
                update_fields.update(
                    {
                        "state": TaskState.PENDING.value,
                        "output_payload": None,
                        "error": None,
                    }
                )
            update_task(conn, existing_task.id, **update_fields)
            append_event(
                conn,
                "task",
                existing_task.id,
                "task_replanned",
                {
                    "command_id": command.id,
                    "task_key": spec.key,
                },
            )
            task_id = existing_task.id

        planned_key_map[spec.key] = task_id
        planned_ids.append(task_id)

    conn.execute(
        """
        DELETE FROM task_dependencies
        WHERE task_id IN (
            SELECT id FROM tasks WHERE command_id = ?
        )
        """,
        (command.id,),
    )

    for spec in specs:
        task_id = planned_key_map[spec.key]
        for dep_ref in spec.depends_on:
            dependency_id = _resolve_dependency_target(dep_ref, existing_key_map, planned_key_map)
            if dependency_id is None:
                raise ValueError(f"Unknown dependency '{dep_ref}' for task '{spec.key}'")
            conn.execute(
                """
                INSERT OR IGNORE INTO task_dependencies (task_id, depends_on_task_id)
                VALUES (?, ?)
                """,
                (task_id, dependency_id),
            )

    for task in existing_tasks:
        if task.task_key and task.task_key in seen_keys:
            continue
        if task.state in {
            TaskState.PENDING,
            TaskState.BLOCKED,
            TaskState.FAILED,
            TaskState.CANCELED,
        }:
            update_task(
                conn,
                task.id,
                state=TaskState.CANCELED.value,
                error="Superseded by replanning.",
                assigned_run_id=None,
                operator_issue_kind=None,
            )
            append_event(
                conn,
                "task",
                task.id,
                "task_superseded",
                {"command_id": command.id, "task_key": task.task_key},
            )

    rows = conn.execute(
        f"SELECT * FROM tasks WHERE id IN ({','.join('?' for _ in planned_ids)}) ORDER BY plan_order ASC",
        planned_ids,
    ).fetchall()
    return attach_dependencies(
        [task_from_row(row) for row in rows], dependency_map_for_command(conn, command.id)
    )


def create_question_record(
    conn,
    command: CommandRecord,
    source: str,
    resolution_mode: QuestionResolutionMode,
    resume_stage: CommandStage,
    question: str,
    task_id: Optional[str] = None,
) -> QuestionRecord:
    question_id = new_id("question")
    now = utc_now()
    conn.execute(
        """
        INSERT INTO questions (
            id, command_id, task_id, source, resolution_mode, resume_stage,
            question, answer, state, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            question_id,
            command.id,
            task_id,
            source,
            resolution_mode.value,
            resume_stage.value,
            question,
            None,
            QuestionState.OPEN.value,
            now,
            now,
        ),
    )
    append_event(
        conn,
        "question",
        question_id,
        "question_created",
        {
            "command_id": command.id,
            "task_id": task_id,
            "source": source,
            "resolution_mode": resolution_mode.value,
            "resume_stage": resume_stage.value,
        },
    )
    row = conn.execute("SELECT * FROM questions WHERE id = ?", (question_id,)).fetchone()
    return question_from_row(row)


def create_review_record(
    conn,
    command: CommandRecord,
    review_output: ReviewerOutput,
    reviewer_slot: int,
    reviewer_kind: str,
    task_id: Optional[str] = None,
) -> ReviewRecord:
    review_id = new_id("review")
    now = utc_now()
    conn.execute(
        """
        INSERT INTO reviews (
            id, command_id, task_id, reviewer_kind, reviewer_slot, decision, summary, findings_json, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            review_id,
            command.id,
            task_id,
            reviewer_kind,
            reviewer_slot,
            review_output.decision.value,
            review_output.summary,
            json.dumps(review_output.findings, sort_keys=True),
            now,
            now,
        ),
    )
    append_event(
        conn,
        "review",
        review_id,
        "review_recorded",
        {
            "command_id": command.id,
            "decision": review_output.decision.value,
            "reviewer_slot": reviewer_slot,
        },
    )
    row = conn.execute("SELECT * FROM reviews WHERE id = ?", (review_id,)).fetchone()
    return review_from_row(row)


def submit_command(
    db_path: Path,
    goal: str,
    workflow_mode: WorkflowMode,
    priority: Priority,
    title: Optional[str] = None,
    approval_mode: ApprovalMode = ApprovalMode.AUTO,
    backend: Optional[RuntimeBackend] = None,
    workspace_root: Optional[Path] = None,
    runbook_path: Optional[str | Path] = None,
    depends_on_command_ids: Sequence[str] = (),
    allow_parallel: bool = False,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
    locale: Optional[str] = None,
) -> CommandRecord:
    settings = resolve_settings(settings=settings, repo_root=repo_root)
    initialize_database(db_path)
    if workspace_root is None:
        raise ValueError("workspace_root_required")
    resolved_root = normalized_workspace_root(workspace_root)
    resolved_runbook_path: Optional[str] = None
    if workflow_mode == WorkflowMode.DOGFOODING:
        if runbook_path is None or not str(runbook_path).strip():
            raise ValueError("runbook_path_required")
        resolved_runbook_path = normalized_runbook_path(runbook_path, resolved_root)
        if not Path(resolved_runbook_path).exists():
            raise ValueError("runbook_path_not_found")
    elif runbook_path and str(runbook_path).strip():
        resolved_runbook_path = normalized_runbook_path(runbook_path, resolved_root)
    selected_backend = backend or RuntimeBackend.INHERIT
    resolved_title = (title or "").strip() or generate_command_title(
        goal=goal,
        workflow_mode=workflow_mode,
        workspace_root=resolved_root,
        runbook_path=resolved_runbook_path,
        backend=selected_backend if selected_backend != RuntimeBackend.INHERIT else None,
        settings=settings,
        repo_root=repo_root,
        locale=locale,
    )

    with connect(db_path) as conn:
        existing_rows = conn.execute("SELECT id FROM commands").fetchall()
        existing_ids = {row["id"] for row in existing_rows}
        unique_dependencies = []
        for dependency_id in depends_on_command_ids:
            if not dependency_id or dependency_id in unique_dependencies:
                continue
            if dependency_id not in existing_ids:
                raise ValueError(f"Unknown dependency command: {dependency_id}")
            unique_dependencies.append(dependency_id)
        if allow_parallel and not unique_dependencies:
            overlaps = nonterminal_workspace_overlap_commands(conn, resolved_root)
            if overlaps:
                raise ValueError("parallel_workspace_overlap")
        created = _insert_command_record(
            conn,
            title=resolved_title,
            goal=goal,
            workflow_mode=workflow_mode,
            approval_mode=approval_mode,
            priority=priority,
            backend=selected_backend,
            workspace_root=resolved_root,
            runbook_path=resolved_runbook_path,
            depends_on_command_ids=unique_dependencies,
            allow_parallel=allow_parallel,
        )
        conn.commit()
        rows = conn.execute("SELECT * FROM commands").fetchall()
        commands = [command_from_row(row) for row in rows]
        commands = attach_command_dependencies(commands, command_dependency_map(conn))
        commands = enrich_command_dependency_state(commands)
        return next(command for command in commands if command.id == created.id)


def create_instruction_record(conn, command_id: str, body: str) -> InstructionRecord:
    instruction_id = new_id("instruction")
    now = utc_now()
    conn.execute(
        """
        INSERT INTO instructions (id, command_id, body, created_at)
        VALUES (?, ?, ?, ?)
        """,
        (instruction_id, command_id, body, now),
    )
    append_event(
        conn,
        "instruction",
        instruction_id,
        "instruction_appended",
        {"command_id": command_id, "body": body},
    )
    row = conn.execute("SELECT * FROM instructions WHERE id = ?", (instruction_id,)).fetchone()
    return instruction_from_row(row)


def append_instruction(
    db_path: Path, command_id: str, body: str
) -> Tuple[InstructionRecord, CommandRecord]:
    initialize_database(db_path)
    instruction_body = body.strip()
    if not instruction_body:
        raise ValueError("Instruction body must not be blank.")

    with connect(db_path) as conn:
        command_row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if command_row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(command_row)
        if command.stage in {CommandStage.DONE, CommandStage.FAILED, CommandStage.CANCELED}:
            raise ValueError(f"Cannot append instructions to a terminal command: {command_id}")
        instruction = create_instruction_record(conn, command.id, instruction_body)

        if command.stage == CommandStage.WAITING_QUESTION:
            task_rows = conn.execute(
                "SELECT * FROM tasks WHERE command_id = ? ORDER BY created_at ASC",
                (command.id,),
            ).fetchall()
            for task_row in task_rows:
                task = task_from_row(task_row)
                if task.state == TaskState.DONE:
                    continue
                update_task(
                    conn,
                    task.id,
                    state=TaskState.CANCELED.value,
                    error="Superseded by appended instruction.",
                )
            question_rows = conn.execute(
                """
                SELECT * FROM questions
                WHERE command_id = ? AND state IN (?, ?)
                ORDER BY created_at ASC
                """,
                (command.id, QuestionState.OPEN.value, QuestionState.ANSWERED.value),
            ).fetchall()
            for row in question_rows:
                question = question_from_row(row)
                fields = {"state": QuestionState.RESOLVED.value}
                if question.state == QuestionState.OPEN:
                    fields["answer"] = "[superseded by appended instruction]"
                update_question(conn, question.id, **fields)

        if command.stage == CommandStage.WAITING_APPROVAL:
            conn.execute(
                """
                UPDATE agent_runs
                SET state = ?, error = ?, finished_at = ?, updated_at = ?
                WHERE command_id = ? AND state IN (?, ?)
                """,
                (
                    AgentRunState.DENIED.value,
                    "Superseded by appended instruction.",
                    utc_now(),
                    utc_now(),
                    command.id,
                    AgentRunState.PENDING_APPROVAL.value,
                    AgentRunState.APPROVED.value,
                ),
            )
            update_command(
                conn,
                command.id,
                stage=CommandStage.REPLANNING.value,
                question_state="none",
                replan_requested=0,
                final_response=None,
                failure_reason=None,
            )
            append_event(
                conn,
                "command",
                command.id,
                "replanning_requested",
                {
                    "source": "instruction_append",
                    "mode": "supersede_pending_approval",
                    "instruction_id": instruction.id,
                },
            )
        elif command.stage == CommandStage.WAITING_OPERATOR:
            update_command(
                conn,
                command.id,
                stage=CommandStage.REPLANNING.value,
                question_state="none",
                replan_requested=0,
                final_response=None,
                failure_reason=None,
                **clear_operator_issue_fields(),
            )
            conn.execute(
                """
                UPDATE tasks
                SET state = ?, error = ?, operator_issue_kind = NULL, assigned_run_id = NULL, updated_at = ?
                WHERE command_id = ? AND state = ? AND operator_issue_kind IS NOT NULL
                """,
                (
                    TaskState.CANCELED.value,
                    "Superseded by appended instruction.",
                    utc_now(),
                    command.id,
                    TaskState.BLOCKED.value,
                ),
            )
            append_event(
                conn,
                "command",
                command.id,
                "replanning_requested",
                {
                    "source": "instruction_append",
                    "mode": "supersede_operator_issue",
                    "instruction_id": instruction.id,
                },
            )
        elif command.stage in {CommandStage.RUNNING, CommandStage.VERIFYING}:
            update_command(
                conn,
                command.id,
                replan_requested=1,
                final_response=None,
                failure_reason=None,
                **clear_operator_issue_fields(),
            )
            append_event(
                conn,
                "command",
                command.id,
                "replanning_requested",
                {
                    "source": "instruction_append",
                    "mode": "after_active_batch",
                    "instruction_id": instruction.id,
                },
            )
        else:
            update_command(
                conn,
                command.id,
                stage=CommandStage.REPLANNING.value,
                question_state="none",
                replan_requested=0,
                final_response=None,
                failure_reason=None,
                **clear_operator_issue_fields(),
            )
            append_event(
                conn,
                "command",
                command.id,
                "replanning_requested",
                {
                    "source": "instruction_append",
                    "mode": "immediate",
                    "instruction_id": instruction.id,
                },
            )

        conn.commit()
        refreshed_row = conn.execute(
            "SELECT * FROM commands WHERE id = ?", (command_id,)
        ).fetchone()
        return instruction, command_from_row(refreshed_row)


def pause_command_record(
    conn, command: CommandRecord, *, note: Optional[str] = None
) -> CommandRecord:
    resume_stage = command.resume_stage or command.stage
    update_command(
        conn,
        command.id,
        stage=CommandStage.PAUSED.value,
        resume_stage=resume_stage.value,
        stop_requested=0,
    )
    append_event(
        conn,
        "command",
        command.id,
        "command_paused",
        {
            "from_stage": command.stage.value,
            "resume_stage": resume_stage.value,
            "note": note,
        },
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return command_from_row(row)


def request_command_stop(db_path: Path, command_id: str) -> CommandRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage in {CommandStage.DONE, CommandStage.FAILED, CommandStage.CANCELED}:
            raise ValueError(f"Cannot stop a terminal command: {command_id}")
        if command.stage == CommandStage.PAUSED:
            return command

        if command.stage in {
            CommandStage.QUEUED,
            CommandStage.WAITING_QUESTION,
            CommandStage.WAITING_APPROVAL,
            CommandStage.WAITING_OPERATOR,
        }:
            paused = pause_command_record(conn, command, note="paused by operator")
            conn.commit()
            return paused

        update_command(
            conn,
            command.id,
            stop_requested=1,
            resume_stage=command.stage.value,
        )
        append_event(
            conn,
            "command",
            command.id,
            "command_stop_requested",
            {"stage": command.stage.value},
        )
        conn.commit()
        return get_command(db_path, command.id)


def resume_command(db_path: Path, command_id: str) -> CommandRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage != CommandStage.PAUSED:
            raise ValueError(f"Command is not paused: {command_id}")

        resume_stage = command.resume_stage or CommandStage.PLANNING
        update_command(
            conn,
            command.id,
            stage=resume_stage.value,
            resume_stage=None,
            stop_requested=0,
        )
        append_event(
            conn,
            "command",
            command.id,
            "command_resumed",
            {"resume_stage": resume_stage.value},
        )
        conn.commit()
        return get_command(db_path, command.id)


def ignore_command_dependencies(db_path: Path, command_id: str) -> CommandRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage in {CommandStage.DONE, CommandStage.FAILED, CommandStage.CANCELED}:
            raise ValueError(f"Cannot ignore dependencies for a terminal command: {command_id}")
        dep_ids = command_dependency_map(conn).get(command_id, [])
        if not dep_ids:
            return get_command(db_path, command_id)
        conn.execute("DELETE FROM command_dependencies WHERE command_id = ?", (command_id,))
        update_command(conn, command_id, allow_parallel=0)
        append_event(
            conn,
            "command",
            command_id,
            "command_dependencies_ignored",
            {"ignored_dependency_ids": dep_ids},
        )
        conn.commit()
        return get_command(db_path, command_id)


def _insert_command_record(
    conn,
    *,
    title: str,
    goal: str,
    workflow_mode: WorkflowMode,
    approval_mode: ApprovalMode,
    priority: Priority,
    backend: RuntimeBackend,
    workspace_root: str,
    runbook_path: Optional[str],
    depends_on_command_ids: Sequence[str],
    allow_parallel: bool,
) -> CommandRecord:
    command_id = new_id("cmd")
    now = utc_now()
    conn.execute(
        """
        INSERT INTO commands (
            id, title, goal, stage, workflow_mode, approval_mode, effective_mode, priority, backend, workspace_root, runbook_path, allow_parallel,
            resume_stage, resume_hint, final_response, failure_reason, operator_issue_kind, operator_issue_detail,
            operator_issue_agent_run_id, operator_issue_task_id, operator_issue_role_name, operator_issue_runtime,
            operator_issue_model, question_state, planning_attempts, run_count, replan_requested, stop_requested,
            version, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command_id,
            title,
            goal,
            CommandStage.QUEUED.value,
            workflow_mode.value,
            approval_mode.value,
            None if workflow_mode == WorkflowMode.AUTO else workflow_mode.value,
            priority.value,
            backend.value,
            workspace_root,
            runbook_path,
            int(bool(allow_parallel and not depends_on_command_ids)),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            "none",
            0,
            0,
            0,
            0,
            1,
            now,
            now,
        ),
    )
    if depends_on_command_ids:
        conn.executemany(
            """
            INSERT INTO command_dependencies (command_id, depends_on_command_id)
            VALUES (?, ?)
            """,
            [(command_id, dependency_id) for dependency_id in depends_on_command_ids],
        )
    append_event(
        conn,
        "command",
        command_id,
        "command_submitted",
        {
            "title": title,
            "goal": goal,
            "workflow_mode": workflow_mode.value,
            "approval_mode": approval_mode.value,
            "priority": priority.value,
            "backend": backend.value,
            "workspace_root": workspace_root,
            "runbook_path": runbook_path,
            "allow_parallel": bool(allow_parallel and not depends_on_command_ids),
            "depends_on_command_ids": list(depends_on_command_ids),
        },
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
    return command_from_row(row)


def retry_operator_issue(
    db_path: Path,
    command_id: str,
    *,
    backend_override: Optional[RuntimeBackend] = None,
) -> CommandRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage != CommandStage.WAITING_OPERATOR:
            raise ValueError(f"Command is not waiting for operator action: {command_id}")
        if command.operator_issue_kind is None:
            raise ValueError(f"No recoverable operator issue is recorded for: {command_id}")

        target_backend = backend_override or command.backend
        role_name = command.operator_issue_role_name or "agent"
        issue_kind = command.operator_issue_kind
        if command.operator_issue_task_id:
            conn.execute(
                """
                UPDATE tasks
                SET state = ?, error = NULL, operator_issue_kind = NULL, assigned_run_id = NULL, updated_at = ?
                WHERE command_id = ? AND state = ? AND operator_issue_kind IS NOT NULL
                """,
                (
                    TaskState.PENDING.value,
                    utc_now(),
                    command_id,
                    TaskState.BLOCKED.value,
                ),
            )

        next_stage = command.resume_stage or CommandStage.RUNNING
        unresolved_question = conn.execute(
            """
            SELECT 1 FROM questions
            WHERE command_id = ? AND state IN (?, ?)
            LIMIT 1
            """,
            (
                command_id,
                QuestionState.OPEN.value,
                QuestionState.ANSWERED.value,
            ),
        ).fetchone()
        if unresolved_question is not None:
            next_stage = CommandStage.WAITING_QUESTION

        cleared_fields = clear_operator_issue_fields()
        cleared_fields["resume_hint"] = build_operator_resume_hint(
            issue_kind,
            runtime=command.operator_issue_runtime or command.backend,
            title=command.goal,
            backend_override=backend_override,
        )
        update_command(
            conn,
            command.id,
            stage=next_stage.value,
            resume_stage=None,
            backend=target_backend.value,
            failure_reason=None,
            **cleared_fields,
        )
        append_event(
            conn,
            "command",
            command.id,
            "operator_issue_retry_requested",
            {
                "kind": issue_kind.value,
                "role_name": role_name,
                "backend": target_backend.value,
                "switched_backend": backend_override is not None,
            },
        )
        conn.commit()
        return get_command(db_path, command_id)


def cancel_command_with_repair(
    db_path: Path,
    command_id: str,
    *,
    repair_goal: str,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
) -> tuple[CommandRecord, CommandRecord]:
    settings = resolve_settings(settings=settings, repo_root=repo_root)
    initialize_database(db_path)
    repair_goal = repair_goal.strip()
    if not repair_goal:
        raise ValueError("repair_goal_required")
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage in {CommandStage.DONE, CommandStage.FAILED, CommandStage.CANCELED}:
            raise ValueError(f"Cannot repair a terminal command: {command_id}")
        terminate_running_agent_runs_for_command(conn, command_id)

        conn.execute(
            """
            UPDATE tasks
            SET state = ?, error = COALESCE(error, ?), updated_at = ?
            WHERE command_id = ? AND state IN (?, ?, ?)
            """,
            (
                TaskState.CANCELED.value,
                "Canceled after interruption. Repair job created.",
                utc_now(),
                command_id,
                TaskState.PENDING.value,
                TaskState.RUNNING.value,
                TaskState.BLOCKED.value,
            ),
        )
        conn.execute(
            """
            UPDATE questions
            SET state = ?, updated_at = ?
            WHERE command_id = ? AND state IN (?, ?)
            """,
            (
                QuestionState.RESOLVED.value,
                utc_now(),
                command_id,
                QuestionState.OPEN.value,
                QuestionState.ANSWERED.value,
            ),
        )
        conn.execute(
            """
            UPDATE agent_runs
            SET state = ?, error = COALESCE(error, ?), finished_at = COALESCE(finished_at, ?), updated_at = ?
            WHERE command_id = ? AND state IN (?, ?, ?)
            """,
            (
                AgentRunState.DENIED.value,
                "Canceled after interruption. Repair job created.",
                utc_now(),
                utc_now(),
                command_id,
                AgentRunState.PENDING_APPROVAL.value,
                AgentRunState.APPROVED.value,
                AgentRunState.RUNNING.value,
            ),
        )
        update_command(
            conn,
            command_id,
            stage=CommandStage.CANCELED.value,
            resume_stage=None,
            stop_requested=0,
            final_response=None,
            failure_reason="Canceled after interruption. Repair job created.",
            **clear_operator_issue_fields(),
        )
        append_event(
            conn,
            "command",
            command_id,
            "command_canceled",
            {"reason": "Canceled after interruption. Repair job created."},
        )

        repair_command = _insert_command_record(
            conn,
            title=repair_goal,
            goal=repair_goal,
            workflow_mode=WorkflowMode.IMPLEMENTATION,
            approval_mode=command.approval_mode,
            priority=Priority.HIGH,
            backend=command.backend,
            workspace_root=command.workspace_root,
            runbook_path=None,
            depends_on_command_ids=[],
            allow_parallel=False,
        )
        append_event(
            conn,
            "command",
            repair_command.id,
            "repair_job_created",
            {"source_command_id": command.id},
        )
        conn.commit()
        return get_command(db_path, command_id), get_command(db_path, repair_command.id)


def cancel_command(db_path: Path, command_id: str, reason: Optional[str] = None) -> CommandRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        if row is None:
            raise ValueError(f"Unknown command: {command_id}")
        command = command_from_row(row)
        if command.stage in {CommandStage.DONE, CommandStage.FAILED, CommandStage.CANCELED}:
            raise ValueError(f"Cannot cancel a terminal command: {command_id}")
        terminate_running_agent_runs_for_command(conn, command_id)
        conn.execute(
            """
            UPDATE tasks
            SET state = ?, error = COALESCE(error, ?), updated_at = ?
            WHERE command_id = ? AND state IN (?, ?, ?)
            """,
            (
                TaskState.CANCELED.value,
                reason or "Canceled by operator.",
                utc_now(),
                command_id,
                TaskState.PENDING.value,
                TaskState.RUNNING.value,
                TaskState.BLOCKED.value,
            ),
        )
        conn.execute(
            """
            UPDATE questions
            SET state = ?, updated_at = ?
            WHERE command_id = ? AND state IN (?, ?)
            """,
            (
                QuestionState.RESOLVED.value,
                utc_now(),
                command_id,
                QuestionState.OPEN.value,
                QuestionState.ANSWERED.value,
            ),
        )
        conn.execute(
            """
            UPDATE agent_runs
            SET state = ?, error = COALESCE(error, ?), finished_at = COALESCE(finished_at, ?), updated_at = ?
            WHERE command_id = ? AND state IN (?, ?, ?)
            """,
            (
                AgentRunState.DENIED.value,
                reason or "Canceled by operator.",
                utc_now(),
                utc_now(),
                command_id,
                AgentRunState.PENDING_APPROVAL.value,
                AgentRunState.APPROVED.value,
                AgentRunState.RUNNING.value,
            ),
        )
        update_command(
            conn,
            command_id,
            stage=CommandStage.CANCELED.value,
            resume_stage=None,
            stop_requested=0,
            final_response=None,
            failure_reason=reason or "Canceled by operator.",
            **clear_operator_issue_fields(),
        )
        append_event(
            conn,
            "command",
            command_id,
            "command_canceled",
            {"reason": reason or "Canceled by operator."},
        )
        conn.commit()
        return get_command(db_path, command_id)


def get_command(db_path: Path, command_id: str) -> Optional[CommandRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM commands").fetchall()
        commands = [command_from_row(row) for row in rows]
        commands = attach_command_dependencies(commands, command_dependency_map(conn))
        commands = enrich_command_dependency_state(commands)
        return next((command for command in commands if command.id == command_id), None)


def clear_operator_issue_fields() -> dict[str, object]:
    return {
        "operator_issue_kind": None,
        "operator_issue_detail": None,
        "operator_issue_agent_run_id": None,
        "operator_issue_task_id": None,
        "operator_issue_role_name": None,
        "operator_issue_runtime": None,
        "operator_issue_model": None,
        "resume_hint": None,
    }


def set_command_operator_issue(
    conn,
    command: CommandRecord,
    *,
    issue: AgentExecutionError,
    agent_run: AgentRunRecord,
    task_id: Optional[str] = None,
    **extra_fields,
) -> CommandRecord:
    update_command(
        conn,
        command.id,
        stage=CommandStage.WAITING_OPERATOR.value,
        resume_stage=agent_run.resume_stage.value,
        question_state="none",
        final_response=None,
        failure_reason=None,
        operator_issue_kind=issue.kind.value,
        operator_issue_detail=issue.detail,
        operator_issue_agent_run_id=agent_run.id,
        operator_issue_task_id=task_id,
        operator_issue_role_name=agent_run.role_name,
        operator_issue_runtime=agent_run.runtime.value,
        operator_issue_model=agent_run.model,
        resume_hint=None,
        **extra_fields,
    )
    append_event(
        conn,
        "command",
        command.id,
        "operator_attention_required",
        {
            "kind": issue.kind.value,
            "agent_run_id": agent_run.id,
            "task_id": task_id,
            "role_name": agent_run.role_name,
            "runtime": agent_run.runtime.value,
            "model": agent_run.model,
            "resume_stage": agent_run.resume_stage.value,
            "detail": issue.detail,
        },
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return command_from_row(row)


def list_commands(db_path: Path) -> List[CommandRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM commands").fetchall()
        commands = [command_from_row(row) for row in rows]
        commands = attach_command_dependencies(commands, command_dependency_map(conn))
        commands = enrich_command_dependency_state(commands)
    return sorted(
        commands,
        key=lambda command: (
            PRIORITY_RANK[command.priority.value],
            STAGE_RANK[command.stage.value],
            command.created_at,
        ),
    )


def list_tasks(db_path: Path, command_id: Optional[str] = None) -> List[TaskRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        if command_id:
            return list_tasks_for_command(conn, command_id)
        rows = conn.execute(
            "SELECT * FROM tasks ORDER BY created_at ASC, plan_order ASC"
        ).fetchall()
        tasks = [task_from_row(row) for row in rows]
        dep_rows = conn.execute(
            "SELECT task_id, depends_on_task_id FROM task_dependencies"
        ).fetchall()
        dep_map: Dict[str, List[str]] = {}
        for row in dep_rows:
            dep_map.setdefault(row["task_id"], []).append(row["depends_on_task_id"])
        return attach_dependencies(tasks, dep_map)


def list_questions(
    db_path: Path,
    command_id: Optional[str] = None,
    open_only: bool = False,
) -> List[QuestionRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        query = "SELECT * FROM questions"
        clauses = []
        params: List[str] = []
        if command_id:
            clauses.append("command_id = ?")
            params.append(command_id)
        if open_only:
            clauses.append("state = ?")
            params.append(QuestionState.OPEN.value)
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY created_at ASC"
        rows = conn.execute(query, params).fetchall()
        return [question_from_row(row) for row in rows]


def list_reviews(db_path: Path, command_id: Optional[str] = None) -> List[ReviewRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        if command_id:
            return list_reviews_for_command(conn, command_id)
        rows = conn.execute(
            "SELECT * FROM reviews ORDER BY created_at ASC, reviewer_slot ASC"
        ).fetchall()
        return [review_from_row(row) for row in rows]


def list_instructions(db_path: Path, command_id: Optional[str] = None) -> List[InstructionRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        if command_id:
            return list_instructions_for_command(conn, command_id)
        rows = conn.execute("SELECT * FROM instructions ORDER BY created_at ASC").fetchall()
        return [instruction_from_row(row) for row in rows]


def list_artifacts(db_path: Path, command_id: Optional[str] = None) -> List[ArtifactRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        if command_id:
            return list_artifacts_for_command(conn, command_id)
        rows = conn.execute("SELECT * FROM artifacts ORDER BY created_at ASC").fetchall()
        return [artifact_from_row(row) for row in rows]


def list_events(db_path: Path, command_id: Optional[str] = None) -> List[EventRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM events ORDER BY id ASC").fetchall()
    events = [event_from_row(row) for row in rows]
    if not command_id:
        return events
    filtered = []
    for event in events:
        if event.stream_type == "command" and event.stream_id == command_id:
            filtered.append(event)
            continue
        if event.payload.get("command_id") == command_id:
            filtered.append(event)
    return filtered


def answered_question_ready(conn, command_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM questions
        WHERE command_id = ? AND state = ?
        LIMIT 1
        """,
        (command_id, QuestionState.ANSWERED.value),
    ).fetchone()
    return row is not None


def approved_agent_run_ready(conn, command_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM agent_runs
        WHERE command_id = ? AND state = ?
        LIMIT 1
        """,
        (command_id, AgentRunState.APPROVED.value),
    ).fetchone()
    return row is not None


def command_dependencies_ready(command: CommandRecord) -> bool:
    return command.dependency_state in {"none", "ready"}


def command_can_join_parallel_frontier(command: CommandRecord) -> bool:
    return command.allow_parallel and not command.depends_on


def nonterminal_workspace_overlap_commands(
    conn, workspace_root: str | Path, *, exclude_ids: Sequence[str] = ()
) -> List[CommandRecord]:
    excluded = {item for item in exclude_ids if item}
    rows = conn.execute(
        "SELECT * FROM commands WHERE stage NOT IN (?, ?, ?, ?)",
        (
            CommandStage.DONE.value,
            CommandStage.FAILED.value,
            CommandStage.PAUSED.value,
            CommandStage.CANCELED.value,
        ),
    ).fetchall()
    commands = [command_from_row(row) for row in rows]
    return [
        command
        for command in commands
        if command.id not in excluded
        and workspace_roots_overlap(command.workspace_root, workspace_root)
    ]


def command_is_blocked_by_operator(command: CommandRecord, conn) -> bool:
    if command.stage == CommandStage.WAITING_QUESTION and not answered_question_ready(
        conn, command.id
    ):
        return True
    if command.stage == CommandStage.WAITING_APPROVAL and not approved_agent_run_ready(
        conn, command.id
    ):
        return True
    if command.stage == CommandStage.WAITING_OPERATOR:
        return True
    return False


def build_parallel_frontier(commands: Sequence[CommandRecord]) -> List[CommandRecord]:
    if not commands:
        return []
    ordered = sorted(commands, key=command_order_key)
    head = ordered[0]
    frontier = [head]
    if not command_can_join_parallel_frontier(head):
        return frontier
    for command in ordered[1:]:
        if not command_can_join_parallel_frontier(command):
            break
        if any(
            workspace_roots_overlap(command.workspace_root, other.workspace_root)
            for other in frontier
        ):
            break
        frontier.append(command)
    return frontier


def select_actionable_command(conn, command_id: Optional[str]) -> Optional[CommandRecord]:
    if command_id:
        rows = conn.execute("SELECT * FROM commands").fetchall()
        commands = [command_from_row(row) for row in rows]
        commands = attach_command_dependencies(commands, command_dependency_map(conn))
        commands = enrich_command_dependency_state(commands)
        return next((command for command in commands if command.id == command_id), None)

    rows = conn.execute(
        "SELECT * FROM commands WHERE stage NOT IN (?, ?, ?, ?)",
        (
            CommandStage.DONE.value,
            CommandStage.FAILED.value,
            CommandStage.PAUSED.value,
            CommandStage.CANCELED.value,
        ),
    ).fetchall()
    commands = [command_from_row(row) for row in rows]
    commands = attach_command_dependencies(commands, command_dependency_map(conn))
    commands = enrich_command_dependency_state(commands)
    frontier = build_parallel_frontier(commands)
    actionable = []
    for command in frontier:
        if not command_dependencies_ready(command):
            continue
        if command_is_blocked_by_operator(command, conn):
            continue
        actionable.append(command)
    if not actionable:
        return None
    actionable.sort(
        key=lambda command: (
            PARALLEL_STAGE_RANK[command.stage.value],
            PRIORITY_RANK[command.priority.value],
            command.created_at,
        )
    )
    return actionable[0]


def request_agent_approval(
    conn,
    command: CommandRecord,
    agent_run: AgentRunRecord,
    note: str,
) -> TickOutcome:
    return request_agent_approvals(conn, command, [agent_run], note)


def request_agent_approvals(
    conn,
    command: CommandRecord,
    agent_runs: Sequence[AgentRunRecord],
    note: str,
) -> TickOutcome:
    if not agent_runs:
        raise ValueError("At least one agent run is required.")
    update_command(conn, command.id, stage=CommandStage.WAITING_APPROVAL.value)
    append_event(
        conn,
        "command",
        command.id,
        "agent_approval_requested",
        {
            "agent_run_ids": [agent_run.id for agent_run in agent_runs],
            "count": len(agent_runs),
            "run_kind": agent_runs[0].run_kind.value if len(agent_runs) == 1 else "mixed",
            "role_names": sorted({agent_run.role_name for agent_run in agent_runs}),
            "titles": [agent_run.title for agent_run in agent_runs],
        },
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return TickOutcome(
        action="approval_required",
        command=command_from_row(row),
        note=note,
    )


def _resume_stage_for_agent_runs(agent_runs: Sequence[AgentRunRecord]) -> CommandStage:
    stages = {agent_run.resume_stage for agent_run in agent_runs}
    if not stages:
        raise ValueError("No agent runs were supplied.")
    return min(stages, key=lambda stage: STAGE_RANK[stage.value])


def _approve_agent_run_record(conn, agent_run: AgentRunRecord) -> AgentRunRecord:
    update_agent_run(conn, agent_run.id, state=AgentRunState.APPROVED.value, error=None)
    append_event(
        conn,
        "agent_run",
        agent_run.id,
        "agent_run_approved",
        {"command_id": agent_run.command_id, "resume_stage": agent_run.resume_stage.value},
    )
    append_event(
        conn,
        "command",
        agent_run.command_id,
        "agent_approval_resolved",
        {"agent_run_id": agent_run.id, "decision": "approved"},
    )
    refreshed = get_agent_run_by_id(conn, agent_run.id)
    append_agent_run_log_conn(
        conn,
        agent_run.id,
        format_agent_log_line("operator approved this run"),
    )
    assert refreshed is not None
    return refreshed


def _pending_agent_runs_for_command(
    conn, command_id: str, role_name: Optional[str] = None
) -> List[AgentRunRecord]:
    query = """
        SELECT * FROM agent_runs
        WHERE command_id = ? AND state = ?
    """
    params: List[object] = [command_id, AgentRunState.PENDING_APPROVAL.value]
    if role_name:
        query += " AND role_name = ?"
        params.append(role_name)
    query += " ORDER BY created_at ASC"
    rows = conn.execute(query, params).fetchall()
    return [agent_run_from_row(row) for row in rows]


def terminate_running_agent_runs_for_command(conn, command_id: str) -> None:
    rows = conn.execute(
        """
        SELECT process_id FROM agent_runs
        WHERE command_id = ? AND state = ? AND process_id IS NOT NULL
        """,
        (command_id, AgentRunState.RUNNING.value),
    ).fetchall()
    for row in rows:
        terminate_process_group(row["process_id"])


def approve_agent_run(db_path: Path, agent_run_id: str) -> AgentRunRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        agent_run = get_agent_run_by_id(conn, agent_run_id)
        if agent_run is None:
            raise ValueError(f"Unknown agent run: {agent_run_id}")
        if agent_run.state != AgentRunState.PENDING_APPROVAL:
            raise ValueError(f"Agent run is not pending approval: {agent_run_id}")

        refreshed = _approve_agent_run_record(conn, agent_run)
        update_command(conn, agent_run.command_id, stage=agent_run.resume_stage.value)
        conn.commit()
        return refreshed


def approve_agent_runs_batch(
    db_path: Path, command_id: str, role_name: Optional[str] = None
) -> List[AgentRunRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        pending = _pending_agent_runs_for_command(conn, command_id, role_name=role_name)
        if not pending:
            return []

        approved = [_approve_agent_run_record(conn, agent_run) for agent_run in pending]
        update_command(conn, command_id, stage=_resume_stage_for_agent_runs(pending).value)
        append_event(
            conn,
            "command",
            command_id,
            "agent_approval_batch_resolved",
            {
                "decision": "approved",
                "count": len(approved),
                "role_name": role_name,
                "agent_run_ids": [agent_run.id for agent_run in approved],
            },
        )
        conn.commit()
        return approved


def deny_agent_run(
    db_path: Path, agent_run_id: str, reason: Optional[str] = None
) -> AgentRunRecord:
    initialize_database(db_path)
    denial_reason = (reason or "").strip() or "Operator denied the requested agent action."
    with connect(db_path) as conn:
        agent_run = get_agent_run_by_id(conn, agent_run_id)
        if agent_run is None:
            raise ValueError(f"Unknown agent run: {agent_run_id}")
        if agent_run.state != AgentRunState.PENDING_APPROVAL:
            raise ValueError(f"Agent run is not pending approval: {agent_run_id}")

        update_agent_run(
            conn,
            agent_run.id,
            state=AgentRunState.DENIED.value,
            error=denial_reason,
            finished_at=utc_now(),
        )
        append_agent_run_log_conn(
            conn,
            agent_run.id,
            format_agent_log_line(denial_reason, channel="error"),
        )
        update_command(
            conn,
            agent_run.command_id,
            stage=CommandStage.FAILED.value,
            failure_reason=denial_reason,
        )
        append_event(
            conn,
            "agent_run",
            agent_run.id,
            "agent_run_denied",
            {"command_id": agent_run.command_id, "reason": denial_reason},
        )
        append_event(
            conn,
            "command",
            agent_run.command_id,
            "agent_approval_resolved",
            {"agent_run_id": agent_run.id, "decision": "denied", "reason": denial_reason},
        )
        conn.commit()
        refreshed = get_agent_run_by_id(conn, agent_run.id)
        assert refreshed is not None
        return refreshed


def reduce_queued(conn, command: CommandRecord) -> TickOutcome:
    update_command(conn, command.id, stage=CommandStage.PLANNING.value)
    append_event(
        conn,
        "command",
        command.id,
        "planning_started",
        {"from_stage": CommandStage.QUEUED.value, "to_stage": CommandStage.PLANNING.value},
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return TickOutcome(action="planning_started", command=command_from_row(row))


def reduce_planning(conn, command: CommandRecord, settings: AppConfig) -> TickOutcome:
    tasks = list_tasks_for_command(conn, command.id)
    questions = list_questions_for_command(conn, command.id)
    reviews = list_reviews_for_command(conn, command.id)
    instructions = list_instructions_for_command(conn, command.id)
    role_name, runtime, model = execution_profile(command, settings, "planner")
    agent_run = ensure_agent_run(
        conn,
        command=command,
        role_name=role_name,
        capability="planner",
        runtime=runtime,
        model=model,
        run_kind=AgentRunKind.PLANNER,
        title="Plan the next workflow step",
        resume_stage=command.stage,
        approval_required=approval_required_for_runtime(command, runtime),
        prompt_excerpt=prompt_excerpt(command.goal),
    )
    if agent_run.state == AgentRunState.PENDING_APPROVAL:
        return request_agent_approval(conn, command, agent_run, note="waiting for planner approval")
    if agent_run.state == AgentRunState.APPROVED:
        agent_run = mark_agent_run_running(conn, agent_run.id)
    conn.commit()
    backend = backend_for(command, settings, "planner")

    try:
        output = backend.plan(
            command,
            tasks,
            questions,
            reviews,
            instructions,
            agent_run_id=agent_run.id,
            db_path=settings.db_path,
        )
    except AgentExecutionError as exc:
        mark_agent_run_finished(
            conn,
            agent_run.id,
            state=AgentRunState.FAILED,
            error=exc.detail,
        )
        waiting = set_command_operator_issue(
            conn,
            command,
            issue=exc,
            agent_run=agent_run,
            run_count=command.run_count + 1,
            planning_attempts=command.planning_attempts + 1,
            replan_requested=0,
        )
        append_event(
            conn,
            "command",
            command.id,
            "planning_interrupted",
            {"kind": exc.kind.value, "agent_run_id": agent_run.id},
        )
        return TickOutcome(
            action="operator_attention_required",
            command=waiting,
            note=exc.detail,
        )
    except Exception as exc:
        mark_agent_run_finished(
            conn,
            agent_run.id,
            state=AgentRunState.FAILED,
            error=str(exc),
        )
        update_command(
            conn,
            command.id,
            stage=CommandStage.FAILED.value,
            failure_reason=str(exc),
            run_count=command.run_count + 1,
            planning_attempts=command.planning_attempts + 1,
            replan_requested=0,
        )
        append_event(conn, "command", command.id, "planning_failed", {"error": str(exc)})
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(action="planning_failed", command=command_from_row(row), note=str(exc))

    mark_agent_run_finished(
        conn,
        agent_run.id,
        state=AgentRunState.COMPLETED,
        output_summary=output.decision.value,
    )

    next_run_count = command.run_count + 1
    next_planning_attempts = command.planning_attempts + 1
    resolved_mode = resolve_command_mode(command, output)

    if output.decision == PlannerDecision.CREATE_TASKS:
        planned_specs = add_mode_control_tasks(
            resolved_mode,
            output.tasks,
            runbook_path=command.runbook_path,
        )
        try:
            created_tasks = create_task_records(conn, command, planned_specs)
        except Exception as exc:
            update_command(
                conn,
                command.id,
                stage=CommandStage.FAILED.value,
                failure_reason=str(exc),
                run_count=next_run_count,
                planning_attempts=next_planning_attempts,
                replan_requested=0,
                effective_mode=resolved_mode.value,
            )
            append_event(conn, "command", command.id, "planning_failed", {"error": str(exc)})
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
            return TickOutcome(
                action="planning_failed", command=command_from_row(row), note=str(exc)
            )

        update_command(
            conn,
            command.id,
            stage=CommandStage.RUNNING.value,
            question_state="none",
            run_count=next_run_count,
            planning_attempts=next_planning_attempts,
            failure_reason=None,
            replan_requested=0,
            effective_mode=resolved_mode.value,
            resume_hint=None,
        )
        append_event(
            conn,
            "command",
            command.id,
            "tasks_planned",
            {
                "task_ids": [task.id for task in created_tasks],
                "count": len(created_tasks),
                "effective_mode": resolved_mode.value,
            },
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="tasks_planned",
            command=command_from_row(row),
            tasks=created_tasks,
            note=f"{len(created_tasks)} tasks created",
        )

    if output.decision == PlannerDecision.ASK_QUESTION:
        question = create_question_record(
            conn,
            command=command,
            source="planner",
            resolution_mode=QuestionResolutionMode.REPLAN_COMMAND,
            resume_stage=command.stage,
            question=output.question or "Planner requested clarification.",
        )
        update_command(
            conn,
            command.id,
            stage=CommandStage.WAITING_QUESTION.value,
            question_state=QuestionState.OPEN.value,
            run_count=next_run_count,
            planning_attempts=next_planning_attempts,
            replan_requested=0,
            effective_mode=resolved_mode.value,
            resume_hint=None,
        )
        append_event(
            conn,
            "command",
            command.id,
            "planning_blocked_on_question",
            {"question_id": question.id, "effective_mode": resolved_mode.value},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="question_created", command=command_from_row(row), question=question
        )

    if output.decision == PlannerDecision.COMPLETE:
        existing_tester = any(task.capability == "tester" for task in tasks)
        if mode_requires_test(resolved_mode) and not existing_tester:
            implicit_spec = (
                build_dogfooding_validation_spec(
                    [task.task_key for task in tasks if task.task_key],
                    command.runbook_path,
                )
                if resolved_mode == WorkflowMode.DOGFOODING
                else build_final_test_spec([task.task_key for task in tasks if task.task_key])
            )
            created_tasks = create_task_records(
                conn,
                command,
                [implicit_spec],
            )
            update_command(
                conn,
                command.id,
                stage=CommandStage.RUNNING.value,
                question_state="none",
                run_count=next_run_count,
                planning_attempts=next_planning_attempts,
                failure_reason=None,
                replan_requested=0,
                effective_mode=resolved_mode.value,
                resume_hint=None,
            )
            append_event(
                conn,
                "command",
                command.id,
                "tasks_planned",
                {
                    "task_ids": [task.id for task in created_tasks],
                    "count": len(created_tasks),
                    "effective_mode": resolved_mode.value,
                    "source": "implicit_test_gate",
                },
            )
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
            return TickOutcome(
                action="tasks_planned",
                command=command_from_row(row),
                tasks=created_tasks,
                note="inserted final test gate",
            )

        if mode_requires_review(resolved_mode):
            update_command(
                conn,
                command.id,
                stage=CommandStage.VERIFYING.value,
                run_count=next_run_count,
                planning_attempts=next_planning_attempts,
                failure_reason=None,
                replan_requested=0,
                effective_mode=resolved_mode.value,
                resume_hint=None,
            )
            append_event(
                conn,
                "command",
                command.id,
                "verification_started",
                {"source": "planner_complete", "effective_mode": resolved_mode.value},
            )
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
            return TickOutcome(
                action="verification_started",
                command=command_from_row(row),
                note=output.final_response,
            )

        resolved_command = command.model_copy(update={"effective_mode": resolved_mode})
        final_response = output.final_response or build_final_response(
            resolved_command,
            tasks,
            [],
        )
        replace_result_artifacts(conn, resolved_command, final_response, tasks, [])
        update_command(
            conn,
            command.id,
            stage=CommandStage.DONE.value,
            final_response=final_response,
            run_count=next_run_count,
            planning_attempts=next_planning_attempts,
            failure_reason=None,
            replan_requested=0,
            effective_mode=resolved_mode.value,
            resume_hint=None,
        )
        append_event(
            conn,
            "command",
            command.id,
            "command_completed",
            {"source": "planner", "effective_mode": resolved_mode.value},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="command_completed", command=command_from_row(row), note=final_response
        )

    update_command(
        conn,
        command.id,
        stage=CommandStage.FAILED.value,
        failure_reason=output.failure_reason or "Planner returned failure.",
        run_count=next_run_count,
        planning_attempts=next_planning_attempts,
        replan_requested=0,
        effective_mode=resolved_mode.value,
    )
    append_event(
        conn,
        "command",
        command.id,
        "planning_failed",
        {"reason": output.failure_reason or "Planner returned failure."},
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return TickOutcome(
        action="planning_failed", command=command_from_row(row), note=output.failure_reason
    )


def determine_resume_stage(answered_questions: Sequence[QuestionRecord]) -> CommandStage:
    stage = CommandStage.RUNNING
    for question in answered_questions:
        if question.resume_stage == CommandStage.PLANNING:
            return CommandStage.PLANNING
        if question.resume_stage == CommandStage.REPLANNING:
            stage = CommandStage.REPLANNING
        elif (
            question.resolution_mode == QuestionResolutionMode.REPLAN_COMMAND
            and stage != CommandStage.PLANNING
        ):
            stage = CommandStage.REPLANNING
    return stage


def reduce_waiting_question(conn, command: CommandRecord) -> TickOutcome:
    open_rows = conn.execute(
        """
        SELECT * FROM questions
        WHERE command_id = ? AND state = ?
        ORDER BY created_at ASC
        """,
        (command.id, QuestionState.OPEN.value),
    ).fetchall()
    if open_rows:
        first = question_from_row(open_rows[0])
        return TickOutcome(
            action="blocked",
            command=command,
            question=first,
            note=f"waiting for {len(open_rows)} user answer(s)",
        )

    answered_rows = conn.execute(
        """
        SELECT * FROM questions
        WHERE command_id = ? AND state = ?
        ORDER BY created_at ASC
        """,
        (command.id, QuestionState.ANSWERED.value),
    ).fetchall()
    if not answered_rows:
        return TickOutcome(action="no_op", command=command, note="no answered questions to resolve")

    answered_questions = [question_from_row(row) for row in answered_rows]
    for question in answered_questions:
        if question.task_id and question.resolution_mode == QuestionResolutionMode.RESUME_TASK:
            update_task(conn, question.task_id, state=TaskState.PENDING.value)
        elif question.task_id and question.resolution_mode == QuestionResolutionMode.REPLAN_COMMAND:
            update_task(conn, question.task_id, state=TaskState.CANCELED.value)
        update_question(conn, question.id, state=QuestionState.RESOLVED.value)

    next_stage = determine_resume_stage(answered_questions)
    update_command(
        conn,
        command.id,
        stage=next_stage.value,
        question_state="none",
    )
    append_event(
        conn,
        "command",
        command.id,
        "question_resolved",
        {
            "question_ids": [question.id for question in answered_questions],
            "resume_stage": next_stage.value,
        },
    )
    command_row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    question_row = conn.execute(
        "SELECT * FROM questions WHERE id = ?", (answered_questions[0].id,)
    ).fetchone()
    return TickOutcome(
        action="question_resolved",
        command=command_from_row(command_row),
        question=question_from_row(question_row),
        note=f"resolved {len(answered_questions)} question(s)",
    )


def task_write_tokens(task: TaskRecord) -> List[str]:
    files = task.input_payload.get("write_files", [])
    if isinstance(files, list) and files:
        return [str(item) for item in files]
    return [f"__unknown__:{task.capability}"]


def select_ready_batch(tasks: Sequence[TaskRecord], settings: AppConfig) -> List[TaskRecord]:
    done_ids = {task.id for task in tasks if task.state == TaskState.DONE}
    ready = [
        task
        for task in tasks
        if task.state == TaskState.PENDING and all(dep in done_ids for dep in task.depends_on)
    ]
    selected: List[TaskRecord] = []
    used_counts: Dict[str, int] = {}
    locked_tokens: set[str] = set()
    for task in ready:
        role = settings.role_for(task.capability)
        if used_counts.get(role.name, 0) >= role.count:
            continue
        tokens = set(task_write_tokens(task))
        if locked_tokens & tokens:
            continue
        selected.append(task)
        used_counts[role.name] = used_counts.get(role.name, 0) + 1
        locked_tokens.update(tokens)
    return selected


def execute_task_batch(
    command: CommandRecord,
    batch: Sequence[TaskRecord],
    agent_run_map: Dict[str, AgentRunRecord],
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
    settings: AppConfig,
) -> Dict[str, Tuple[Optional[WorkerOutput], Optional[Exception]]]:
    def run_single(task: TaskRecord) -> Tuple[str, Optional[WorkerOutput], Optional[Exception]]:
        try:
            if is_deterministic_test_gate(task):
                output = execute_deterministic_test_task(
                    command,
                    task,
                    settings,
                    agent_run_id=agent_run_map[task.id].id,
                )
            else:
                backend = backend_for(command, settings, task.capability)
                output = backend.execute_task(
                    command,
                    task,
                    tasks,
                    questions,
                    reviews,
                    instructions,
                    agent_run_id=agent_run_map[task.id].id,
                    db_path=settings.db_path,
                )
            return task.id, output, None
        except Exception as exc:
            return task.id, None, exc

    results: Dict[str, Tuple[Optional[WorkerOutput], Optional[Exception]]] = {}
    with ThreadPoolExecutor(max_workers=len(batch)) as executor:
        futures = [executor.submit(run_single, task) for task in batch]
        for future in as_completed(futures):
            task_id, output, error = future.result()
            results[task_id] = (output, error)
    return results


def reduce_running(conn, command: CommandRecord, settings: AppConfig) -> TickOutcome:
    resolved_mode = effective_mode(command)
    if command.replan_requested:
        update_command(
            conn,
            command.id,
            stage=CommandStage.REPLANNING.value,
            replan_requested=0,
        )
        append_event(
            conn,
            "command",
            command.id,
            "replanning_started",
            {"source": "instruction_append"},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="replanning_requested",
            command=command_from_row(row),
            note="appended instruction queued replanning",
        )

    tasks = list_tasks_for_command(conn, command.id)
    batch = select_ready_batch(tasks, settings)
    if not batch:
        if any(task.state == TaskState.PENDING for task in tasks):
            update_command(
                conn,
                command.id,
                stage=CommandStage.FAILED.value,
                failure_reason="Pending tasks remain but no dependency-safe batch is ready.",
            )
            append_event(
                conn,
                "command",
                command.id,
                "dispatch_failed",
                {"reason": "No dependency-safe batch was ready."},
            )
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
            return TickOutcome(action="dispatch_failed", command=command_from_row(row))

        if mode_requires_review(resolved_mode):
            update_command(conn, command.id, stage=CommandStage.VERIFYING.value)
            append_event(
                conn,
                "command",
                command.id,
                "verification_started",
                {"effective_mode": resolved_mode.value},
            )
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
            return TickOutcome(action="verification_started", command=command_from_row(row))

        final_response = build_final_response(command, tasks, [])
        replace_result_artifacts(conn, command, final_response, tasks, [])
        update_command(
            conn,
            command.id,
            stage=CommandStage.DONE.value,
            final_response=final_response,
        )
        append_event(
            conn,
            "command",
            command.id,
            "command_completed",
            {"source": "tasks", "effective_mode": resolved_mode.value},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="command_completed", command=command_from_row(row), note="all tasks completed"
        )

    precreated_runs: Dict[str, AgentRunRecord] = {}
    approval_runs: List[AgentRunRecord] = []
    for task in batch:
        if is_deterministic_test_gate(task):
            continue
        role_name, runtime, model = execution_profile(command, settings, task.capability)
        if not approval_required_for_runtime(command, runtime):
            continue
        agent_run = ensure_agent_run(
            conn,
            command=command,
            role_name=role_name,
            capability=task.capability,
            runtime=runtime,
            model=model,
            run_kind=AgentRunKind.TASK,
            title=task.title,
            resume_stage=CommandStage.RUNNING,
            approval_required=True,
            task_id=task.id,
            prompt_excerpt=prompt_excerpt(task.description),
        )
        approval_runs.append(agent_run)
        if agent_run.state == AgentRunState.APPROVED:
            agent_run = mark_agent_run_running(conn, agent_run.id)
        precreated_runs[task.id] = agent_run
    pending_approval_runs = [
        agent_run
        for agent_run in approval_runs
        if agent_run.state == AgentRunState.PENDING_APPROVAL
    ]
    if pending_approval_runs:
        return request_agent_approvals(
            conn,
            command,
            pending_approval_runs,
            note=f"waiting for approval to run {len(pending_approval_runs)} agent action(s)",
        )

    agent_run_map: Dict[str, AgentRunRecord] = {}
    for task in batch:
        agent_run = precreated_runs.get(task.id)
        if agent_run is None:
            role_name, runtime, model = execution_profile(command, settings, task.capability)
            if is_deterministic_test_gate(task):
                runtime = RuntimeBackend.INHERIT
                model = ""
            agent_run = ensure_agent_run(
                conn,
                command=command,
                role_name=role_name,
                capability=task.capability,
                runtime=runtime,
                model=model,
                run_kind=AgentRunKind.TASK,
                title=task.title,
                resume_stage=CommandStage.RUNNING,
                approval_required=False,
                task_id=task.id,
                prompt_excerpt=prompt_excerpt(task.description),
            )
        agent_run_map[task.id] = agent_run
        update_task(
            conn,
            task.id,
            state=TaskState.RUNNING.value,
            attempt_count=task.attempt_count + 1,
            assigned_run_id=agent_run.id,
            operator_issue_kind=None,
        )
        append_event(conn, "task", task.id, "task_started", {"command_id": command.id})

    tasks = list_tasks_for_command(conn, command.id)
    batch_ids = {task.id for task in batch}
    active_batch = [task for task in tasks if task.id in batch_ids]
    questions = list_questions_for_command(conn, command.id)
    reviews = list_reviews_for_command(conn, command.id)
    instructions = list_instructions_for_command(conn, command.id)
    conn.commit()
    results = execute_task_batch(
        command,
        active_batch,
        agent_run_map,
        tasks,
        questions,
        reviews,
        instructions,
        settings,
    )

    updated_tasks: List[TaskRecord] = []
    created_questions: List[QuestionRecord] = []
    failed_any = False
    operator_issues: List[Tuple[TaskRecord, AgentRunRecord, AgentExecutionError]] = []
    for task in active_batch:
        output, error = results[task.id]
        agent_run = agent_run_map[task.id]
        if error is not None:
            if isinstance(error, AgentExecutionError):
                update_task(
                    conn,
                    task.id,
                    state=TaskState.BLOCKED.value,
                    error=error.detail,
                    operator_issue_kind=error.kind.value,
                    output_payload=None,
                )
                mark_agent_run_finished(
                    conn,
                    agent_run.id,
                    state=AgentRunState.FAILED,
                    error=error.detail,
                )
                append_event(
                    conn,
                    "task",
                    task.id,
                    "task_interrupted",
                    {
                        "command_id": command.id,
                        "kind": error.kind.value,
                        "detail": error.detail,
                    },
                )
                operator_issues.append((task, agent_run, error))
            else:
                failure_text = str(error)
                update_task(
                    conn,
                    task.id,
                    state=TaskState.FAILED.value,
                    error=failure_text,
                    operator_issue_kind=None,
                    output_payload=json.dumps({"error": failure_text}, sort_keys=True),
                )
                mark_agent_run_finished(
                    conn,
                    agent_run.id,
                    state=AgentRunState.FAILED,
                    error=failure_text,
                )
                append_event(
                    conn,
                    "task",
                    task.id,
                    "task_failed",
                    {"command_id": command.id, "error": failure_text},
                )
                failed_any = True
        elif output and output.decision == WorkerDecision.COMPLETE:
            result_payload = dict(output.result)
            if output.summary:
                result_payload["summary"] = output.summary
            update_task(
                conn,
                task.id,
                state=TaskState.DONE.value,
                output_payload=json.dumps(result_payload, sort_keys=True),
                error=None,
                operator_issue_kind=None,
            )
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.COMPLETED,
                output_summary=output.summary or task.title,
            )
            append_event(
                conn,
                "task",
                task.id,
                "task_completed",
                {"command_id": command.id, "summary": output.summary},
            )
        elif output and output.decision == WorkerDecision.ASK_QUESTION:
            resolution_mode = output.resolution_mode or QuestionResolutionMode.RESUME_TASK
            resume_stage = (
                CommandStage.RUNNING
                if resolution_mode == QuestionResolutionMode.RESUME_TASK
                else CommandStage.REPLANNING
            )
            task_state = (
                TaskState.BLOCKED
                if resolution_mode == QuestionResolutionMode.RESUME_TASK
                else TaskState.CANCELED
            )
            update_task(
                conn,
                task.id,
                state=task_state.value,
                error=None,
                operator_issue_kind=None,
            )
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.COMPLETED,
                output_summary=output.question or "question requested",
            )
            question = create_question_record(
                conn,
                command=command,
                source=task.capability,
                resolution_mode=resolution_mode,
                resume_stage=resume_stage,
                question=output.question or f"Task '{task.title}' requested clarification.",
                task_id=task.id,
            )
            append_event(
                conn,
                "command",
                command.id,
                "task_blocked_on_question",
                {"task_id": task.id, "question_id": question.id},
            )
            created_questions.append(question)
        else:
            failure_reason = output.failure_reason if output else "Task failed."
            update_task(
                conn,
                task.id,
                state=TaskState.FAILED.value,
                error=failure_reason,
                operator_issue_kind=None,
                output_payload=json.dumps({"error": failure_reason}, sort_keys=True),
            )
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.FAILED,
                error=failure_reason,
            )
            append_event(
                conn,
                "task",
                task.id,
                "task_failed",
                {"command_id": command.id, "reason": failure_reason},
            )
            failed_any = True

    updated_rows = conn.execute(
        f"SELECT * FROM tasks WHERE id IN ({','.join('?' for _ in batch_ids)}) ORDER BY plan_order ASC",
        list(batch_ids),
    ).fetchall()
    updated_tasks = attach_dependencies(
        [task_from_row(row) for row in updated_rows], dependency_map_for_command(conn, command.id)
    )

    next_stage = CommandStage.RUNNING
    question_state = "none"
    if operator_issues:
        next_stage = CommandStage.WAITING_OPERATOR
    elif created_questions:
        next_stage = CommandStage.WAITING_QUESTION
        question_state = QuestionState.OPEN.value
    elif failed_any:
        next_stage = CommandStage.REPLANNING
    else:
        refreshed_tasks = list_tasks_for_command(conn, command.id)
        if any(task.state == TaskState.PENDING for task in refreshed_tasks):
            next_stage = CommandStage.RUNNING
        else:
            next_stage = (
                CommandStage.VERIFYING if mode_requires_review(resolved_mode) else CommandStage.DONE
            )

    final_response = (
        build_final_response(command, list_tasks_for_command(conn, command.id), [])
        if next_stage == CommandStage.DONE
        else None
    )
    if final_response is not None:
        replace_result_artifacts(
            conn,
            command,
            final_response,
            list_tasks_for_command(conn, command.id),
            [],
        )
    if next_stage == CommandStage.WAITING_OPERATOR:
        blocked_task, blocked_run, blocked_issue = operator_issues[0]
        updated_command = set_command_operator_issue(
            conn,
            command,
            issue=blocked_issue,
            agent_run=blocked_run,
            task_id=blocked_task.id,
            run_count=command.run_count + len(active_batch),
        )
        if created_questions:
            update_command(conn, command.id, question_state=QuestionState.OPEN.value)
        action = "operator_attention_required"
    else:
        update_command(
            conn,
            command.id,
            stage=next_stage.value,
            question_state=question_state,
            run_count=command.run_count + len(active_batch),
            failure_reason=None,
            final_response=final_response,
            resume_hint=None,
        )
        updated_command = None
    if next_stage == CommandStage.WAITING_OPERATOR:
        action = "operator_attention_required"
    elif next_stage == CommandStage.REPLANNING:
        append_event(
            conn,
            "command",
            command.id,
            "replanning_requested",
            {"task_ids": [task.id for task in updated_tasks]},
        )
        action = "replanning_requested"
    elif next_stage == CommandStage.WAITING_QUESTION:
        action = "question_created"
    elif next_stage == CommandStage.VERIFYING:
        append_event(
            conn,
            "command",
            command.id,
            "verification_started",
            {"effective_mode": resolved_mode.value},
        )
        action = "verification_started"
    elif next_stage == CommandStage.DONE:
        append_event(
            conn,
            "command",
            command.id,
            "command_completed",
            {"source": "tasks", "effective_mode": resolved_mode.value},
        )
        action = "command_completed"
    else:
        action = "task_batch_completed"

    command_row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return TickOutcome(
        action=action,
        command=updated_command or command_from_row(command_row),
        task=updated_tasks[0] if updated_tasks else None,
        tasks=updated_tasks,
        question=created_questions[0] if created_questions else None,
        note=f"{len(active_batch)} task(s) processed",
    )


def run_review_batch(
    command: CommandRecord,
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
    settings: AppConfig,
    slots: Optional[List[int]] = None,
    agent_run_ids: Optional[Dict[int, str]] = None,
) -> List[Tuple[int, Optional[ReviewerOutput], Optional[Exception]]]:
    reviewer_count = settings.role_for("reviewer").count
    active_slots = slots or list(range(1, reviewer_count + 1))

    def run_single(slot: int) -> Tuple[int, Optional[ReviewerOutput], Optional[Exception]]:
        backend = backend_for(command, settings, "reviewer")
        try:
            output = backend.review(
                command,
                tasks,
                questions,
                reviews,
                instructions,
                reviewer_slot=slot,
                agent_run_id=agent_run_ids.get(slot) if agent_run_ids else None,
                db_path=settings.db_path,
            )
            return slot, output, None
        except Exception as exc:
            return slot, None, exc

    outputs: List[Tuple[int, Optional[ReviewerOutput], Optional[Exception]]] = []
    with ThreadPoolExecutor(max_workers=max(len(active_slots), 1)) as executor:
        futures = [executor.submit(run_single, slot) for slot in active_slots]
        for future in as_completed(futures):
            outputs.append(future.result())
    outputs.sort(key=lambda item: item[0])
    return outputs


def reduce_verifying(conn, command: CommandRecord, settings: AppConfig) -> TickOutcome:
    if command.replan_requested:
        update_command(
            conn,
            command.id,
            stage=CommandStage.REPLANNING.value,
            replan_requested=0,
        )
        append_event(
            conn,
            "command",
            command.id,
            "replanning_started",
            {"source": "instruction_append"},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="replanning_requested",
            command=command_from_row(row),
            note="appended instruction bypassed verification",
        )

    tasks = list_tasks_for_command(conn, command.id)
    questions = list_questions_for_command(conn, command.id)
    previous_reviews = list_reviews_for_command(conn, command.id)
    instructions = list_instructions_for_command(conn, command.id)
    reviewer_count = settings.role_for("reviewer").count
    slot_run_map: Dict[int, AgentRunRecord] = {}
    for slot in range(1, reviewer_count + 1):
        role_name, runtime, model = execution_profile(command, settings, "reviewer")
        agent_run = ensure_agent_run(
            conn,
            command=command,
            role_name=role_name,
            capability="reviewer",
            runtime=runtime,
            model=model,
            run_kind=AgentRunKind.REVIEW,
            title=f"Final review slot {slot}",
            resume_stage=CommandStage.VERIFYING,
            approval_required=approval_required_for_runtime(command, runtime),
            reviewer_slot=slot,
            prompt_excerpt=prompt_excerpt(command.goal),
        )
        slot_run_map[slot] = agent_run

    pending_runs = [
        run for run in slot_run_map.values() if run.state == AgentRunState.PENDING_APPROVAL
    ]
    if pending_runs:
        return request_agent_approvals(
            conn,
            command,
            pending_runs,
            note=f"waiting for approval to run {len(pending_runs)} reviewer action(s)",
        )

    if any(run.state == AgentRunState.APPROVED for run in slot_run_map.values()):
        for slot, agent_run in list(slot_run_map.items()):
            if agent_run.state == AgentRunState.APPROVED:
                slot_run_map[slot] = mark_agent_run_running(conn, agent_run.id)
    conn.commit()

    outputs = run_review_batch(
        command,
        tasks,
        questions,
        previous_reviews,
        instructions,
        settings,
        slots=list(slot_run_map.keys()),
        agent_run_ids={slot: run.id for slot, run in slot_run_map.items()},
    )
    latest_command = get_command(settings.db_path, command.id)
    if latest_command and latest_command.stage == CommandStage.CANCELED:
        return TickOutcome(
            action="command_canceled",
            command=latest_command,
            note=latest_command.failure_reason or "canceled during final review",
        )
    recoverable_review_issues: List[Tuple[int, AgentRunRecord, AgentExecutionError]] = []

    reviewer_kind = (
        command.backend.value
        if command.backend != RuntimeBackend.INHERIT
        else settings.role_for("reviewer").runtime.value
    )
    review_outputs: List[Tuple[int, ReviewerOutput]] = []
    for slot, output, error in outputs:
        if isinstance(error, AgentExecutionError):
            recoverable_review_issues.append((slot, slot_run_map[slot], error))
            continue
        if error is not None:
            output = ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary=f"Reviewer {slot} failed.",
                findings=[],
                failure_reason=str(error),
            )
        if output is None:
            output = ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary=f"Reviewer {slot} ended without returning a result.",
                findings=[],
                failure_reason="The reviewer process ended without returning a structured result.",
            )
        review_outputs.append((slot, output))

    created_reviews = [
        create_review_record(conn, command, output, reviewer_slot=slot, reviewer_kind=reviewer_kind)
        for slot, output in review_outputs
    ]
    next_run_count = command.run_count + len(outputs)

    review_map = {review.reviewer_slot: review for review in created_reviews}
    for slot, output, error in outputs:
        agent_run = slot_run_map[slot]
        if isinstance(error, AgentExecutionError):
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.FAILED,
                error=error.detail,
            )
            continue
        if output is None:
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.FAILED,
                error="The reviewer process ended without returning a structured result.",
            )
            continue
        if output.decision == ReviewDecision.FAIL:
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.FAILED,
                error=output.failure_reason or output.summary,
            )
        else:
            mark_agent_run_finished(
                conn,
                agent_run.id,
                state=AgentRunState.COMPLETED,
                output_summary=review_map[slot].summary,
            )

    if recoverable_review_issues:
        _, blocked_run, blocked_issue = recoverable_review_issues[0]
        waiting = set_command_operator_issue(
            conn,
            command,
            issue=blocked_issue,
            agent_run=blocked_run,
            run_count=next_run_count,
        )
        append_event(
            conn,
            "command",
            command.id,
            "review_interrupted",
            {"kind": blocked_issue.kind.value, "agent_run_id": blocked_run.id},
        )
        return TickOutcome(
            action="operator_attention_required",
            command=waiting,
            note=blocked_issue.detail,
        )

    if any(review.decision == ReviewDecision.FAIL for review in created_reviews):
        failure_reason = next(
            (
                review.summary
                for review in created_reviews
                if review.decision == ReviewDecision.FAIL
            ),
            "Review failed.",
        )
        update_command(
            conn,
            command.id,
            stage=CommandStage.FAILED.value,
            run_count=next_run_count,
            failure_reason=failure_reason,
            resume_hint=None,
        )
        append_event(
            conn,
            "command",
            command.id,
            "review_failed",
            {"review_ids": [review.id for review in created_reviews]},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="review_failed",
            command=command_from_row(row),
            review=created_reviews[0],
            note=failure_reason,
        )

    if any(review.decision == ReviewDecision.REQUEST_CHANGES for review in created_reviews):
        update_command(
            conn,
            command.id,
            stage=CommandStage.REPLANNING.value,
            run_count=next_run_count,
            failure_reason=None,
            resume_hint=None,
        )
        append_event(
            conn,
            "command",
            command.id,
            "replanning_requested",
            {"review_ids": [review.id for review in created_reviews]},
        )
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
        return TickOutcome(
            action="replanning_requested",
            command=command_from_row(row),
            review=created_reviews[0],
            note=f"{len(created_reviews)} review(s) completed",
        )

    final_response = build_final_response(command, tasks, created_reviews)
    replace_result_artifacts(conn, command, final_response, tasks, created_reviews)
    update_command(
        conn,
        command.id,
        stage=CommandStage.DONE.value,
        final_response=final_response,
        run_count=next_run_count,
        failure_reason=None,
        resume_hint=None,
    )
    append_event(
        conn,
        "command",
        command.id,
        "command_completed",
        {"source": "review", "review_count": len(created_reviews)},
    )
    row = conn.execute("SELECT * FROM commands WHERE id = ?", (command.id,)).fetchone()
    return TickOutcome(
        action="command_completed",
        command=command_from_row(row),
        review=created_reviews[0],
        note=f"{len(created_reviews)} review(s) approved",
    )


def tick_once(
    db_path: Path,
    command_id: Optional[str] = None,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
) -> TickOutcome:
    settings = resolve_settings(settings=settings, repo_root=repo_root)
    initialize_database(db_path)

    with connect(db_path) as conn:
        reconciled_command_ids = reconcile_orphaned_agent_runs(conn)
        if reconciled_command_ids:
            target_id = (
                command_id if command_id in reconciled_command_ids else reconciled_command_ids[0]
            )
            row = conn.execute("SELECT * FROM commands WHERE id = ?", (target_id,)).fetchone()
            if row is not None:
                reconciled_command = command_from_row(row)
                return TickOutcome(
                    action="operator_attention_required",
                    command=reconciled_command,
                    note=reconciled_command.operator_issue_detail or RUNTIME_INTERRUPTED_DETAIL,
                )
        command = select_actionable_command(conn, command_id)
        if command is None:
            return TickOutcome(action="no_op", note="no actionable commands")

        if not command_dependencies_ready(command):
            return TickOutcome(
                action="blocked",
                command=command,
                note="waiting on command dependencies",
            )

        if command.stop_requested:
            paused = pause_command_record(conn, command, note="paused at next checkpoint")
            return TickOutcome(
                action="command_paused",
                command=paused,
                note="paused after the current step",
            )

        if command.stage == CommandStage.QUEUED:
            outcome = reduce_queued(conn, command)
        elif command.stage in {CommandStage.PLANNING, CommandStage.REPLANNING}:
            outcome = reduce_planning(conn, command, settings)
        elif command.stage == CommandStage.RUNNING:
            outcome = reduce_running(conn, command, settings)
        elif command.stage == CommandStage.WAITING_APPROVAL:
            outcome = TickOutcome(
                action="blocked",
                command=command,
                note="waiting for operator approval",
            )
        elif command.stage == CommandStage.WAITING_OPERATOR:
            outcome = TickOutcome(
                action="blocked",
                command=command,
                note="waiting for operator action",
            )
        elif command.stage == CommandStage.WAITING_QUESTION:
            outcome = reduce_waiting_question(conn, command)
        elif command.stage == CommandStage.VERIFYING:
            outcome = reduce_verifying(conn, command, settings)
        else:
            outcome = TickOutcome(
                action="no_op", command=command, note=f"stage '{command.stage.value}' is terminal"
            )

        conn.commit()
        return outcome


def answer_question(db_path: Path, question_id: str, answer: str) -> QuestionRecord:
    initialize_database(db_path)
    with connect(db_path) as conn:
        question = get_question_by_id(conn, question_id)
        if question is None:
            raise ValueError(f"Unknown question: {question_id}")
        if question.state != QuestionState.OPEN:
            raise ValueError(f"Question is not open: {question_id}")

        update_question(conn, question.id, answer=answer, state=QuestionState.ANSWERED.value)
        update_command(conn, question.command_id, question_state=QuestionState.ANSWERED.value)
        append_event(
            conn,
            "question",
            question.id,
            "question_answered",
            {"command_id": question.command_id},
        )
        conn.commit()
        row = conn.execute("SELECT * FROM questions WHERE id = ?", (question_id,)).fetchone()
        return question_from_row(row)


def run_engine(
    db_path: Path,
    command_id: Optional[str] = None,
    max_steps: int = 100,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
) -> Dict[str, object]:
    settings = resolve_settings(settings=settings, repo_root=repo_root)
    outcomes: List[TickOutcome] = []
    last_command_id = command_id
    for _ in range(max_steps):
        outcome = tick_once(db_path, command_id=command_id, settings=settings)
        outcomes.append(outcome)
        if outcome.command:
            last_command_id = outcome.command.id
        if outcome.action in {
            "no_op",
            "blocked",
            "command_paused",
            "command_completed",
            "planning_failed",
            "review_failed",
            "dispatch_failed",
        }:
            break
        if outcome.command and outcome.command.stage in {
            CommandStage.DONE,
            CommandStage.FAILED,
            CommandStage.CANCELED,
        }:
            break
    final_command = get_command(db_path, last_command_id) if last_command_id else None
    return {
        "steps": [outcome.model_dump(mode="json") for outcome in outcomes],
        "final_command": final_command.model_dump(mode="json") if final_command else None,
    }
