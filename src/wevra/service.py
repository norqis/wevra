from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from wevra.config import AppConfig, load_config
from wevra.db import connect, initialize_database
from wevra.models import (
    CommandRecord,
    CommandStage,
    EventRecord,
    InstructionRecord,
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
    CommandStage.WAITING_QUESTION.value: 4,
    CommandStage.QUEUED.value: 5,
    CommandStage.DONE.value: 6,
    CommandStage.FAILED.value: 7,
}

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
    WorkflowMode.PLANNING: ("plan", "design", "spec", "task breakdown", "設計", "計画", "分解"),
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def resolve_settings(
    settings: Optional[AppConfig] = None, repo_root: Optional[Path] = None
) -> AppConfig:
    if settings is not None:
        return settings
    return load_config((repo_root or Path.cwd()).resolve())


def requested_mode(command: CommandRecord) -> WorkflowMode:
    return command.workflow_mode


def effective_mode(command: CommandRecord) -> WorkflowMode:
    if command.effective_mode:
        return command.effective_mode
    if command.workflow_mode == WorkflowMode.AUTO:
        return WorkflowMode.IMPLEMENTATION
    return command.workflow_mode


def mode_requires_review(mode: WorkflowMode) -> bool:
    return mode in {WorkflowMode.IMPLEMENTATION, WorkflowMode.REVIEW}


def mode_requires_test(mode: WorkflowMode) -> bool:
    return mode == WorkflowMode.IMPLEMENTATION


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
            "Do not assume implementation, testing, or final review are required."
        )
    return (
        "This is auto mode. Infer whether the request is best handled as implementation, research, review, or planning, "
        "and set workflow_mode in the JSON response."
    )


def add_mode_control_tasks(
    mode: WorkflowMode, specs: Sequence[PlannerTaskSpec]
) -> List[PlannerTaskSpec]:
    planned = list(specs)
    if not mode_requires_test(mode):
        return planned
    if any(spec.capability == "tester" for spec in planned):
        return planned

    dependency_keys = [spec.key for spec in planned]
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


def append_event(conn, stream_type: str, stream_id: str, event_type: str, payload: dict) -> None:
    conn.execute(
        """
        INSERT INTO events (stream_type, stream_id, event_type, payload_json, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (stream_type, stream_id, event_type, json.dumps(payload, sort_keys=True), utc_now()),
    )


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


def command_from_row(row) -> CommandRecord:
    payload = dict(row)
    payload["replan_requested"] = bool(payload.get("replan_requested", 0))
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
    if task is not None:
        payload["task"] = task.model_dump(mode="json")
    return payload


class BackendInterface:
    def plan(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
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
    ) -> PlannerOutput:
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
            return PlannerOutput(
                decision=PlannerDecision.ASK_QUESTION,
                workflow_mode=current_mode,
                question="Planner needs clarification before scheduling work.",
            )

        latest_review = reviews[-1] if reviews else None
        if latest_review and latest_review.decision == ReviewDecision.REQUEST_CHANGES:
            if not any(task.input_payload.get("review_id") == latest_review.id for task in tasks):
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
            return PlannerOutput(
                decision=PlannerDecision.CREATE_TASKS,
                workflow_mode=current_mode,
                tasks=default_specs,
            )

        if not active_or_done:
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
    ) -> WorkerOutput:
        goal = " ".join([command.goal, *[instruction.body for instruction in instructions]]).lower()
        task_questions = [question for question in questions if question.task_id == task.id]
        is_primary_worker = task.capability == "implementer"
        if (
            "[worker_question]" in goal
            and is_primary_worker
            and not any(question.answer for question in task_questions)
        ):
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
            return WorkerOutput(
                decision=WorkerDecision.FAIL,
                failure_reason=f"Mock worker failed '{task.title}' and requested replanning.",
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
    ) -> ReviewerOutput:
        goal = " ".join([command.goal, *[instruction.body for instruction in instructions]]).lower()
        if "[review_fail]" in goal:
            return ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary=f"Reviewer {reviewer_slot} failed hard.",
                failure_reason="Review backend encountered a terminal validation failure.",
            )

        if "[review_changes]" in goal and not any(
            review.decision == ReviewDecision.REQUEST_CHANGES for review in reviews
        ):
            return ReviewerOutput(
                decision=ReviewDecision.REQUEST_CHANGES,
                summary=f"Reviewer {reviewer_slot} requested a follow-up pass.",
                findings=["Add a second implementation pass before completion."],
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
        danger: bool,
        runtime_home: Path | None = None,
    ):
        self.backend = backend
        self.model = model
        self.danger = danger
        self.runtime_home = runtime_home

    def plan(
        self,
        command: CommandRecord,
        tasks: List[TaskRecord],
        questions: List[QuestionRecord],
        reviews: List[ReviewRecord],
        instructions: List[InstructionRecord],
    ) -> PlannerOutput:
        context = build_context_payload(command, tasks, questions, reviews, instructions)
        planning_mode = "replanning" if command.stage == CommandStage.REPLANNING else "initial_plan"
        requested = requested_mode(command)
        active_mode = effective_mode(command)
        prompt = (
            "You are the Wevra planner.\n"
            "The engine owns all state transitions.\n"
            "Return only JSON that matches the provided schema.\n"
            "Every task must include a stable key, explicit depends_on references, and intended write_files.\n"
            f"Planning mode: {planning_mode}.\n"
            f"Requested workflow mode: {requested.value}.\n"
            f"Current effective workflow mode: {active_mode.value if active_mode else 'unresolved'}.\n"
            f"{mode_prompt_guidance(active_mode if requested != WorkflowMode.AUTO else WorkflowMode.AUTO)}\n"
            "If this is replanning, preserve completed work as fact, reuse still-valid task keys, and only omit tasks that should be superseded.\n\n"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt, PlannerOutput.model_json_schema(), command.workspace_root
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
    ) -> WorkerOutput:
        context = build_context_payload(command, tasks, questions, reviews, instructions, task=task)
        prompt = (
            "You are the Wevra worker.\n"
            "Use the workspace to complete the assigned task if changes are required.\n"
            "Do not mutate engine state. Return only JSON matching the schema.\n\n"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt, WorkerOutput.model_json_schema(), command.workspace_root
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
    ) -> ReviewerOutput:
        context = build_context_payload(command, tasks, questions, reviews, instructions)
        prompt = (
            f"You are Wevra reviewer #{reviewer_slot}.\n"
            "Inspect the current command context and workspace state.\n"
            "Return only JSON matching the schema.\n\n"
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}"
        )
        payload = self._run_structured(
            prompt, ReviewerOutput.model_json_schema(), command.workspace_root
        )
        return ReviewerOutput.model_validate(payload)

    def _run_structured(self, prompt: str, schema: dict, workspace_root: str) -> dict:
        root = Path(workspace_root)
        if self.backend == RuntimeBackend.CODEX:
            return self._run_codex(prompt, schema, root)
        if self.backend == RuntimeBackend.CLAUDE:
            return self._run_claude(prompt, schema, root)
        raise RuntimeError(f"Unsupported backend: {self.backend.value}")

    def _run_codex(self, prompt: str, schema: dict, workspace_root: Path) -> dict:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as schema_file:
            json.dump(schema, schema_file)
            schema_path = schema_file.name
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as output_file:
            output_path = output_file.name
        try:
            command = [
                "codex",
                "exec",
                "--skip-git-repo-check",
                "--output-schema",
                schema_path,
                "--output-last-message",
                output_path,
            ]
            if self.model:
                command.extend(["--model", self.model])
            if self.danger:
                command.append("--dangerously-bypass-approvals-and-sandbox")
            else:
                command.append("--full-auto")
            command.append(prompt)
            env = self._build_runtime_env()

            result = subprocess.run(
                command,
                cwd=str(workspace_root),
                env=env,
                text=True,
                capture_output=True,
                check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(
                    result.stderr.strip() or result.stdout.strip() or "codex exec failed"
                )
            return json.loads(Path(output_path).read_text())
        finally:
            Path(schema_path).unlink(missing_ok=True)
            Path(output_path).unlink(missing_ok=True)

    def _run_claude(self, prompt: str, schema: dict, workspace_root: Path) -> dict:
        command = [
            "claude",
            "-p",
            "--json-schema",
            json.dumps(schema, sort_keys=True),
        ]
        if self.model:
            command.extend(["--model", self.model])
        if self.danger:
            command.append("--dangerously-skip-permissions")
        else:
            command.extend(["--permission-mode", "acceptEdits"])
        command.append(prompt)
        env = self._build_runtime_env()

        result = subprocess.run(
            command,
            cwd=str(workspace_root),
            env=env,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                result.stderr.strip() or result.stdout.strip() or "claude print failed"
            )
        return json.loads(result.stdout)

    def _build_runtime_env(self) -> dict[str, str]:
        env = dict(os.environ)
        if self.runtime_home is not None:
            self.runtime_home.mkdir(parents=True, exist_ok=True)
            env["HOME"] = str(self.runtime_home)
        return env


def backend_for(command: CommandRecord, settings: AppConfig, capability: str) -> BackendInterface:
    role = settings.role_for(capability)
    runtime = role.runtime if command.backend == RuntimeBackend.INHERIT else command.backend
    if runtime == RuntimeBackend.MOCK:
        return MockBackend()
    return StructuredCliBackend(
        runtime,
        role.model,
        settings.danger,
        runtime_home=settings.runtime_home,
    )


def build_final_response(
    command: CommandRecord, tasks: List[TaskRecord], reviews: List[ReviewRecord]
) -> str:
    mode = effective_mode(command).value
    lines = [f"Goal: {command.goal}", f"Mode: {mode}", "", "Completed tasks:"]
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
    backend: Optional[RuntimeBackend] = None,
    workspace_root: Optional[Path] = None,
    settings: Optional[AppConfig] = None,
    repo_root: Optional[Path] = None,
) -> CommandRecord:
    settings = resolve_settings(settings=settings, repo_root=repo_root)
    initialize_database(db_path)

    command_id = new_id("cmd")
    now = utc_now()
    resolved_root = str((workspace_root or settings.working_dir).resolve())
    selected_backend = backend or RuntimeBackend.INHERIT

    with connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO commands (
                id, goal, stage, workflow_mode, effective_mode, priority, backend, workspace_root, final_response, failure_reason,
                question_state, planning_attempts, run_count, version, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                command_id,
                goal,
                CommandStage.QUEUED.value,
                workflow_mode.value,
                None if workflow_mode == WorkflowMode.AUTO else workflow_mode.value,
                priority.value,
                selected_backend.value,
                resolved_root,
                None,
                None,
                "none",
                0,
                0,
                1,
                now,
                now,
            ),
        )
        append_event(
            conn,
            "command",
            command_id,
            "command_submitted",
            {
                "goal": goal,
                "workflow_mode": workflow_mode.value,
                "priority": priority.value,
                "backend": selected_backend.value,
                "workspace_root": resolved_root,
            },
        )
        conn.commit()
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        return command_from_row(row)


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
        instruction = create_instruction_record(conn, command.id, instruction_body)

        if command.stage == CommandStage.WAITING_QUESTION:
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
                if question.task_id:
                    update_task(
                        conn,
                        question.task_id,
                        state=TaskState.CANCELED.value,
                        error="Superseded by appended instruction.",
                    )
                fields = {"state": QuestionState.RESOLVED.value}
                if question.state == QuestionState.OPEN:
                    fields["answer"] = "[superseded by appended instruction]"
                update_question(conn, question.id, **fields)

        if command.stage in {CommandStage.RUNNING, CommandStage.VERIFYING}:
            update_command(
                conn,
                command.id,
                replan_requested=1,
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


def get_command(db_path: Path, command_id: str) -> Optional[CommandRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        return command_from_row(row) if row else None


def list_commands(db_path: Path) -> List[CommandRecord]:
    initialize_database(db_path)
    with connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM commands").fetchall()
    commands = [command_from_row(row) for row in rows]
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


def select_actionable_command(conn, command_id: Optional[str]) -> Optional[CommandRecord]:
    if command_id:
        row = conn.execute("SELECT * FROM commands WHERE id = ?", (command_id,)).fetchone()
        return command_from_row(row) if row else None

    rows = conn.execute(
        "SELECT * FROM commands WHERE stage NOT IN (?, ?)",
        (CommandStage.DONE.value, CommandStage.FAILED.value),
    ).fetchall()
    commands = [command_from_row(row) for row in rows]
    actionable = []
    for command in commands:
        if command.stage == CommandStage.WAITING_QUESTION and not answered_question_ready(
            conn, command.id
        ):
            continue
        actionable.append(command)
    if not actionable:
        return None
    actionable.sort(
        key=lambda command: (
            PRIORITY_RANK[command.priority.value],
            STAGE_RANK[command.stage.value],
            command.created_at,
        )
    )
    return actionable[0]


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
    backend = backend_for(command, settings, "planner")

    try:
        output = backend.plan(command, tasks, questions, reviews, instructions)
    except Exception as exc:
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

    next_run_count = command.run_count + 1
    next_planning_attempts = command.planning_attempts + 1
    resolved_mode = resolve_command_mode(command, output)

    if output.decision == PlannerDecision.CREATE_TASKS:
        planned_specs = add_mode_control_tasks(resolved_mode, output.tasks)
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
            created_tasks = create_task_records(
                conn,
                command,
                [build_final_test_spec([task.task_key for task in tasks if task.task_key])],
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

        final_response = output.final_response or build_final_response(
            command.model_copy(update={"effective_mode": resolved_mode}),
            tasks,
            [],
        )
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
    tasks: List[TaskRecord],
    questions: List[QuestionRecord],
    reviews: List[ReviewRecord],
    instructions: List[InstructionRecord],
    settings: AppConfig,
) -> Dict[str, Tuple[Optional[WorkerOutput], Optional[str]]]:
    def run_single(task: TaskRecord) -> Tuple[str, Optional[WorkerOutput], Optional[str]]:
        backend = backend_for(command, settings, task.capability)
        try:
            output = backend.execute_task(command, task, tasks, questions, reviews, instructions)
            return task.id, output, None
        except Exception as exc:
            return task.id, None, str(exc)

    results: Dict[str, Tuple[Optional[WorkerOutput], Optional[str]]] = {}
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

    for task in batch:
        update_task(
            conn, task.id, state=TaskState.RUNNING.value, attempt_count=task.attempt_count + 1
        )
        append_event(conn, "task", task.id, "task_started", {"command_id": command.id})

    tasks = list_tasks_for_command(conn, command.id)
    batch_ids = {task.id for task in batch}
    active_batch = [task for task in tasks if task.id in batch_ids]
    questions = list_questions_for_command(conn, command.id)
    reviews = list_reviews_for_command(conn, command.id)
    instructions = list_instructions_for_command(conn, command.id)
    results = execute_task_batch(
        command, active_batch, tasks, questions, reviews, instructions, settings
    )

    updated_tasks: List[TaskRecord] = []
    created_questions: List[QuestionRecord] = []
    failed_any = False
    for task in active_batch:
        output, error = results[task.id]
        if error is not None:
            update_task(
                conn,
                task.id,
                state=TaskState.FAILED.value,
                error=error,
                output_payload=json.dumps({"error": error}, sort_keys=True),
            )
            append_event(
                conn, "task", task.id, "task_failed", {"command_id": command.id, "error": error}
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
            update_task(conn, task.id, state=task_state.value, error=None)
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
                output_payload=json.dumps({"error": failure_reason}, sort_keys=True),
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
    if created_questions:
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

    update_command(
        conn,
        command.id,
        stage=next_stage.value,
        question_state=question_state,
        run_count=command.run_count + len(active_batch),
        failure_reason=None,
        final_response=build_final_response(command, list_tasks_for_command(conn, command.id), [])
        if next_stage == CommandStage.DONE
        else None,
    )
    if next_stage == CommandStage.REPLANNING:
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
        command=command_from_row(command_row),
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
) -> List[Tuple[int, ReviewerOutput]]:
    reviewer_count = settings.role_for("reviewer").count

    def run_single(slot: int) -> Tuple[int, ReviewerOutput]:
        backend = backend_for(command, settings, "reviewer")
        try:
            output = backend.review(
                command, tasks, questions, reviews, instructions, reviewer_slot=slot
            )
            return slot, output
        except Exception as exc:
            return slot, ReviewerOutput(
                decision=ReviewDecision.FAIL,
                summary=f"Reviewer {slot} failed.",
                findings=[],
                failure_reason=str(exc),
            )

    outputs: List[Tuple[int, ReviewerOutput]] = []
    with ThreadPoolExecutor(max_workers=reviewer_count) as executor:
        futures = [executor.submit(run_single, slot) for slot in range(1, reviewer_count + 1)]
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
    outputs = run_review_batch(command, tasks, questions, previous_reviews, instructions, settings)

    reviewer_kind = (
        command.backend.value
        if command.backend != RuntimeBackend.INHERIT
        else settings.role_for("reviewer").runtime.value
    )
    created_reviews = [
        create_review_record(conn, command, output, reviewer_slot=slot, reviewer_kind=reviewer_kind)
        for slot, output in outputs
    ]
    next_run_count = command.run_count + len(outputs)

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
    update_command(
        conn,
        command.id,
        stage=CommandStage.DONE.value,
        final_response=final_response,
        run_count=next_run_count,
        failure_reason=None,
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
        command = select_actionable_command(conn, command_id)
        if command is None:
            return TickOutcome(action="no_op", note="no actionable commands")

        if command.stage == CommandStage.QUEUED:
            outcome = reduce_queued(conn, command)
        elif command.stage in {CommandStage.PLANNING, CommandStage.REPLANNING}:
            outcome = reduce_planning(conn, command, settings)
        elif command.stage == CommandStage.RUNNING:
            outcome = reduce_running(conn, command, settings)
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
            "command_completed",
            "planning_failed",
            "review_failed",
            "dispatch_failed",
        }:
            break
        if outcome.command and outcome.command.stage in {CommandStage.DONE, CommandStage.FAILED}:
            break
    final_command = get_command(db_path, last_command_id) if last_command_id else None
    return {
        "steps": [outcome.model_dump(mode="json") for outcome in outcomes],
        "final_command": final_command.model_dump(mode="json") if final_command else None,
    }
