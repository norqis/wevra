from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class Priority(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class RuntimeBackend(str, Enum):
    INHERIT = "inherit"
    MOCK = "mock"
    CODEX = "codex"
    CLAUDE = "claude"


class WorkflowMode(str, Enum):
    AUTO = "auto"
    IMPLEMENTATION = "implementation"
    RESEARCH = "research"
    REVIEW = "review"
    PLANNING = "planning"
    DOGFOODING = "dogfooding"


class ApprovalMode(str, Enum):
    AUTO = "auto"
    MANUAL = "manual"


class OperatorIssueKind(str, Enum):
    PROVIDER_LIMIT = "provider_limit"
    AUTH_REQUIRED = "auth_required"
    INTERACTIVE_PROMPT = "interactive_prompt"
    RUNTIME_TIMEOUT = "runtime_timeout"
    RUNTIME_INTERRUPTED = "runtime_interrupted"


class CommandStage(str, Enum):
    QUEUED = "queued"
    PLANNING = "planning"
    RUNNING = "running"
    WAITING_APPROVAL = "waiting_approval"
    WAITING_OPERATOR = "waiting_operator"
    WAITING_QUESTION = "waiting_question"
    VERIFYING = "verifying"
    REPLANNING = "replanning"
    PAUSED = "paused"
    CANCELED = "canceled"
    DONE = "done"
    FAILED = "failed"


class TaskState(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    BLOCKED = "blocked"
    DONE = "done"
    FAILED = "failed"
    CANCELED = "canceled"


class QuestionState(str, Enum):
    OPEN = "open"
    ANSWERED = "answered"
    RESOLVED = "resolved"


class QuestionResolutionMode(str, Enum):
    RESUME_TASK = "resume_task"
    REPLAN_COMMAND = "replan_command"


class ReviewDecision(str, Enum):
    APPROVE = "approve"
    REQUEST_CHANGES = "request_changes"
    FAIL = "fail"


class AgentRunState(str, Enum):
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    DENIED = "denied"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class AgentRunKind(str, Enum):
    PLANNER = "planner"
    TASK = "task"
    REVIEW = "review"


class PlannerDecision(str, Enum):
    CREATE_TASKS = "create_tasks"
    ASK_QUESTION = "ask_question"
    COMPLETE = "complete"
    FAIL = "fail"


class WorkerDecision(str, Enum):
    COMPLETE = "complete"
    ASK_QUESTION = "ask_question"
    FAIL = "fail"


class CommandRecord(BaseModel):
    id: str
    goal: str
    stage: CommandStage
    workflow_mode: WorkflowMode = WorkflowMode.AUTO
    approval_mode: ApprovalMode = ApprovalMode.AUTO
    effective_mode: Optional[WorkflowMode] = None
    priority: Priority
    backend: RuntimeBackend
    workspace_root: str
    runbook_path: Optional[str] = None
    allow_parallel: bool = False
    depends_on: List[str] = Field(default_factory=list)
    dependency_state: str = "none"
    blocking_dependency_ids: List[str] = Field(default_factory=list)
    can_ignore_dependencies: bool = False
    resume_stage: Optional[CommandStage] = None
    resume_hint: Optional[str] = None
    created_at: str
    updated_at: str
    final_response: Optional[str] = None
    failure_reason: Optional[str] = None
    operator_issue_kind: Optional[OperatorIssueKind] = None
    operator_issue_detail: Optional[str] = None
    operator_issue_agent_run_id: Optional[str] = None
    operator_issue_task_id: Optional[str] = None
    operator_issue_role_name: Optional[str] = None
    operator_issue_runtime: Optional[RuntimeBackend] = None
    operator_issue_model: Optional[str] = None
    question_state: str = "none"
    planning_attempts: int = 0
    run_count: int = 0
    replan_requested: bool = False
    stop_requested: bool = False
    version: int = 1


class TaskRecord(BaseModel):
    id: str
    command_id: str
    task_key: str
    kind: str
    capability: str
    title: str
    description: str
    state: TaskState
    plan_order: int
    depends_on: List[str] = Field(default_factory=list)
    input_payload: Dict[str, Any] = Field(default_factory=dict)
    output_payload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    operator_issue_kind: Optional[OperatorIssueKind] = None
    attempt_count: int = 0
    assigned_run_id: Optional[str] = None
    created_at: str
    updated_at: str


class QuestionRecord(BaseModel):
    id: str
    command_id: str
    task_id: Optional[str] = None
    source: str
    resolution_mode: QuestionResolutionMode
    resume_stage: CommandStage
    question: str
    answer: Optional[str] = None
    state: QuestionState
    created_at: str
    updated_at: str


class ReviewRecord(BaseModel):
    id: str
    command_id: str
    task_id: Optional[str] = None
    reviewer_kind: str
    reviewer_slot: int = 1
    decision: ReviewDecision
    summary: str
    findings: List[str] = Field(default_factory=list)
    created_at: str
    updated_at: str


class InstructionRecord(BaseModel):
    id: str
    command_id: str
    body: str
    created_at: str


class ArtifactRecord(BaseModel):
    id: str
    command_id: str
    task_id: Optional[str] = None
    kind: str
    uri: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    body: str = ""
    created_at: str


class AgentRunRecord(BaseModel):
    id: str
    command_id: str
    task_id: Optional[str] = None
    reviewer_slot: Optional[int] = None
    role_name: str
    capability: str
    runtime: RuntimeBackend
    model: str = ""
    run_kind: AgentRunKind
    title: str
    resume_stage: CommandStage
    state: AgentRunState
    approval_required: bool = False
    prompt_excerpt: Optional[str] = None
    output_summary: Optional[str] = None
    output_log: str = ""
    error: Optional[str] = None
    process_id: Optional[int] = None
    created_at: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    updated_at: str


class EventRecord(BaseModel):
    id: int
    stream_type: str
    stream_id: str
    event_type: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: str


class PlannerTaskSpec(BaseModel):
    key: str
    kind: str
    capability: str
    title: str
    description: str
    depends_on: List[str] = Field(default_factory=list)
    write_files: List[str] = Field(default_factory=list)
    input_payload: Dict[str, Any] = Field(default_factory=dict)


class PlannerOutput(BaseModel):
    decision: PlannerDecision
    workflow_mode: Optional[WorkflowMode] = None
    tasks: List[PlannerTaskSpec] = Field(default_factory=list)
    question: Optional[str] = None
    final_response: Optional[str] = None
    failure_reason: Optional[str] = None


class JobSplitDraftItem(BaseModel):
    key: str
    title: str
    goal: str
    workflow_mode: WorkflowMode
    workspace_path: str = "."
    depends_on: List[str] = Field(default_factory=list)
    allow_parallel: bool = False
    runbook_path: Optional[str] = None
    rationale: Optional[str] = None


class JobSplitDraftOutput(BaseModel):
    summary: Optional[str] = None
    items: List[JobSplitDraftItem] = Field(default_factory=list)
    failure_reason: Optional[str] = None


class JobSplitPreviewItem(BaseModel):
    key: str
    title: str
    goal: str
    workflow_mode: WorkflowMode
    workspace_root: str
    depends_on: List[str] = Field(default_factory=list)
    allow_parallel: bool = False
    runbook_path: Optional[str] = None
    rationale: Optional[str] = None


class JobSplitPreview(BaseModel):
    summary: Optional[str] = None
    items: List[JobSplitPreviewItem] = Field(default_factory=list)


class WorkerOutput(BaseModel):
    decision: WorkerDecision
    summary: Optional[str] = None
    result: Dict[str, Any] = Field(default_factory=dict)
    question: Optional[str] = None
    resolution_mode: Optional[QuestionResolutionMode] = None
    failure_reason: Optional[str] = None


class ReviewerOutput(BaseModel):
    decision: ReviewDecision
    summary: str
    findings: List[str] = Field(default_factory=list)
    failure_reason: Optional[str] = None


class TickOutcome(BaseModel):
    action: str
    command: Optional[CommandRecord] = None
    task: Optional[TaskRecord] = None
    tasks: List[TaskRecord] = Field(default_factory=list)
    question: Optional[QuestionRecord] = None
    review: Optional[ReviewRecord] = None
    note: Optional[str] = None
