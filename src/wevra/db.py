from __future__ import annotations

import sqlite3
from pathlib import Path


SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS commands (
    id TEXT PRIMARY KEY,
    goal TEXT NOT NULL,
    stage TEXT NOT NULL,
    workflow_mode TEXT NOT NULL DEFAULT 'auto',
    approval_mode TEXT NOT NULL DEFAULT 'auto',
    effective_mode TEXT,
    priority TEXT NOT NULL,
    backend TEXT NOT NULL DEFAULT 'inherit',
    workspace_root TEXT NOT NULL DEFAULT '.',
    runbook_path TEXT,
    allow_parallel INTEGER NOT NULL DEFAULT 0,
    resume_stage TEXT,
    resume_hint TEXT,
    final_response TEXT,
    failure_reason TEXT,
    operator_issue_kind TEXT,
    operator_issue_detail TEXT,
    operator_issue_agent_run_id TEXT,
    operator_issue_task_id TEXT,
    operator_issue_role_name TEXT,
    operator_issue_runtime TEXT,
    operator_issue_model TEXT,
    question_state TEXT NOT NULL DEFAULT 'none',
    planning_attempts INTEGER NOT NULL DEFAULT 0,
    run_count INTEGER NOT NULL DEFAULT 0,
    replan_requested INTEGER NOT NULL DEFAULT 0,
    stop_requested INTEGER NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    task_key TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    capability TEXT NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    state TEXT NOT NULL,
    plan_order INTEGER NOT NULL DEFAULT 1,
    input_payload TEXT NOT NULL DEFAULT '{}',
    output_payload TEXT,
    error TEXT,
    operator_issue_kind TEXT,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    assigned_run_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);

CREATE TABLE IF NOT EXISTS command_dependencies (
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    depends_on_command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    PRIMARY KEY (command_id, depends_on_command_id)
);

CREATE TABLE IF NOT EXISTS questions (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    task_id TEXT,
    source TEXT NOT NULL,
    resolution_mode TEXT NOT NULL,
    resume_stage TEXT NOT NULL DEFAULT 'planning',
    question TEXT NOT NULL,
    answer TEXT,
    state TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reviews (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    task_id TEXT,
    reviewer_kind TEXT NOT NULL,
    reviewer_slot INTEGER NOT NULL DEFAULT 1,
    decision TEXT NOT NULL,
    summary TEXT NOT NULL,
    findings_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS instructions (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    task_id TEXT,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    body TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stream_type TEXT NOT NULL,
    stream_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_runs (
    id TEXT PRIMARY KEY,
    command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
    task_id TEXT,
    reviewer_slot INTEGER,
    role_name TEXT NOT NULL,
    capability TEXT NOT NULL,
    runtime TEXT NOT NULL,
    model TEXT NOT NULL DEFAULT '',
    run_kind TEXT NOT NULL,
    title TEXT NOT NULL,
    resume_stage TEXT NOT NULL DEFAULT 'running',
    state TEXT NOT NULL,
    approval_required INTEGER NOT NULL DEFAULT 0,
    prompt_excerpt TEXT,
    output_summary TEXT,
    output_log TEXT NOT NULL DEFAULT '',
    error TEXT,
    process_id INTEGER,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_command_state ON tasks(command_id, state);
CREATE INDEX IF NOT EXISTS idx_tasks_command_key ON tasks(command_id, task_key);
CREATE INDEX IF NOT EXISTS idx_command_dependencies_command ON command_dependencies(command_id);
CREATE INDEX IF NOT EXISTS idx_questions_command_state ON questions(command_id, state);
CREATE INDEX IF NOT EXISTS idx_reviews_command ON reviews(command_id);
CREATE INDEX IF NOT EXISTS idx_instructions_command ON instructions(command_id, created_at);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_type, stream_id, id);
CREATE INDEX IF NOT EXISTS idx_agent_runs_command ON agent_runs(command_id, created_at);
CREATE INDEX IF NOT EXISTS idx_agent_runs_state ON agent_runs(state, created_at);
"""


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("PRAGMA busy_timeout = 10000;")
    conn.execute("PRAGMA journal_mode = WAL;")
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def initialize_database(db_path: Path) -> Path:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        ensure_column(conn, "commands", "workflow_mode", "TEXT NOT NULL DEFAULT 'auto'")
        ensure_column(conn, "commands", "approval_mode", "TEXT NOT NULL DEFAULT 'auto'")
        ensure_column(conn, "commands", "effective_mode", "TEXT")
        ensure_column(conn, "commands", "backend", "TEXT NOT NULL DEFAULT 'inherit'")
        ensure_column(conn, "commands", "workspace_root", "TEXT NOT NULL DEFAULT '.'")
        ensure_column(conn, "commands", "runbook_path", "TEXT")
        ensure_column(conn, "commands", "allow_parallel", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "commands", "resume_stage", "TEXT")
        ensure_column(conn, "commands", "resume_hint", "TEXT")
        ensure_column(conn, "commands", "failure_reason", "TEXT")
        ensure_column(conn, "commands", "operator_issue_kind", "TEXT")
        ensure_column(conn, "commands", "operator_issue_detail", "TEXT")
        ensure_column(conn, "commands", "operator_issue_agent_run_id", "TEXT")
        ensure_column(conn, "commands", "operator_issue_task_id", "TEXT")
        ensure_column(conn, "commands", "operator_issue_role_name", "TEXT")
        ensure_column(conn, "commands", "operator_issue_runtime", "TEXT")
        ensure_column(conn, "commands", "operator_issue_model", "TEXT")
        ensure_column(conn, "commands", "replan_requested", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "commands", "stop_requested", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "tasks", "task_key", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "tasks", "plan_order", "INTEGER NOT NULL DEFAULT 1")
        ensure_column(conn, "tasks", "error", "TEXT")
        ensure_column(conn, "tasks", "operator_issue_kind", "TEXT")
        ensure_column(conn, "tasks", "attempt_count", "INTEGER NOT NULL DEFAULT 0")
        ensure_column(conn, "questions", "resume_stage", "TEXT NOT NULL DEFAULT 'planning'")
        ensure_column(conn, "reviews", "reviewer_slot", "INTEGER NOT NULL DEFAULT 1")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS task_dependencies (
                task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                PRIMARY KEY (task_id, depends_on_task_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS command_dependencies (
                command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
                depends_on_command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
                PRIMARY KEY (command_id, depends_on_command_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS instructions (
                id TEXT PRIMARY KEY,
                command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
                body TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_runs (
                id TEXT PRIMARY KEY,
                command_id TEXT NOT NULL REFERENCES commands(id) ON DELETE CASCADE,
                task_id TEXT,
                reviewer_slot INTEGER,
                role_name TEXT NOT NULL,
                capability TEXT NOT NULL,
                runtime TEXT NOT NULL,
                model TEXT NOT NULL DEFAULT '',
                run_kind TEXT NOT NULL,
                title TEXT NOT NULL,
                resume_stage TEXT NOT NULL DEFAULT 'running',
                state TEXT NOT NULL,
                approval_required INTEGER NOT NULL DEFAULT 0,
                prompt_excerpt TEXT,
                output_summary TEXT,
                output_log TEXT NOT NULL DEFAULT '',
                error TEXT,
                process_id INTEGER,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        ensure_column(conn, "artifacts", "body", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "agent_runs", "resume_stage", "TEXT NOT NULL DEFAULT 'running'")
        ensure_column(conn, "agent_runs", "output_log", "TEXT NOT NULL DEFAULT ''")
        ensure_column(conn, "agent_runs", "process_id", "INTEGER")
        conn.commit()
    return db_path
