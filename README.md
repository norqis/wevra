# Wevra

Wevra is a local workflow engine for structured AI execution.

It turns a user command into explicit runtime state, lets AI backends return structured planning / task / review outputs, and keeps orchestration responsibility inside the engine instead of inside a long-lived AI chat session.

## What Exists Now

- SQLite-backed runtime using Python's standard `sqlite3`
- `wevra.ini` and `agents.ini` driven configuration
- dependency-aware task scheduler with per-role concurrency limits
- built-in `mock` backend for deterministic tests and local dogfooding
- optional `codex` and `claude` backends for planner / implementer / reviewer work
- browser dashboard with snapshot API, command submission, question answering, and append-driven replanning
- first-class `command`, `task`, `question`, `review`, `instruction`, and `event` records

## Runtime Model

Each command moves through explicit stages:

- `queued`
- `planning`
- `running`
- `waiting_question`
- `verifying`
- `replanning`
- `done`
- `failed`

The planner emits task specs with explicit `key`, `depends_on`, and `write_files`.  
The engine uses that DAG plus role concurrency settings from `agents.ini` to decide which tasks are ready and which can run in parallel without colliding on write targets.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

No separate SQLite install is required.

## Quick Start

Initialize the repo-local config and database:

```bash
wevra init
```

Or start the runtime surface in one step:

```bash
wevra start
wevra status
```

Submit and run from the CLI:

```bash
wevra submit "Implement a planner-backed workflow"
wevra run
wevra list
wevra tasks
wevra reviews
wevra events
```

Question flow:

```bash
wevra submit "[worker_question] clarify implementation details"
wevra run
wevra questions --open-only
wevra answer <question-id> "Proceed with the existing interface."
wevra run
```

Append an instruction to an existing command:

```bash
wevra append <command-id> "Keep the current work, but also add a final follow-up pass."
wevra run --command-id <command-id>
```

Dashboard flow:

```bash
wevra dashboard start
wevra dashboard status
wevra dashboard stop
```

Default dashboard URL:

```text
http://127.0.0.1:43861
```

## Config

`wevra init` creates:

- `wevra.ini`
- `agents.ini`
- `.env`

`wevra.ini` holds shared runtime settings such as:

- `runtime.working_dir`
- `runtime.db_path`
- `runtime.language`
- `runtime.dangerously_bypass_approvals_and_sandbox`
- `ui.host`
- `ui.port`
- `ui.auto_start`
- `ui.open_browser`

`agents.ini` holds role-level backend settings and pool sizes such as:

- `planner.runtime`
- `planner.model`
- `implementer.runtime`
- `implementer.count`
- `reviewer.runtime`
- `reviewer.count`

## Commands

- `wevra init`
- `wevra start`
- `wevra stop`
- `wevra status`
- `wevra init-db`
- `wevra submit`
- `wevra append`
- `wevra show`
- `wevra list`
- `wevra tasks`
- `wevra questions`
- `wevra answer`
- `wevra reviews`
- `wevra events`
- `wevra tick`
- `wevra run`
- `wevra dashboard start`
- `wevra dashboard stop`
- `wevra dashboard status`

## Development

```bash
pytest -q
```
