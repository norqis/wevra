# Wevra

[日本語](README.ja.md)

Wevra is a local workflow engine for structured AI execution.

It turns a user command into explicit runtime state, lets AI backends return structured planning / task / review outputs, and keeps orchestration responsibility inside the engine instead of inside a long-lived AI chat session.

## What Exists Now

- SQLite-backed runtime using Python's standard `sqlite3`
- `wevra.ini` and `agents.ini` driven configuration
- workflow modes: `auto`, `implementation`, `research`, `review`, and `planning`
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

## Workflow Modes

- `auto`
  Wevra chooses the most suitable workflow from the request.
- `implementation`
  Use this when you want Wevra to build, change, or fix something. It can research first, then implement the work, run the existing tests, and keep iterating until the final review passes. The final outcome is stored as the command result and can be viewed in the dashboard.
- `research`
  Use this when you want an investigation, comparison, or report. Wevra gathers findings, analyzes them, and stores the written conclusion as the command result instead of treating the request as an implementation task.
- `review`
  Use this when you want feedback on the current workspace or changes. Wevra inspects what already exists and stores the review findings, risks, and follow-up points as the command result.
- `planning`
  Use this when you want a plan before writing code. Wevra turns the request into a design, rollout plan, or task breakdown and stores that plan as the command result without carrying it through implementation.

## Workflow

Typical execution looks like this:

1. Submit a request from the CLI or dashboard and choose a mode.
2. Wevra breaks the request into the steps needed for that mode.
3. If the work needs research first, it does that before moving on.
4. Ready steps run in order, with independent work running in parallel when it is safe.
5. If clarification is needed, Wevra pauses and asks the user.
6. If the user adds new instructions, the current in-flight work is allowed to finish, then the plan is updated.
7. In `implementation` mode, once the implementation tasks are finished, Wevra runs the existing feature and unit tests.
8. After tests pass, Wevra runs the final review pass.
9. The work is only marked complete when every reviewer approves. If any reviewer requests changes, the work goes through another pass and the full review is repeated.

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
wevra submit --mode implementation "Implement a planner-backed workflow"
wevra run
wevra list
wevra tasks
wevra reviews
wevra events
```

Research-only flow:

```bash
wevra submit --mode research "Investigate the current architecture and summarize the tradeoffs"
wevra run
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

The dashboard keeps a command history. Selecting a command shows its current details, appended instructions, and final result when one is available.

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
