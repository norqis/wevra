from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from wevra import __version__
from wevra.config import init_repo_config, load_config
from wevra.dashboard import build_snapshot, dashboard_status, start_dashboard, stop_dashboard
from wevra.db import initialize_database
from wevra.models import Priority, RuntimeBackend, WorkflowMode
from wevra.service import (
    answer_question,
    append_instruction,
    get_command,
    list_commands,
    list_events,
    list_questions,
    list_reviews,
    list_tasks,
    run_engine,
    submit_command,
    tick_once,
)


app = typer.Typer(help="Wevra: structured AI workflow engine.")
dashboard_app = typer.Typer(help="Manage the local Wevra dashboard server.")
app.add_typer(dashboard_app, name="dashboard")


def repo_root() -> Path:
    return Path.cwd().resolve()


def settings():
    return load_config(repo_root())


def resolve_db_path(db_path: Optional[Path]) -> Path:
    if db_path is not None:
        return db_path.resolve()
    return settings().db_path


def print_json(payload) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@app.command("version")
def version() -> None:
    """Print the current Wevra version."""
    typer.echo(__version__)


@app.command("init")
def init() -> None:
    """Create local config files and initialize the runtime database."""
    created = init_repo_config(repo_root())
    config = settings()
    initialize_database(config.db_path)
    print_json(
        {
            "created_files": created,
            "db_path": str(config.db_path),
            "working_dir": str(config.working_dir),
        }
    )


@app.command("start")
def start() -> None:
    """Initialize config/db and start the dashboard when enabled."""
    created = init_repo_config(repo_root())
    config = settings()
    initialize_database(config.db_path)
    dashboard = dashboard_status(repo_root(), config)
    if config.ui_auto_start and not dashboard["running"]:
        dashboard = start_dashboard(repo_root(), config)
    print_json(
        {
            "created_files": created,
            "db_path": str(config.db_path),
            "working_dir": str(config.working_dir),
            "dashboard": dashboard,
        }
    )


@app.command("stop")
def stop() -> None:
    """Stop the local dashboard server."""
    print_json({"dashboard": stop_dashboard(repo_root(), settings())})


@app.command("status")
def status() -> None:
    """Show dashboard and runtime status."""
    config = settings()
    snapshot = build_snapshot(repo_root(), config)
    print_json(
        {
            "dashboard": dashboard_status(repo_root(), config),
            "db_path": str(config.db_path),
            "working_dir": str(config.working_dir),
            "counts": {
                "commands": len(snapshot["commands"]["items"]),
                "open_questions": len(snapshot["questions"]["open"]),
                "tasks": len(snapshot["tasks"]["items"]),
            },
        }
    )


@app.command("init-db")
def init_db(
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Initialize the runtime database."""
    target = resolve_db_path(db_path)
    initialize_database(target)
    typer.echo(f"initialized: {target}")


@app.command("submit")
def submit(
    goal: str = typer.Argument(..., help="Command goal to submit into the runtime."),
    workflow_mode: WorkflowMode = typer.Option(
        WorkflowMode.AUTO,
        "--mode",
        help="Workflow mode: auto, implementation, research, review, or planning.",
    ),
    priority: Priority = typer.Option(Priority.HIGH, help="Priority assigned to the command."),
    backend: Optional[RuntimeBackend] = typer.Option(
        None, help="Optional backend override for this command."
    ),
    workspace_root: Optional[Path] = typer.Option(None, help="Optional workspace root override."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Submit a new command."""
    config = settings()
    command = submit_command(
        resolve_db_path(db_path),
        goal=goal,
        workflow_mode=workflow_mode,
        priority=priority,
        backend=backend,
        workspace_root=workspace_root,
        settings=config,
        repo_root=repo_root(),
    )
    print_json(command.model_dump(mode="json"))


@app.command("append")
def append(
    command_id: str = typer.Argument(
        ..., help="Command identifier that should receive the additional instruction."
    ),
    instruction: str = typer.Argument(
        ..., help="Additional instruction to append to the existing command."
    ),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Append a user instruction to an existing command and request replanning."""
    appended, command = append_instruction(
        resolve_db_path(db_path), command_id=command_id, body=instruction
    )
    print_json(
        {
            "instruction": appended.model_dump(mode="json"),
            "command": command.model_dump(mode="json"),
        }
    )


@app.command("show")
def show(
    command_id: str = typer.Argument(..., help="Command identifier."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Show the current state of a command."""
    command = get_command(resolve_db_path(db_path), command_id)
    if command is None:
        raise typer.Exit(code=1)
    print_json(command.model_dump(mode="json"))


@app.command("list")
def list_command_records(
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """List commands in execution order."""
    commands = list_commands(resolve_db_path(db_path))
    print_json({"commands": [command.model_dump(mode="json") for command in commands]})


@app.command("tasks")
def tasks(
    command_id: Optional[str] = typer.Option(
        None, help="Optional command identifier to filter by."
    ),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """List task records."""
    print_json(
        {
            "tasks": [
                task.model_dump(mode="json")
                for task in list_tasks(resolve_db_path(db_path), command_id=command_id)
            ]
        }
    )


@app.command("questions")
def questions(
    command_id: Optional[str] = typer.Option(
        None, help="Optional command identifier to filter by."
    ),
    open_only: bool = typer.Option(False, help="Only show open questions."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """List question records."""
    print_json(
        {
            "questions": [
                question.model_dump(mode="json")
                for question in list_questions(
                    resolve_db_path(db_path), command_id=command_id, open_only=open_only
                )
            ]
        }
    )


@app.command("answer")
def answer(
    question_id: str = typer.Argument(..., help="Question identifier."),
    answer_text: str = typer.Argument(..., help="Answer that should unblock the runtime."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Answer an open question."""
    question = answer_question(
        resolve_db_path(db_path), question_id=question_id, answer=answer_text
    )
    print_json(question.model_dump(mode="json"))


@app.command("reviews")
def reviews(
    command_id: Optional[str] = typer.Option(
        None, help="Optional command identifier to filter by."
    ),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """List review records."""
    print_json(
        {
            "reviews": [
                review.model_dump(mode="json")
                for review in list_reviews(resolve_db_path(db_path), command_id=command_id)
            ]
        }
    )


@app.command("events")
def events(
    command_id: Optional[str] = typer.Option(
        None, help="Optional command identifier to filter by."
    ),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """List runtime events."""
    print_json(
        {
            "events": [
                event.model_dump(mode="json")
                for event in list_events(resolve_db_path(db_path), command_id=command_id)
            ]
        }
    )


@app.command("tick")
def tick(
    command_id: Optional[str] = typer.Option(None, help="Optional command identifier to target."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Advance the engine by one deterministic step."""
    outcome = tick_once(
        resolve_db_path(db_path), command_id=command_id, settings=settings(), repo_root=repo_root()
    )
    print_json(outcome.model_dump(mode="json"))


@app.command("run")
def run(
    command_id: Optional[str] = typer.Option(None, help="Optional command identifier to target."),
    max_steps: int = typer.Option(100, min=1, help="Maximum reducer steps to execute."),
    db_path: Optional[Path] = typer.Option(
        None, help="Optional path to the SQLite runtime database."
    ),
) -> None:
    """Run the engine until it blocks, finishes, or reaches the step limit."""
    print_json(
        run_engine(
            resolve_db_path(db_path),
            command_id=command_id,
            max_steps=max_steps,
            settings=settings(),
            repo_root=repo_root(),
        )
    )


@dashboard_app.command("start")
def dashboard_start() -> None:
    """Start the local dashboard server."""
    print_json(start_dashboard(repo_root(), settings()))


@dashboard_app.command("stop")
def dashboard_stop() -> None:
    """Stop the local dashboard server."""
    print_json(stop_dashboard(repo_root(), settings()))


@dashboard_app.command("status")
def dashboard_show_status() -> None:
    """Show dashboard server status."""
    print_json(dashboard_status(repo_root(), settings()))


if __name__ == "__main__":
    app()
