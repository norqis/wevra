import json
import sqlite3
import threading
from urllib import request

from typer.testing import CliRunner

from wevra.cli import app
from wevra.dashboard import create_server


runner = CliRunner()


def read_json(result):
    assert result.exit_code == 0, result.stdout
    return json.loads(result.stdout)


def post_json(url: str, payload: dict):
    req = request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req) as response:
        return json.loads(response.read().decode("utf-8"))


def get_json(url: str):
    with request.urlopen(url) as response:
        return json.loads(response.read().decode("utf-8"))


def test_init_creates_config_and_db(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["init"])
    payload = read_json(result)

    assert (tmp_path / "wevra.ini").exists()
    assert (tmp_path / "agents.ini").exists()
    assert (tmp_path / ".env").exists()
    assert (tmp_path / ".wevra" / "wevra.db").exists()
    assert payload["db_path"].endswith(".wevra/wevra.db")

    conn = sqlite3.connect(str(tmp_path / ".wevra" / "wevra.db"))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    finally:
        conn.close()
    names = {row[0] for row in rows}
    assert {"commands", "tasks", "task_dependencies", "questions", "reviews", "artifacts", "events", "instructions"} <= names


def test_run_happy_path_completes_command(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "implement happy path"]))
    command_id = submitted["id"]

    run_result = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert run_result["final_command"]["stage"] == "done"
    assert run_result["final_command"]["effective_mode"] == "implementation"
    assert "Completed tasks:" in run_result["final_command"]["final_response"]
    assert any(step["action"] == "command_completed" for step in run_result["steps"])

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert all(task["state"] == "done" for task in tasks["tasks"])
    assert any(task["capability"] == "tester" for task in tasks["tasks"])

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert len(reviews["reviews"]) == 2


def test_worker_question_can_be_answered_and_resumed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[worker_question] worker asks once"]))
    command_id = submitted["id"]

    blocked = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert blocked["final_command"]["stage"] == "waiting_question"

    questions = read_json(runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"]))
    question_id = questions["questions"][0]["id"]

    answered = read_json(runner.invoke(app, ["answer", question_id, "Continue."]))
    assert answered["state"] == "answered"

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"


def test_parallel_plan_creates_dependency_graph(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[parallel] split the work"]))
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert len(tasks["tasks"]) == 4
    integration = next(task for task in tasks["tasks"] if task["task_key"] == "integration")
    assert len(integration["depends_on"]) == 2
    tester = next(task for task in tasks["tasks"] if task["task_key"] == "__system_test_gate__")
    assert len(tester["depends_on"]) == 3
    assert all(task["state"] == "done" for task in tasks["tasks"])


def test_research_mode_skips_final_review(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "--mode", "research", "investigate the current architecture"]))
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"
    assert completed["final_command"]["effective_mode"] == "research"
    assert "Completed tasks:" in completed["final_command"]["final_response"]

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert [task["capability"] for task in tasks["tasks"]] == ["investigation", "analyst"]

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert reviews["reviews"] == []


def test_review_mode_runs_review_without_tester_gate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "--mode", "review", "review the existing changes"]))
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"
    assert completed["final_command"]["effective_mode"] == "review"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert all(task["capability"] != "tester" for task in tasks["tasks"])

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert len(reviews["reviews"]) == 2


def test_planning_mode_stops_after_plan_output(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "--mode", "planning", "design a rollout plan"]))
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"
    assert completed["final_command"]["effective_mode"] == "planning"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert [task["capability"] for task in tasks["tasks"]] == ["analyst"]

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert reviews["reviews"] == []


def test_append_instruction_replans_after_current_batch(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[parallel] build the first pass"]))
    command_id = submitted["id"]

    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    batch = read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    assert batch["command"]["stage"] == "running"

    before_append = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    integration_before = next(task for task in before_append["tasks"] if task["task_key"] == "integration")
    assert integration_before["state"] == "pending"

    appended = read_json(runner.invoke(app, ["append", command_id, "[append_extra] add a final follow-up task"]))
    assert appended["command"]["replan_requested"] is True

    replan = read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    assert replan["action"] == "replanning_requested"
    assert replan["command"]["stage"] == "replanning"

    still_pending = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    integration_pending = next(task for task in still_pending["tasks"] if task["task_key"] == "integration")
    assert integration_pending["state"] == "pending"

    replanned = read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    assert replanned["command"]["stage"] == "running"

    after_replan = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    followup = next(task for task in after_replan["tasks"] if task["task_key"] == "append_followup")
    assert followup["state"] == "pending"
    assert len(followup["depends_on"]) == 1

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"


def test_dashboard_api_answers_question_and_resumes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    read_json(runner.invoke(app, ["submit", "[worker_question] answer from dashboard"]))
    read_json(runner.invoke(app, ["run"]))

    server = create_server(tmp_path, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        snapshot = get_json(f"http://{host}:{port}/api/snapshot")
        assert len(snapshot["questions"]["open"]) == 1
        question_id = snapshot["questions"]["open"][0]["id"]

        answered = post_json(
            f"http://{host}:{port}/api/questions/answer",
            {"question_id": question_id, "answer": "Proceed with the current approach."},
        )
        assert answered["ok"] is True
        assert answered["result"]["final_command"]["stage"] == "done"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_dashboard_api_appends_instruction_and_replans(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    submitted = read_json(runner.invoke(app, ["submit", "[parallel] dashboard append path"]))
    command_id = submitted["id"]

    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))

    server = create_server(tmp_path, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        appended = post_json(
            f"http://{host}:{port}/api/commands/append",
            {"command_id": command_id, "body": "[append_extra] append from dashboard"},
        )
        assert appended["ok"] is True
        assert appended["command"]["replan_requested"] is True

        result = post_json(
            f"http://{host}:{port}/api/commands/run",
            {"command_id": command_id, "max_steps": 20},
        )
        assert result["ok"] is True
        assert result["result"]["final_command"]["stage"] == "done"

        snapshot = get_json(f"http://{host}:{port}/api/snapshot")
        assert any(item["command_id"] == command_id for item in snapshot["instructions"]["items"])
        assert any(task["task_key"] == "append_followup" for task in snapshot["tasks"]["items"])
    finally:
        server.shutdown()
        thread.join(timeout=5)
