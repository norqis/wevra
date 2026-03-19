import json
import sqlite3
import threading
from urllib import request

from typer.testing import CliRunner

from wevra.cli import app
from wevra.dashboard import create_server
from wevra.models import RuntimeBackend
from wevra.service import StructuredCliBackend


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
    assert {
        "commands",
        "tasks",
        "task_dependencies",
        "questions",
        "reviews",
        "artifacts",
        "events",
        "instructions",
    } <= names


def test_runtime_home_overrides_are_applied_to_external_clis(tmp_path, monkeypatch):
    runtime_home = tmp_path / "runtime-home"
    monkeypatch.setenv("HOME", "/tmp/original-home")

    backend = StructuredCliBackend(
        RuntimeBackend.CODEX,
        model="",
        danger=False,
        runtime_home=runtime_home,
    )

    codex_env = backend._build_runtime_env()
    assert codex_env["HOME"] == str(runtime_home)
    assert runtime_home.is_dir()

    claude_env = backend._build_runtime_env()
    assert claude_env["HOME"] == str(runtime_home)


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

    questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
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

    submitted = read_json(
        runner.invoke(app, ["submit", "--mode", "research", "investigate the current architecture"])
    )
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

    submitted = read_json(
        runner.invoke(app, ["submit", "--mode", "review", "review the existing changes"])
    )
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"
    assert completed["final_command"]["effective_mode"] == "review"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert all(task["capability"] != "tester" for task in tasks["tasks"])

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert len(reviews["reviews"]) == 2


def test_auto_mode_infers_non_implementation_flows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    cases = [
        ("investigate the current architecture", "research", ["investigation", "analyst"], 0),
        ("review the existing changes", "review", ["analyst"], 2),
        ("design a rollout plan", "planning", ["analyst"], 0),
    ]

    for goal, expected_mode, expected_capabilities, expected_review_count in cases:
        submitted = read_json(runner.invoke(app, ["submit", goal]))
        command_id = submitted["id"]

        completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
        assert completed["final_command"]["stage"] == "done"
        assert completed["final_command"]["effective_mode"] == expected_mode

        tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
        assert [task["capability"] for task in tasks["tasks"]] == expected_capabilities

        reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
        assert len(reviews["reviews"]) == expected_review_count


def test_planning_mode_stops_after_plan_output(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(
        runner.invoke(app, ["submit", "--mode", "planning", "design a rollout plan"])
    )
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"
    assert completed["final_command"]["effective_mode"] == "planning"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert [task["capability"] for task in tasks["tasks"]] == ["analyst"]

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert reviews["reviews"] == []


def test_planner_question_can_be_answered_and_resumed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[planner_question] clarify before planning"]))
    command_id = submitted["id"]

    blocked = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert blocked["final_command"]["stage"] == "waiting_question"

    questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
    question = questions["questions"][0]
    assert question["source"] == "planner"
    assert question["resolution_mode"] == "replan_command"

    read_json(runner.invoke(app, ["answer", question["id"], "Proceed with the default plan."]))

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"


def test_worker_replan_question_cancels_current_task_before_replanning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(
        runner.invoke(
            app,
            ["submit", "--mode", "implementation", "[worker_replan] replan during execution"],
        )
    )
    command_id = submitted["id"]

    blocked = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert blocked["final_command"]["stage"] == "waiting_question"

    questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
    question = questions["questions"][0]
    assert question["resolution_mode"] == "replan_command"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert tasks["tasks"][0]["state"] == "canceled"

    read_json(runner.invoke(app, ["answer", question["id"], "Change the plan and continue."]))
    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"


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
    integration_before = next(
        task for task in before_append["tasks"] if task["task_key"] == "integration"
    )
    assert integration_before["state"] == "pending"

    appended = read_json(
        runner.invoke(app, ["append", command_id, "[append_extra] add a final follow-up task"])
    )
    assert appended["command"]["replan_requested"] is True

    replan = read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    assert replan["action"] == "replanning_requested"
    assert replan["command"]["stage"] == "replanning"

    still_pending = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    integration_pending = next(
        task for task in still_pending["tasks"] if task["task_key"] == "integration"
    )
    assert integration_pending["state"] == "pending"

    replanned = read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    assert replanned["command"]["stage"] == "running"

    after_replan = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    followup = next(task for task in after_replan["tasks"] if task["task_key"] == "append_followup")
    assert followup["state"] == "pending"
    assert len(followup["depends_on"]) == 1

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"


def test_append_instruction_while_waiting_question_resolves_old_question(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[worker_question] append while blocked"]))
    command_id = submitted["id"]

    blocked = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert blocked["final_command"]["stage"] == "waiting_question"

    appended = read_json(
        runner.invoke(app, ["append", command_id, "[append_extra] replace the blocked direction"])
    )
    assert appended["command"]["stage"] == "replanning"
    assert appended["command"]["question_state"] == "none"

    open_questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
    assert open_questions["questions"] == []

    all_questions = read_json(runner.invoke(app, ["questions", "--command-id", command_id]))
    assert all_questions["questions"][0]["state"] == "resolved"
    assert all_questions["questions"][0]["answer"] == "[superseded by appended instruction]"

    resumed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert resumed["final_command"]["stage"] == "waiting_question"

    new_questions = read_json(
        runner.invoke(app, ["questions", "--command-id", command_id, "--open-only"])
    )
    read_json(runner.invoke(app, ["answer", new_questions["questions"][0]["id"], "Continue."]))

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    assert any(task["task_key"] == "append_followup" for task in tasks["tasks"])


def test_worker_failure_creates_retry_task_and_recovers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(runner.invoke(app, ["submit", "[worker_fail] recover after one failure"]))
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    retry_tasks = [task for task in tasks["tasks"] if task["task_key"].startswith("retry_")]
    assert len(retry_tasks) == 1
    assert retry_tasks[0]["state"] == "done"
    assert retry_tasks[0]["input_payload"]["retry_of"]
    assert any(task["state"] == "canceled" for task in tasks["tasks"])


def test_review_changes_in_implementation_mode_reruns_tester_and_reviews(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(
        runner.invoke(
            app,
            ["submit", "--mode", "implementation", "[review_changes] rerun tests after review rework"],
        )
    )
    command_id = submitted["id"]

    completed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert completed["final_command"]["stage"] == "done"

    tasks = read_json(runner.invoke(app, ["tasks", "--command-id", command_id]))
    tester = next(task for task in tasks["tasks"] if task["capability"] == "tester")
    assert tester["attempt_count"] == 2
    assert any(task["task_key"] == "review_rework" for task in tasks["tasks"])

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert len(reviews["reviews"]) == 4
    assert any(review["decision"] == "request_changes" for review in reviews["reviews"])


def test_review_fail_marks_command_failed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    submitted = read_json(
        runner.invoke(app, ["submit", "--mode", "implementation", "[review_fail] fail in review"])
    )
    command_id = submitted["id"]

    failed = read_json(runner.invoke(app, ["run", "--command-id", command_id]))
    assert failed["final_command"]["stage"] == "failed"
    assert "failed hard" in failed["final_command"]["failure_reason"]

    reviews = read_json(runner.invoke(app, ["reviews", "--command-id", command_id]))
    assert len(reviews["reviews"]) == 2
    assert all(review["decision"] == "fail" for review in reviews["reviews"])


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
