import configparser
import json
import threading
import time
from contextlib import contextmanager
from urllib import request

import pytest
from playwright.sync_api import expect, sync_playwright
from typer.testing import CliRunner

import wevra.service as service_module
from wevra.cli import app
from wevra.config import load_config
from wevra.dashboard import create_server
from wevra.service import list_agent_runs, list_commands, list_instructions, list_reviews


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


def wait_for_refresh_condition(page, predicate, refresh_selector="#refreshBtn", attempts=12):
    last_error = None
    for _ in range(attempts):
        try:
            if predicate():
                return
        except AssertionError as exc:
            last_error = exc
        page.locator(refresh_selector).click()
        page.wait_for_timeout(300)
    if last_error:
        raise last_error
    raise AssertionError("Condition was not satisfied after refresh attempts.")


def wait_for_python_condition(predicate, attempts=12, delay_seconds=0.3):
    last_result = None
    for _ in range(attempts):
        last_result = predicate()
        if last_result:
            return last_result
        time.sleep(delay_seconds)
    raise AssertionError("Condition was not satisfied after polling.") from None


@contextmanager
def browser_page(viewport=None):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport=viewport or {"width": 1600, "height": 1080})
        page.set_default_timeout(15000)
        try:
            yield page
        finally:
            browser.close()


@contextmanager
def dashboard_server(tmp_path):
    server = create_server(tmp_path, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def test_dashboard_browser_submit_and_view_result(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#goal").fill("implement happy path")
        page.locator("#submitBtn").click()

        expect(page.locator("#commandsList .command-item")).to_have_count(1)
        expect(page.locator("#engineOwnerBadge")).to_contain_text("Dashboard")
        expect(page.locator("#viewResultBtn")).to_be_enabled()
        expect(page.locator("#openAppendBtn")).to_be_disabled()

        page.locator("#viewResultBtn").click()
        expect(page.locator("#resultModal")).to_be_visible()
        expect(page.locator("#resultModalBody")).to_contain_text("Completed tasks:")


def test_dashboard_browser_question_answer_survives_refresh_and_resumes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#goal").fill("[worker_question] answer from browser dashboard")
        page.locator("#submitBtn").click()

        expect(page.locator("#questionAlert")).to_be_visible()
        answer_input = page.locator("#questionAlertAnswer")
        answer_text = "Proceed with the current approach."
        answer_input.fill(answer_text)

        post_json(
            f"{base_url}/api/commands",
            {"goal": "background request to force a snapshot refresh"},
        )
        page.locator("#refreshBtn").click()

        expect(page.locator("#questionAlertAnswer")).to_have_value(answer_text)
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#questionAlert")).not_to_be_visible()
        expect(page.locator("#viewResultBtn")).to_be_enabled()


def test_dashboard_browser_append_instruction_in_japanese_locale(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)
    submitted = read_json(runner.invoke(app, ["submit", "[worker_question] dashboard append path"]))
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        expect(page.locator("#submitBtn")).to_have_text("依頼を投入")
        expect(page.locator("#openAppendBtn")).to_have_text("追加指示")
        expect(page.locator("#openAppendBtn")).to_be_enabled()

        page.locator("#openAppendBtn").click()
        expect(page.locator("#appendModal")).to_be_visible()
        page.locator("#appendModalInput").fill("[append_extra] ブラウザから追記")
        page.locator("#appendModalSubmit").click()

        wait_for_python_condition(
            lambda: any(
                instruction.body == "[append_extra] ブラウザから追記"
                for instruction in list_instructions(settings.db_path, command_id=command_id)
            )
        )
        page.locator("#overviewTab").click()

        page.locator("#questionAlertAnswer").fill("このまま進めてください。")
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()


@pytest.mark.parametrize(
    ("mode", "goal", "expect_reviews"),
    [
        ("research", "investigate the current architecture", False),
        ("review", "review the existing changes", True),
        ("planning", "design a rollout plan", False),
    ],
)
def test_dashboard_browser_workflow_modes_complete_from_ui(
    tmp_path, monkeypatch, mode, goal, expect_reviews
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#workflowMode").select_option(mode)
        page.locator("#goal").fill(goal)
        page.locator("#submitBtn").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        expect(page.locator("#openAppendBtn")).to_be_disabled()
        if expect_reviews:
            wait_for_python_condition(lambda: len(list_reviews(settings.db_path)) == 2)
        page.locator("#refreshBtn").click()
        page.locator("#reviewsTab").click()
        if expect_reviews:
            expect(page.locator("#reviewsList .card")).to_have_count(2)
        else:
            expect(page.locator("#reviewsList")).to_contain_text("No reviews yet.")


def test_dashboard_browser_agents_tab_handles_manual_approval(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    agents_path = tmp_path / "agents.ini"
    parser = configparser.ConfigParser()
    parser.read(agents_path, encoding="utf-8")
    parser.set("implementer", "runtime", "codex")
    parser.set("implementer", "model", "mock-implementer")
    with agents_path.open("w", encoding="utf-8") as handle:
        parser.write(handle)
    settings = load_config(tmp_path)
    monkeypatch.setattr(
        service_module, "backend_for", lambda *args, **kwargs: service_module.MockBackend()
    )

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#workflowMode").select_option("implementation")
        page.locator("#goal").fill("agent approval coverage")
        page.locator("#submitBtn").click()

        wait_for_python_condition(
            lambda: any(
                agent_run.state.value == "pending_approval"
                for agent_run in list_agent_runs(settings.db_path)
            )
        )
        page.locator("#refreshBtn").click()
        page.locator("#agentsTab").click()
        expect(page.locator("[data-agent-allow]")).to_be_visible()
        page.locator("[data-agent-allow]").click()

        wait_for_python_condition(
            lambda: any(command.final_response for command in list_commands(settings.db_path)),
            attempts=40,
            delay_seconds=0.25,
        )
        page.locator("#refreshBtn").click()
        page.locator("#overviewTab").click()
        expect(page.locator("#viewResultBtn")).to_be_enabled()


def test_dashboard_browser_selection_language_switch_and_working_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    workspace_a = (tmp_path / "workspace-a").resolve()
    workspace_b = (tmp_path / "workspace-b").resolve()
    first = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--mode",
                "research",
                "--workspace-root",
                str(workspace_a),
                "investigate the current architecture",
            ],
        )
    )
    second = read_json(
        runner.invoke(
            app,
            [
                "submit",
                "--mode",
                "review",
                "--workspace-root",
                str(workspace_b),
                "review the existing changes",
            ],
        )
    )
    read_json(runner.invoke(app, ["run", "--command-id", first["id"]]))
    read_json(runner.invoke(app, ["run", "--command-id", second["id"]]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        expect(page.locator("#commandsList .command-item")).to_have_count(2)
        page.locator("#languageSelect").select_option("ja")
        expect(page.locator("#submitBtn")).to_have_text("依頼を投入")

        page.locator(f'[data-command-id="{second["id"]}"]').click()
        expect(page.locator("#commandDetail")).to_contain_text("作業ディレクトリ")
        expect(page.locator("#commandDetail")).to_contain_text(str(workspace_b))

        page.locator("#tasksTab").click()
        expect(page.locator("#tasksList")).to_contain_text("Collect review context")
        page.locator("#activityTab").click()
        expect(page.locator("#instructionsList")).to_contain_text("追加指示はありません。")


def test_dashboard_browser_state_classes_and_result_modal_close(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    completed = read_json(runner.invoke(app, ["submit", "completed request"]))
    waiting = read_json(runner.invoke(app, ["submit", "[worker_question] waiting request"]))
    read_json(runner.invoke(app, ["run", "--command-id", completed["id"]]))
    read_json(runner.invoke(app, ["run", "--command-id", waiting["id"]]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        completed_class = page.locator(f'[data-command-id="{completed["id"]}"]').get_attribute(
            "class"
        )
        waiting_class = page.locator(f'[data-command-id="{waiting["id"]}"]').get_attribute("class")
        assert "done-state" in completed_class
        assert "attention" in waiting_class

        page.locator(f'[data-command-id="{completed["id"]}"]').click()
        expect(page.locator("#viewResultBtn")).to_be_enabled()
        page.locator("#viewResultBtn").click()
        expect(page.locator("#resultModal")).to_be_visible()
        page.locator("#closeResultBtn").click()
        expect(page.locator("#resultModal")).not_to_be_visible()


def test_dashboard_browser_mobile_question_flow(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with (
        dashboard_server(tmp_path) as base_url,
        browser_page({"width": 390, "height": 844}) as page,
    ):
        page.goto(f"{base_url}/?lang=ja")

        expect(page.locator("#goal")).to_be_visible()
        expect(page.locator("#submitBtn")).to_have_text("依頼を投入")
        page.locator("#goal").fill("[worker_question] mobile browser flow")
        page.locator("#submitBtn").click()

        expect(page.locator("#questionAlert")).to_be_visible()
        page.locator("#questionAlertAnswer").fill("モバイルから継続してください。")
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#questionAlert")).not_to_be_visible()
        expect(page.locator("#viewResultBtn")).to_be_enabled()
