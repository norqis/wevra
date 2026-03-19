import json
import threading
from contextlib import contextmanager
from urllib import request

from playwright.sync_api import expect, sync_playwright
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


@contextmanager
def browser_page():
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1080})
        page.set_default_timeout(15000)
        try:
            yield page
        finally:
            browser.close()


@contextmanager
def dashboard_server(tmp_path):
    server = create_server(tmp_path, "127.0.0.1", 0)
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
    submitted = read_json(runner.invoke(app, ["submit", "[parallel] dashboard append path"]))
    command_id = submitted["id"]

    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))
    read_json(runner.invoke(app, ["tick", "--command-id", command_id]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        expect(page.locator("#submitBtn")).to_have_text("依頼を投入")
        expect(page.locator("#openAppendBtn")).to_have_text("追加指示")
        expect(page.locator("#openAppendBtn")).to_be_enabled()

        page.locator("#openAppendBtn").click()
        expect(page.locator("#appendModal")).to_be_visible()
        page.locator("#appendModalInput").fill("[append_extra] ブラウザから追記")
        page.locator("#appendModalSubmit").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        page.locator("#tasksTab").click()
        expect(page.locator("#tasksList")).to_contain_text("append_followup")
