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
from wevra.db import connect
from wevra.models import CommandStage
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


def submit_job(tmp_path, *args):
    return read_json(runner.invoke(app, ["submit", "--workspace-dir", str(tmp_path), *args]))


def wait_for_refresh_condition(page, predicate, refresh_selector="#refreshBtn", attempts=12):
    last_error = None
    for _ in range(attempts):
        try:
            result = predicate()
            if result is None or result:
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
        context = browser.new_context(
            viewport=viewport or {"width": 1600, "height": 1080},
            accept_downloads=True,
        )
        page = context.new_page()
        page.set_default_timeout(15000)
        try:
            yield page
        finally:
            context.close()
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

        expect(page.locator("#openAppendBtn")).to_have_text("Append Instruction")
        expect(page.locator("#openAppendBtn")).to_be_disabled()
        assert page.locator("#openAppendBtn").get_attribute("title") is None

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("implement happy path")
        page.locator("#submitBtn").click()

        expect(page.locator("#commandsList .command-item")).to_have_count(1)
        expect(page.locator("#liveIndicator")).to_contain_text("LIVE")
        expect(page.locator("#viewResultBtn")).to_be_enabled()
        expect(page.locator("#openAppendBtn")).to_be_disabled()

        page.locator("#viewResultBtn").click()
        expect(page.locator("#resultModal")).to_be_visible()
        expect(page.locator("#resultModalTabs [data-result-tab]")).to_have_count(1)
        expect(page.locator("#resultModalContent")).to_contain_text("Completed tasks:")


def test_dashboard_browser_planning_result_tabs_and_download(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        page.locator("#workflowMode").select_option("planning")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("planning browser result")
        page.locator("#submitBtn").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        page.locator("#viewResultBtn").click()
        expect(page.locator("#resultModal")).to_be_visible()
        expect(page.locator("#resultModalTabs [data-result-tab]")).to_have_count(3)
        expect(page.locator("#resultModalTabs [data-result-tab]").nth(0)).to_have_text("Plan")
        expect(page.locator("#resultModalTabs [data-result-tab]").nth(1)).to_have_text(
            "Design Direction"
        )
        expect(page.locator("#resultModalTabs [data-result-tab]").nth(2)).to_have_text(
            "Task Breakdown"
        )
        expect(page.locator("#resultModalContent")).to_contain_text("Goal: planning browser result")
        expect(page.locator("#resultModalContent")).to_contain_text(
            "Primary planning task: Produce a structured execution plan"
        )
        expect(page.locator("#resultModalContent pre")).to_have_count(0)
        expect(page.locator("#resultModalContent ul li").first).to_contain_text(
            "Goal: planning browser result"
        )

        page.locator('#resultModalTabs [data-result-tab="task_breakdown"]').click()
        expect(page.locator("#resultModalContent")).to_contain_text(
            "Produce a structured execution plan"
        )

        with page.expect_download() as download_info:
            page.locator("#downloadResultBtn").click()
        download = download_info.value
        assert download.suggested_filename == "planning_browser_result_Task_Breakdown.md"
        target = tmp_path / "task-breakdown.md"
        download.save_as(target)
        assert "Produce a structured execution plan" in target.read_text(encoding="utf-8")


def test_dashboard_browser_planning_result_download_uses_japanese_section_name(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        page.locator("#workflowMode").select_option("planning")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("planning browser result")
        page.locator("#submitBtn").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        page.locator("#viewResultBtn").click()
        expect(page.locator('#resultModalTabs [data-result-tab="task_breakdown"]')).to_have_text(
            "タスク分解"
        )
        page.locator('#resultModalTabs [data-result-tab="task_breakdown"]').click()

        with page.expect_download() as download_info:
            page.locator("#downloadResultBtn").click()
        download = download_info.value
        assert download.suggested_filename == "planning_browser_result_タスク分解.md"


def test_dashboard_browser_operator_issue_can_retry_with_another_ai(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    class SwitchablePlanner:
        def plan(self, command, *args, **kwargs):
            if command.backend == service_module.RuntimeBackend.CODEX:
                raise service_module.AgentExecutionError(
                    service_module.OperatorIssueKind.PROVIDER_LIMIT,
                    "provider limit",
                    detail="429 rate limit exceeded",
                )
            return service_module.PlannerOutput(
                decision=service_module.PlannerDecision.COMPLETE,
                workflow_mode=service_module.WorkflowMode.PLANNING,
                final_response=(
                    "## Plan\nContinue from the interrupted state.\n\n"
                    "## Design Direction\nKeep the current files.\n\n"
                    "## Task Breakdown\n1. Resume with the alternate AI."
                ),
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: SwitchablePlanner())

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("planning")
        page.locator("#backend").select_option("codex")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("operator issue retry")
        page.locator("#submitBtn").click()

        wait_for_refresh_condition(
            page,
            lambda: expect(page.locator("#commandDetail")).to_contain_text(
                "The selected AI hit a usage limit"
            ),
        )
        page.locator("#agentsTab").click()
        expect(page.locator("#agentRunsList")).to_contain_text("429 rate limit exceeded")
        page.locator("#overviewTab").click()
        page.locator("#commandDetail").locator("[data-open-operator-resume]").click()
        expect(page.locator("#operatorResumeModal")).to_be_visible()
        expect(page.locator("#operatorResumeBackend")).to_have_value("codex")
        page.locator("#operatorResumeBackend").select_option("claude")
        page.locator("#operatorResumeBtn").click()

        wait_for_refresh_condition(
            page,
            lambda: expect(page.locator("#viewResultBtn")).to_be_enabled(),
        )
        page.locator("#viewResultBtn").click()
        expect(page.locator("#resultModalContent")).to_contain_text(
            "Continue from the interrupted state."
        )


def test_dashboard_browser_operator_issue_cancel_with_repair_creates_new_job(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    class InterruptedPlanner:
        def plan(self, *args, **kwargs):
            raise service_module.AgentExecutionError(
                service_module.OperatorIssueKind.PROVIDER_LIMIT,
                "provider limit",
                detail="usage limit exceeded",
            )

    monkeypatch.setattr(service_module, "backend_for", lambda *args, **kwargs: InterruptedPlanner())

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("planning")
        page.locator("#backend").select_option("codex")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("修復対象ジョブ")
        page.locator("#submitBtn").click()

        wait_for_refresh_condition(
            page,
            lambda: expect(page.locator("#commandDetail")).to_contain_text(
                "AI の利用上限に達しました"
            ),
        )
        page.locator("#commandDetail").locator("[data-open-operator-cancel]").click()
        expect(page.locator("#operatorIssueModal")).to_be_visible()
        expect(page.locator("#operatorRepairChoiceTitle")).to_contain_text(
            "中断したジョブの変更を元に戻す"
        )
        page.locator("#operatorRepairBtn").click()

        wait_for_refresh_condition(
            page,
            lambda: expect(page.locator("#commandsList .command-item")).to_have_count(2),
        )
        expect(page.locator("#commandDetail")).to_contain_text("中断したジョブの変更を元に戻す")


def test_dashboard_browser_submit_modal_manual_notice_and_workspace_root(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    workspace_root = (tmp_path / "browser-workspace").resolve()

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        expect(page.locator("#approvalModeNotice")).not_to_be_visible()

        page.locator("#approvalMode").select_option("manual")
        expect(page.locator("#approvalModeNotice")).to_be_visible()
        expect(page.locator("#approvalModeNoticeBody")).to_contain_text("AI 実行タブ")

        page.locator("#workflowMode").select_option("research")
        page.locator("#workspaceRoot").fill(str(workspace_root))
        page.locator("#goal").fill("workspace root from modal")
        page.locator("#submitBtn").click()

        expect(page.locator("#submitModal")).not_to_be_visible()
        expect(page.locator("#commandDetail")).to_contain_text(str(workspace_root))


def test_dashboard_browser_submit_modal_dogfooding_mode_shows_description_notice_and_runbook(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    runbook = tmp_path / "docs" / "RUNBOOK.md"
    runbook.parent.mkdir(parents=True, exist_ok=True)
    runbook.write_text("# Runbook\n\n1. Start the app.\n", encoding="utf-8")

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        expect(page.locator("#modeIntroBody")).to_contain_text("最適な実行モードを自動で選びます")
        expect(page.locator("#dogfoodingNotice")).not_to_be_visible()
        expect(page.locator("#runbookPathField")).not_to_be_visible()

        page.locator("#workflowMode").select_option("dogfooding")
        expect(page.locator("#modeIntroBody")).to_contain_text("運用手順に沿って実際のフローを試し")
        expect(page.locator("#dogfoodingNotice")).to_be_visible()
        expect(page.locator("#dogfoodingNoticeBody")).to_contain_text("トークン消費量")
        expect(page.locator("#runbookPathField")).to_be_visible()

        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#runbookPath").fill(str(runbook))
        page.locator("#goal").fill("release candidate を dogfooding する")
        page.locator("#submitBtn").click()

        expect(page.locator("#submitModal")).not_to_be_visible()
        expect(page.locator("#commandDetail")).to_contain_text(str(runbook.resolve()))


def test_dashboard_browser_job_split_preview_and_apply(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        page.locator("#submitModeSplitTab").click()

        expect(page.locator("#splitModeHelper")).to_contain_text(
            "AI が依存関係つきの複数ジョブ案を作成します。投入前に確認ができます。"
        )
        expect(page.locator("#singleWorkflowField")).to_be_hidden()

        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#backend").select_option("mock")
        page.locator("#goal").fill("Update docs and implementation for the same change")
        page.locator("#generateSplitPreviewBtn").click()

        expect(page.locator("#splitPreviewSection")).to_be_visible()
        expect(page.locator("#splitPreviewTabs [data-split-preview-tab]")).to_have_count(3)
        expect(page.locator("#splitPreviewDetail")).to_contain_text("実装を進める")
        page.locator('#splitPreviewTabs [data-split-preview-tab="1"]').click()
        expect(page.locator("#splitPreviewDetail")).to_contain_text("仕様書と案内を更新する")
        page.locator('#splitPreviewTabs [data-split-preview-tab="2"]').click()
        expect(page.locator("#splitPreviewDetail")).to_contain_text("仕上がりを確認する")

        page.locator("#applySplitPreviewBtn").click()

        wait_for_python_condition(lambda: len(list_commands(settings.db_path)) == 3)
        expect(page.locator("#commandsList .command-item")).to_have_count(3)
        expect(page.locator("#submitModal")).not_to_be_visible()


def test_dashboard_browser_dependency_picker_and_overlap_warning(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    first_workspace = (tmp_path / "project").resolve()
    nested_workspace = first_workspace / "public"

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workspaceRoot").fill(str(first_workspace))
        page.locator("#goal").fill("先行ジョブ")
        page.locator("#submitBtn").click()

        page.locator("#openSubmitBtn").click()
        expect(page.locator("#dependencyOptions")).to_contain_text("先行ジョブ")
        page.locator("#workspaceRoot").fill(str(nested_workspace))
        expect(page.locator("#workspaceOverlapNotice")).to_be_visible()
        expect(page.locator("#workspaceOverlapNoticeBody")).to_contain_text("先行ジョブ")
        expect(page.locator("#allowParallel")).to_be_disabled()

        first_command = service_module.list_commands(load_config(tmp_path).db_path)[0]
        with connect(load_config(tmp_path).db_path) as conn:
            conn.execute(
                "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
                (CommandStage.DONE.value, service_module.utc_now(), first_command.id),
            )
            conn.commit()

        page.wait_for_timeout(3200)
        expect(page.locator("#workspaceOverlapNotice")).not_to_be_visible()
        expect(page.locator("#allowParallel")).to_be_enabled()
        expect(page.locator("#dependencyOptions")).not_to_contain_text("先行ジョブ")

        page.locator("#goal").fill("後続ジョブ")
        page.locator("#allowParallel").check()
        page.locator("#submitBtn").click()

        expect(page.locator("#commandDetail")).to_contain_text("並列")


def test_dashboard_browser_dependency_canceled_state_disables_workspace_actions(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)

    dependency = service_module.submit_command(
        settings.db_path,
        goal="先行ジョブ",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-a",
        settings=settings,
        repo_root=tmp_path,
    )
    blocked = service_module.submit_command(
        settings.db_path,
        goal="後続ジョブ",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-b",
        depends_on_command_ids=[dependency.id],
        settings=settings,
        repo_root=tmp_path,
    )
    service_module.cancel_command(
        settings.db_path,
        dependency.id,
        reason="先行ジョブを取り下げました。",
    )

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator(f'#commandsList [data-command-id="{blocked.id}"]').click()
        expect(page.locator("#commandDetail")).to_contain_text("依存ジョブが取り下げられました")
        expect(page.locator("#commandDetail")).to_contain_text(
            "このジョブをどうするか選んでください"
        )
        expect(page.locator("#runControlBtn")).to_be_disabled()
        expect(page.locator("#cancelJobBtn")).to_be_disabled()
        expect(page.locator("#commandsList")).to_contain_text("取り下げ済み")

        page.locator("[data-cancel-command]").click()
        wait_for_refresh_condition(
            page,
            lambda: expect(page.locator("#commandDetail")).not_to_contain_text(
                "依存ジョブが取り下げられました"
            ),
        )
        expect(page.locator("#commandDetail")).not_to_contain_text("Canceled by operator.")
        expect(page.locator("#commandDetail")).not_to_contain_text(
            "このジョブをどうするか選んでください"
        )
        expect(page.locator("[data-ignore-dependencies]")).to_have_count(0)
        expect(page.locator("[data-cancel-command]")).to_have_count(0)


def test_dashboard_browser_command_list_orders_active_waiting_then_terminal_and_fills_height(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)
    monkeypatch.setattr(
        "wevra.dashboard.tick_once",
        lambda *args, **kwargs: service_module.TickOutcome(action="idle"),
    )

    running = service_module.submit_command(
        settings.db_path,
        goal="実行中ジョブ",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-running",
        settings=settings,
        repo_root=tmp_path,
    )
    waiting_one = service_module.submit_command(
        settings.db_path,
        goal="待機ジョブ1",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-waiting-1",
        depends_on_command_ids=[running.id],
        settings=settings,
        repo_root=tmp_path,
    )
    service_module.submit_command(
        settings.db_path,
        goal="待機ジョブ2",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-waiting-2",
        depends_on_command_ids=[running.id, waiting_one.id],
        settings=settings,
        repo_root=tmp_path,
    )
    done = service_module.submit_command(
        settings.db_path,
        goal="完了ジョブ",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-done",
        settings=settings,
        repo_root=tmp_path,
    )
    canceled = service_module.submit_command(
        settings.db_path,
        goal="取り下げジョブ",
        workflow_mode=service_module.WorkflowMode.RESEARCH,
        priority=service_module.Priority.HIGH,
        workspace_root=tmp_path / "workspace-canceled",
        settings=settings,
        repo_root=tmp_path,
    )

    with connect(settings.db_path) as conn:
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.RUNNING.value, "2026-03-23T10:00:00+00:00", running.id),
        )
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ? WHERE id = ?",
            (CommandStage.DONE.value, "2026-03-23T10:05:00+00:00", done.id),
        )
        conn.execute(
            "UPDATE commands SET stage = ?, updated_at = ?, failure_reason = ? WHERE id = ?",
            (
                CommandStage.CANCELED.value,
                "2026-03-23T10:10:00+00:00",
                "取り下げました。",
                canceled.id,
            ),
        )
        conn.commit()

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        titles = page.locator("#commandsList .command-item .title").all_inner_texts()
        assert titles[:5] == [
            "実行中ジョブ",
            "待機ジョブ1",
            "待機ジョブ2",
            "取り下げジョブ",
            "完了ジョブ",
        ]

        metrics = page.evaluate(
            """
            () => {
              const list = document.querySelector('#commandsList');
              const rect = list.getBoundingClientRect();
              return {
                bottomGap: window.innerHeight - rect.bottom,
                height: rect.height,
              };
            }
            """
        )
        assert metrics["bottomGap"] < 40
        assert metrics["height"] > 700


def test_dashboard_browser_question_answer_survives_refresh_and_resumes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#openSubmitBtn").click()
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("[worker_question] answer from browser dashboard")
        page.locator("#submitBtn").click()

        expect(page.locator("#questionAlert")).to_be_visible()
        answer_input = page.locator("#questionAlertAnswer")
        answer_text = "Proceed with the current approach."
        answer_input.fill(answer_text)

        post_json(
            f"{base_url}/api/commands",
            {
                "goal": "background request to force a snapshot refresh",
                "workspace_root": str(tmp_path),
            },
        )
        page.locator("#refreshBtn").click()

        expect(page.locator("#questionAlertAnswer")).to_have_value(answer_text)
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#questionAlert")).not_to_be_visible()
        expect(page.locator("#viewResultBtn")).to_be_enabled()


def test_dashboard_browser_stop_and_resume_from_workspace_actions(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("[worker_question] stop and resume from dashboard")
        page.locator("#submitBtn").click()

        expect(page.locator("#questionAlert")).to_be_visible()
        expect(page.locator("#runControlBtn")).to_have_text("一時停止")
        expect(page.locator("#cancelJobBtn")).to_have_text("キャンセル")
        assert page.locator("#runControlBtn").get_attribute("title") is None
        page.locator("#cancelJobBtn").click()
        expect(page.locator("#operatorIssueModal")).to_be_visible()
        expect(page.locator("#operatorIssueModalTitle")).to_contain_text(
            "このジョブを取り下げますか"
        )
        expect(page.locator("#operatorIssueModalContext")).to_contain_text(
            "途中までの変更が残ることがあります"
        )
        page.locator("#closeOperatorIssueBtn").click()
        expect(page.locator("#operatorIssueModal")).not_to_be_visible()
        page.locator("#runControlBtn").click()

        wait_for_refresh_condition(
            page,
            lambda: page.locator("#runControlBtn").inner_text() == "再開",
        )
        expect(page.locator("#runControlBtn")).to_have_attribute(
            "title", "この一時停止中のジョブを現在の段階から再開します。"
        )
        page.locator("#runControlBtn").click()
        wait_for_refresh_condition(
            page,
            lambda: page.locator("#runControlBtn").inner_text() == "一時停止",
        )
        assert page.locator("#runControlBtn").get_attribute("title") is None

        page.locator("#questionAlertAnswer").fill("再開して進めてください。")
        expect(page.locator("#questionAlertSubmit")).to_be_enabled()
        page.locator("#questionAlertSubmit").click()
        expect(page.locator("#viewResultBtn")).to_be_enabled()


def test_dashboard_browser_append_instruction_in_japanese_locale(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)
    submitted = submit_job(tmp_path, "[worker_question] dashboard append path")
    command_id = submitted["id"]
    read_json(runner.invoke(app, ["run", "--command-id", command_id]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        expect(page.locator("#openSubmitBtn")).to_have_text("ジョブを投入")
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
        page.locator("#refreshBtn").click()
        expect(page.locator("#questionAlert")).to_be_visible()
        page.locator("#questionAlertAnswer").fill("このまま進めてください。")
        expect(page.locator("#questionAlertSubmit")).to_be_enabled()
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        expect(page.locator("#openAppendBtn")).to_have_text("追加指示")
        expect(page.locator("#openAppendBtn")).to_be_disabled()
        expect(page.locator("#openAppendBtn")).to_have_attribute(
            "title", "完了または取り下げ済みのジョブには、追加指示を送れません。"
        )


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

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option(mode)
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill(goal)
        page.locator("#submitBtn").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        expect(page.locator("#openAppendBtn")).to_have_text("Append Instruction")
        expect(page.locator("#openAppendBtn")).to_be_disabled()
        expect(page.locator("#openAppendBtn")).to_have_attribute(
            "title",
            "Follow-up instructions are unavailable after a job is completed or canceled.",
        )
        if expect_reviews:
            wait_for_python_condition(lambda: len(list_reviews(settings.db_path)) == 2)
        page.locator("#refreshBtn").click()
        page.locator("#reviewsTab").click()
        if expect_reviews:
            expect(page.locator("#reviewsList .card")).to_have_count(2)
        else:
            expect(page.locator("#reviewsList")).to_contain_text("No reviews yet.")


def test_dashboard_browser_review_card_opens_detail_modal(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))
    settings = load_config(tmp_path)

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(base_url)

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("review")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("[review_changes] review the existing changes")
        page.locator("#submitBtn").click()

        wait_for_python_condition(
            lambda: any(review.findings for review in list_reviews(settings.db_path))
        )
        page.locator("#refreshBtn").click()
        page.locator("#reviewsTab").click()
        review_card = page.locator("#reviewsList .review-card").first
        expect(review_card).to_be_visible()
        expect(review_card).to_contain_text("View details")

        review_card.click()
        expect(page.locator("#reviewModal")).to_be_visible()
        expect(page.locator("#reviewModalSummary")).to_contain_text("requested a follow-up pass")
        expect(page.locator("#reviewModalFindings")).to_contain_text(
            "Add a second implementation pass before completion."
        )
        page.locator("#closeReviewBtn").click()
        expect(page.locator("#reviewModal")).not_to_be_visible()


def test_dashboard_browser_mode_badge_uses_neutral_tone_in_command_rail(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("planning")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("planning badge color separation")
        page.locator("#submitBtn").click()

        rail_item = page.locator("#commandsList .command-item").first
        expect(rail_item).to_be_visible()
        stage_pill = rail_item.locator(".stage-pill").first
        mode_pill = rail_item.locator(".mode-pill").first
        expect(stage_pill).to_have_text("完了")
        expect(mode_pill).to_contain_text("計画")

        stage_color = stage_pill.evaluate("el => window.getComputedStyle(el).color")
        mode_color = mode_pill.evaluate("el => window.getComputedStyle(el).color")
        assert stage_color != mode_color


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

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("implementation")
        page.locator("#approvalMode").select_option("manual")
        page.locator("#workspaceRoot").fill(str(tmp_path))
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
        expect(page.locator('[data-agent-role-tab="implementer"]')).to_be_visible()
        expect(page.locator("[data-agent-allow]")).to_be_visible()
        expect(
            page.locator('[data-agent-role-panel="implementer"].active [data-agent-log-pane]')
        ).to_contain_text("waiting for operator approval")
        page.locator("[data-agent-allow]").click()

        wait_for_python_condition(
            lambda: any(command.final_response for command in list_commands(settings.db_path)),
            attempts=40,
            delay_seconds=0.25,
        )
        page.locator("#refreshBtn").click()
        page.locator("#overviewTab").click()
        expect(page.locator("#viewResultBtn")).to_be_enabled()


def test_dashboard_browser_agents_tab_denies_with_operator_note(tmp_path, monkeypatch):
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

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("implementation")
        page.locator("#approvalMode").select_option("manual")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("agent deny note coverage")
        page.locator("#submitBtn").click()

        pending_run = wait_for_python_condition(
            lambda: next(
                (
                    agent_run
                    for agent_run in list_agent_runs(settings.db_path)
                    if agent_run.state.value == "pending_approval"
                ),
                None,
            )
        )
        deny_reason = "Need a human to inspect the current workspace first."

        page.locator("#refreshBtn").click()
        page.locator("#agentsTab").click()
        page.locator(f'[data-agent-deny="{pending_run.id}"]').click()

        expect(page.locator("#agentDenyModal")).to_be_visible()
        expect(page.locator("#agentDenyModalContext")).to_contain_text("agent deny note coverage")
        assert (
            next(
                run for run in list_agent_runs(settings.db_path) if run.id == pending_run.id
            ).state.value
            == "pending_approval"
        )

        page.locator("#agentDenyReason").fill(deny_reason)
        page.locator("#agentDenySubmitBtn").click()

        denied_run = wait_for_python_condition(
            lambda: next(
                (
                    run
                    for run in list_agent_runs(settings.db_path)
                    if run.id == pending_run.id
                    and run.state.value == "denied"
                    and run.error == deny_reason
                ),
                None,
            )
        )
        failed_command = wait_for_python_condition(
            lambda: next(
                (
                    command
                    for command in list_commands(settings.db_path)
                    if command.id == pending_run.command_id
                    and command.stage.value == "failed"
                    and command.failure_reason == deny_reason
                ),
                None,
            )
        )

        assert denied_run.error == deny_reason
        assert failed_command.failure_reason == deny_reason

        page.locator("#refreshBtn").click()
        expect(page.locator("#agentDenyModal")).not_to_be_visible()
        page.locator("#overviewTab").click()
        expect(page.locator("#commandDetail")).to_contain_text(deny_reason)
        expect(page.locator("#openAppendBtn")).to_have_text("Append Instruction")
        expect(page.locator("#openAppendBtn")).to_be_disabled()
        assert page.locator("#openAppendBtn").get_attribute("title") is None


def test_dashboard_browser_agents_tab_groups_runs_by_role_for_completed_implementation(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    read_json(runner.invoke(app, ["init"]))

    with dashboard_server(tmp_path) as base_url, browser_page() as page:
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("implementation")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("role grouped logs")
        page.locator("#submitBtn").click()

        expect(page.locator("#viewResultBtn")).to_be_enabled()
        page.locator("#agentsTab").click()

        expect(page.locator('[data-agent-role-tab="planner"]')).to_be_visible()
        expect(page.locator('[data-agent-role-tab="implementer"]')).to_be_visible()
        expect(page.locator('[data-agent-role-tab="tester"]')).to_be_visible()
        expect(page.locator('[data-agent-role-tab="reviewer"]')).to_be_visible()

        page.locator('[data-agent-role-tab="reviewer"]').click()
        expect(
            page.locator('[data-agent-role-panel="reviewer"].active .agent-run-card')
        ).to_have_count(2)
        reviewer_log = page.locator(
            '[data-agent-role-panel="reviewer"].active [data-agent-log-pane]'
        ).first
        expect(reviewer_log).to_contain_text("mock reviewer")


def test_dashboard_browser_agents_log_auto_follow_resets_on_tab_switch(tmp_path, monkeypatch):
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
        page.goto(f"{base_url}/?lang=ja")

        page.locator("#openSubmitBtn").click()
        page.locator("#workflowMode").select_option("implementation")
        page.locator("#approvalMode").select_option("manual")
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("agent log follow coverage")
        page.locator("#submitBtn").click()

        pending_run = wait_for_python_condition(
            lambda: next(
                (
                    run
                    for run in list_agent_runs(settings.db_path)
                    if run.state.value == "pending_approval"
                ),
                None,
            )
        )

        page.locator("#refreshBtn").click()
        page.locator("#agentsTab").click()
        expect(page.locator('[data-agent-role-tab="implementer"]')).to_be_visible()

        log_selector = (
            f'[data-agent-role-panel="implementer"].active [data-agent-log-pane="{pending_run.id}"]'
        )
        badge_selector = f'[data-agent-log-follow="{pending_run.id}"]'
        expect(page.locator(log_selector)).to_contain_text("waiting for operator approval")
        expect(page.locator(badge_selector)).to_have_text("自動追尾")

        for index in range(80):
            service_module.append_agent_run_log(
                settings.db_path,
                pending_run.id,
                service_module.format_agent_log_line(f"overflow line {index} {'x' * 48}"),
            )
        service_module.append_agent_run_log(
            settings.db_path,
            pending_run.id,
            service_module.format_agent_log_line("manual test line 1"),
        )
        page.locator("#refreshBtn").click()
        wait_for_python_condition(
            lambda: page.locator(log_selector).evaluate("(el) => el.scrollHeight > el.clientHeight")
        )
        assert page.locator(log_selector).evaluate(
            "(el) => Math.abs(el.scrollHeight - el.scrollTop - el.clientHeight) <= 16"
        )

        page.locator(log_selector).evaluate(
            "(el) => { el.scrollTop = 0; el.dispatchEvent(new Event('scroll')); }"
        )
        page.wait_for_timeout(150)
        expect(page.locator(badge_selector)).to_have_text("追尾停止")

        service_module.append_agent_run_log(
            settings.db_path,
            pending_run.id,
            service_module.format_agent_log_line("manual test line 2"),
        )
        page.locator("#refreshBtn").click()
        expect(page.locator(log_selector)).to_contain_text("manual test line 2")
        assert page.locator(log_selector).evaluate("(el) => el.scrollTop <= 4")

        page.locator("#overviewTab").click()
        page.locator("#agentsTab").click()
        expect(page.locator(badge_selector)).to_have_text("自動追尾")
        assert page.locator(log_selector).evaluate(
            "(el) => Math.abs(el.scrollHeight - el.scrollTop - el.clientHeight) <= 16"
        )


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
                "--workspace-dir",
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
                "--workspace-dir",
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
        expect(page.locator("#openSubmitBtn")).to_have_text("ジョブを投入")

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

    completed = submit_job(tmp_path, "completed request")
    waiting = submit_job(tmp_path, "[worker_question] waiting request")
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

        expect(page.locator("#openSubmitBtn")).to_have_text("ジョブを投入")
        page.locator("#openSubmitBtn").click()
        expect(page.locator("#submitModal")).to_be_visible()
        page.locator("#workspaceRoot").fill(str(tmp_path))
        page.locator("#goal").fill("[worker_question] mobile browser flow")
        page.locator("#submitBtn").click()

        expect(page.locator("#questionAlert")).to_be_visible()
        page.locator("#questionAlertAnswer").fill("モバイルから継続してください。")
        page.locator("#questionAlertSubmit").click()

        expect(page.locator("#questionAlert")).not_to_be_visible()
        expect(page.locator("#viewResultBtn")).to_be_enabled()
