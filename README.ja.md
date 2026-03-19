# Wevra

Wevra は、構造化された AI 実行のためのローカル workflow engine です。

ユーザーの指示を明示的な runtime state に変換し、AI backend には構造化された planning / task / review 出力だけを返させ、オーケストレーション責務は長寿命の AI チャットではなく engine 側に持たせます。

## 現在あるもの

- Python 標準の `sqlite3` を使った SQLite ベースの runtime
- `wevra.ini` と `agents.ini` による設定
- `auto` / `implementation` / `research` / `review` / `planning` の workflow mode
- role ごとの並列数を考慮した dependency-aware task scheduler
- テストとローカル検証用の `mock` backend
- planner / implementer / reviewer 向けの `codex` / `claude` backend
- snapshot API、command submit、質問回答、append-driven replanning を備えた browser dashboard
- `command`、`task`、`question`、`review`、`instruction`、`event` の永続レコード

## Runtime Model

各 command は明示的な stage を通ります。

- `queued`
- `planning`
- `running`
- `waiting_question`
- `verifying`
- `replanning`
- `done`
- `failed`

planner は `key`、`depends_on`、`write_files` を持つ task spec を返します。  
engine はその DAG と `agents.ini` の role 設定を使って、どの task が ready か、どの task を安全に並列実行できるかを決めます。

## Workflow Modes

- `auto`
  依頼内容を見て、もっとも合う mode を Wevra が選びます。
- `implementation`
  実装や変更を進める mode です。必要なら先に調査しますが、既存テストが通り、レビュアー全員が承認するまで完了しません。
- `research`
  調査して報告や結論を返す mode です。最終テスト gate は不要です。
- `review`
  いまの workspace をレビューして、所見を返す mode です。実装フェーズは必須ではありません。
- `planning`
  設計、方針決め、タスク分解だけを行う mode です。実装完了までは持っていきません。

## 実行フロー

典型的な流れはこうです。

1. CLI か dashboard から依頼を投入し、mode を選ぶ
2. Wevra がその mode に必要な作業へ分解する
3. 調査が必要なら、次の作業に進む前に先に調べる
4. 実行できる作業から順に進め、安全なものは並列に進める
5. 途中で確認が必要になったら、ユーザーに質問して止まる
6. ユーザーが追加指示を入れたら、いま動いている作業だけ完了させてから計画を更新する
7. `implementation` mode では、実装作業が終わったあとに既存の Feature / Unit テストを実行する
8. テスト完了後に全体レビューを行う
9. レビュアー全員が承認したときだけ完了し、1人でも修正を求めたら、修正してから全体レビューをやり直す

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

SQLite を別途インストールする必要はありません。

## Quick Start

repo ローカルの設定と DB を初期化します。

```bash
wevra init
```

dashboard を含めて一括で起動するならこちらです。

```bash
wevra start
wevra status
```

CLI から command を投入して実行する基本フローです。

```bash
wevra submit --mode implementation "Implement a planner-backed workflow"
wevra run
wevra list
wevra tasks
wevra reviews
wevra events
```

調査だけ回したい場合:

```bash
wevra submit --mode research "現在の構成を調べてトレードオフを整理する"
wevra run
```

質問が出たときのフローです。

```bash
wevra submit "[worker_question] clarify implementation details"
wevra run
wevra questions --open-only
wevra answer <question-id> "Proceed with the existing interface."
wevra run
```

既存 command に追加指示を入れて再計画させるフローです。

```bash
wevra append <command-id> "Keep the current work, but also add a final follow-up pass."
wevra run --command-id <command-id>
```

dashboard の起動と停止です。

```bash
wevra dashboard start
wevra dashboard status
wevra dashboard stop
```

既定の dashboard URL:

```text
http://127.0.0.1:43861
```

## Config

`wevra init` は次のファイルを作ります。

- `wevra.ini`
- `agents.ini`
- `.env`

`wevra.ini` には runtime 全体の設定を持たせます。たとえば:

- `runtime.working_dir`
- `runtime.db_path`
- `runtime.language`
- `runtime.dangerously_bypass_approvals_and_sandbox`
- `ui.host`
- `ui.port`
- `ui.auto_start`
- `ui.open_browser`

`agents.ini` には role ごとの backend 設定や並列数を持たせます。たとえば:

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
