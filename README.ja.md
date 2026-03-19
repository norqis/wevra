# Wevra

Wevra は、構造化された AI 実行のためのローカル workflow engine です。

ユーザーの指示を明示的な runtime state に変換し、AI backend には構造化された planning / task / review 出力だけを返させ、オーケストレーション責務は長寿命の AI チャットではなく engine 側に持たせます。

## 現在あるもの

- Python 標準の `sqlite3` を使った SQLite ベースの runtime
- `wevra.ini` と `agents.ini` による設定
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

## 実行フロー

典型的な command の流れはこうです。

1. ユーザーが CLI か dashboard から command を投入する
2. engine が command を `planning` に進める
3. planner が command、workspace 状態、過去の質問、過去の review、append された追加指示を見て判断する
4. planner は「質問する」「即完了する」「task DAG を返す」のどれかを返す
5. command に調査が必要なら、implementation の前に investigation / analysis 系の task を切れる
6. engine は `depends_on`、`write_files`、role ごとの並列数を見ながら `running` で task を実行する
7. planner や worker から質問が出たら `waiting_question` に入り、ユーザー回答を待つ
8. ユーザーが追加指示を append したら、いま走っている active batch だけ完走させてから `replanning` に入り、planner が task graph を更新する
9. task が全部終わると `verifying` に進み、最後の全体 review を回す
10. review で差し戻しがあれば `replanning` に戻り、問題なければ `done` で完了する

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
wevra submit "Implement a planner-backed workflow"
wevra run
wevra list
wevra tasks
wevra reviews
wevra events
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
