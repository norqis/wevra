# Wevra

Wevra は、構造化された AI 作業をローカルで回す workflow engine です。

ジョブを受け取り、必要な作業に分解し、適切な mode で進め、ユーザー確認が必要なときは止まり、長寿命の AI チャットに依存せずに engine 側でフローを管理します。
1 度ジョブを投入すれば、計画、実装、テスト、最終レビューまで一貫して進められます。

Wevra では、ジョブを次の実行モードで回せます。

| Mode | 説明 |
| --- | --- |
| `auto` | 実行前にジョブ内容を見て最適なモードへ解決します。曖昧な場合は `implementation` を選びます。 |
| `implementation` | 作業を追跡可能なタスクへ分解し、実装、既存テストの実行、最終レビューまで進めます。 |
| `research` | 調査と分析を進め、実装や最終レビューには入らずに markdown の結果として返します。 |
| `review` | レビューに必要な文脈を集めたうえで、最終レビューパスまで進めます。 |
| `planning` | 実装には進まず、計画、設計方針、タスク分解を分けた最終結果として返します。 |

![日本語ダッシュボードのスクリーンショット](docs/images/dashboard-ja.png)

## 初回セットアップ

ローカル checkout 直後の初回セットアップ:

```bash
python3 -m venv .venv
./.venv/bin/pip install -e '.[dev]'
./wevra init
```

`wevra init` を実行すると、`wevra.ini`、`agents.ini`、`.env` などのローカル設定ファイルが作られます。

## 設定を調整する

`./wevra init` のあと、必要に応じて生成されたローカル設定ファイルを編集します。

- `wevra.ini`: dashboard の port、通知、runtime の既定値、CLI 用の `HOME` 上書き
- `agents.ini`: role ごとの実行先と model
- `.env`: `DISCORD_WEBHOOK_URL` のようなローカル secret

## Quick Start

初回セットアップ後は、通常これだけで始められます。

```bash
./wevra start
```

その後、`http://127.0.0.1:43861` を開いて dashboard からジョブを投入します。

## Dashboard

dashboard では次の操作ができます。

- 右上の投入モーダルから新しいジョブを作る
- ジョブごとに実行モード、承認方式、実行先、作業ディレクトリを設定する
- 進行状況をリアルタイムで見る
- 質問が来たときに回答する
- 実行中のジョブを、現在の作業が終わったタイミングで停止し、あとで再開する
- タスク、レビュー、エージェントの実行ログ、最終結果を確認する
- 結果を dashboard 上で開き、開いているタブの内容を `.md` でダウンロードする
- 外部 runtime の実行ごとに許可 / 拒否を出す
- 選択中のジョブや role 単位で承認待ちをまとめて許可する
- 進行中のジョブに追加指示を送る

## CLI Examples

同じ操作は CLI からも実行できるので、スクリプト化や自動化にも向いています。

実装ジョブを流す例:

```bash
./wevra submit --mode implementation --workspace-dir /path/to/worktree "Implement a planner-backed workflow"
./wevra run
```

調査ジョブを流す例:

```bash
./wevra submit --mode research --workspace-dir /path/to/worktree "現在の構成を調べてトレードオフを整理する"
./wevra run
```

質問に回答する例:

```bash
./wevra questions --open-only
./wevra answer <question-id> "Proceed with the existing interface."
./wevra run
```

進行中のジョブに追加指示を入れる例:

```bash
./wevra append <command-id> "Keep the current work, but also add a final follow-up pass."
./wevra run --command-id <command-id>
```

承認待ちのエージェント実行を CLI で確認・操作する例:

```bash
./wevra submit --mode implementation --approval-mode manual --workspace-dir /path/to/worktree "Implement a planner-backed workflow"
./wevra run --command-id <command-id>
./wevra agent-runs --command-id <command-id>
./wevra approve-agent-run <agent-run-id>
./wevra approve-agent-runs <command-id> --role implementer
./wevra deny-agent-run <agent-run-id> "このジョブでは外部実行を許可しない"
```

## 実行フロー

1. CLI か dashboard からジョブを投入します。
2. Wevra が mode に応じて必要な作業へ分解します。
3. 実行できる作業から順に進め、安全なものは並列に進めます。
4. 確認が必要になったら質問して止まります。
5. 外部 runtime の実行に承認が必要な場合は、`エージェント` タブで許可または拒否されるまで停止します。
   ジョブ全体、または `implementer` のような role 単位でまとめて許可することもできます。
6. `implementation` mode では、実装後に既存テストと最終レビューを行います。
7. 最終レビューが通ったときだけ完了します。

CLI から dashboard を操作する例:

```bash
./wevra dashboard start
./wevra dashboard status
./wevra dashboard stop
```

## 設定リファレンス

`wevra init` を実行すると、次のローカル設定ファイルが作られます。

- `wevra.ini`
- `agents.ini`
- `.env`

### `wevra.ini`

runtime、UI、通知まわりの挙動を設定します。

| キー | 既定値 | 内容 |
| --- | --- | --- |
| `runtime.db_path` | `.wevra/wevra.db` | SQLite DB の保存先です。 |
| `runtime.language` | `en` | runtime の既定言語です。 |
| `runtime.agent_timeout_seconds` | `1800` | Codex / Claude の構造化応答を待つ最大秒数です。超えたらその実行を失敗扱いにします。 |
| `runtime.home` | 空 | Codex や Claude など外部 CLI を起動するときに使う `HOME` の上書きです。 |
| `ui.auto_start` | `true` | `wevra start` 実行時に dashboard を自動起動します。 |
| `ui.port` | `43861` | dashboard の port です。 |
| `ui.open_browser` | `true` | dashboard 起動時にブラウザを開きます。 |
| `ui.language` | 空 | dashboard の言語を明示指定できます。 |
| `notification.question_opened` | `false` | 新しい質問が開いたときの通知フックです。 |
| `notification.workflow_completed` | `false` | workflow 完了時の通知フックです。 |
| `discord.enable` | `false` | Discord 通知を有効化します。 |
| `discord.webhook_url` | `DISCORD_WEBHOOK_URL` | `.env` または実行中の環境変数から読むキー名です。 |

### `agents.ini`

role ごとに、どの実行先と model を使うかを設定します。

- `runtime`: その role をどの実行先で動かすか
- `model`: その実行先に渡す model 名
- `count`: その role を同時にいくつ動かすか

| セクション | キー | 内容 |
| --- | --- | --- |
| `coordinator` | `runtime`, `model` | ジョブの受付や進行調整で使う実行先と model です。 |
| `planner` | `runtime`, `model` | ジョブを作業に分けるときに使う実行先と model です。 |
| `investigation` | `runtime`, `model` | 調査タスクで使う実行先と model です。 |
| `analyst` | `runtime`, `model` | 分析や整理で使う実行先と model です。 |
| `tester` | `runtime`, `model` | テスト工程で使う実行先と model です。 |
| `implementer` | `runtime`, `model`, `count` | 実装工程で使う実行先、model、並列数です。 |
| `reviewer` | `runtime`, `model`, `count` | 最終レビューで使う実行先、model、並列数です。 |

`runtime` に指定できる値は `mock`、`codex`、`claude` です。

`mock` は、デモ、ローカル開発、CI、フロー確認のための擬似実行先です。Codex や Claude を使った実際の実装やレビューは行いません。実運用するときは、`planner`、`implementer`、`reviewer` などを `codex` か `claude` に切り替えてから使ってください。

承認方式は `wevra.ini` ではなく、ジョブごとに dashboard または CLI から選びます。`自動` にすると Codex / Claude の実行をそのまま流し、`手動` にすると dashboard の `エージェント` タブで許可 / 拒否を判断するまで停止します。

### `.env`

設定ファイルから参照されるローカル secret や env 値を置きます。

| キー | 参照元 | 内容 |
| --- | --- | --- |
| `DISCORD_WEBHOOK_URL` | `wevra.ini` → `discord.webhook_url` | Discord 通知を有効にしたときに使う実際の webhook URL です。 |

## Development

```bash
./.venv/bin/pytest -q
```

dashboard の UI を変更したときは、PR を出す前に `docs/images/dashboard-en.png` と `docs/images/dashboard-ja.png` も更新してください。
