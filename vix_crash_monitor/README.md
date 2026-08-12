# VIX暴落買い監視システム ／ 分割投資判断支援機能

投資可能資金200万円を前提に、市場急落時の分割投資判断を支援するツールです。

**このシステムは投資判断"支援"専用です。証券会社への注文・自動売買・API発注は一切行いません。
最終的な購入判断・実際の発注は必ず人間が行ってください。**

## セットアップ

```bash
pip install -r vix_crash_monitor/requirements.txt
```

## 使い方

### 1. 監視を実行してレポートを生成

```bash
python vix_crash_monitor/main.py run
```

- VIX / NASDAQ100 / SOX / S&P500 を取得し、市場Stage（0〜4）を判定
- 監視銘柄（NVDA, AMD, ASML, AMAT, LRCX, KLAC, AVGO, TSM, MU, ARM, MSFT, GOOGL, AMZN, META）を
  100点満点でスコアリングし S/A/B/C/D ランクを付与
- 投入候補額を上位銘柄に配分（40/30/20/10%、1銘柄上限あり、セクター集中は警告）
- 以下を出力
  - コンソールへのテキストレポート
  - `reports/latest.json`（ChatGPT等への連携用）
  - `reports/chatgpt_prompt.md`（ChatGPTにそのまま貼り付けられる要約）
  - `data/history.csv`（検証用の履歴）

### 2. 実際に購入したら記録する（人間が明示的に入力）

Stage単位でまとめて記録:

```bash
python vix_crash_monitor/main.py record-buy --stage 1 --amount 200000
```

銘柄別に記録:

```bash
python vix_crash_monitor/portfolio.py record --ticker NVDA --amount 100000 --stage 1
```

システムが自動で「買った」と判断することはありません。記録しない限り、
同じStage条件に再び該当しても投入候補額はそのまま提示され続けます
（記録した時点でそのStageは完了扱いになり、以後は追加投入候補が0円になります）。

### 3. 状態確認

```bash
python vix_crash_monitor/main.py status
python vix_crash_monitor/portfolio.py status
```

### 4. ダッシュボード（Streamlit）

```bash
streamlit run vix_crash_monitor/dashboard.py
```

`main.py run` でレポートを生成した後に起動してください。

### 5. バックテスト（雛形）

```bash
python vix_crash_monitor/backtest.py --start 2020-01-01 --end 2020-12-31
python vix_crash_monitor/backtest.py --start 2022-01-01 --end 2022-12-31
```

判定ロジック（`stage_logic.py`）とデータ取得（`data_fetch.py` / `backtest.py`内の
ヒストリカル取得）を分離しているため、本番ロジックの変更がそのままバックテストにも反映されます。
現状は市場全体のStageタイミング検証のみで、銘柄別スコアリングは対象外です。

### 6. テスト

```bash
pytest vix_crash_monitor/tests
```

## 設定 (`config.yaml`)

`vix_crash_monitor/config.yaml` で以下を変更できます。

- `portfolio.crash_buying_budget`: 投資可能資金（既定200万円）
- `portfolio.max_single_ticker_ratio`: 1銘柄上限比率（既定25%）
- `portfolio.max_sector_ratio`: セクター集中警告しきい値（既定50%）
- `stage_thresholds`: 各StageのVIX/下落率しきい値と配分比率
- `pre_alert`: PRE-ALERTの発動条件
- `watchlist`: 監視銘柄とセクター分類
- `allocation_weights`: 銘柄別配分ウェイト（既定 40/30/20/10%）
- `rank_thresholds`: S/A/B/Cランクのスコアしきい値

## モジュール構成

| ファイル | 役割 |
|---|---|
| `config.py` / `config.yaml` | 設定読み込み |
| `models.py` | 共通データモデル（判断ロジックなし） |
| `data_fetch.py` | 市場・個別銘柄データ取得（yfinance、ロジックと分離） |
| `stage_logic.py` | 市場Stage判定・投入候補額の純粋ロジック（テスト・バックテスト再利用可能） |
| `scoring.py` | 銘柄別100点スコアリング・ランク判定 |
| `news_check.py` | 個別悪材料チェック（機械判定可能な項目＋「ニュース確認必要」表示） |
| `allocation.py` | 銘柄別配分・1銘柄上限・セクター集中警告 |
| `portfolio_state.py` | `data/state.json` の読み書き（人間が記録した購入のみ反映） |
| `report.py` | テキスト/JSON/ChatGPT用Markdownレポート生成 |
| `history.py` | `data/history.csv` への履歴保存 |
| `main.py` | CLI（run / record-buy / status） |
| `portfolio.py` | CLI（銘柄別 record / status） |
| `dashboard.py` | Streamlitダッシュボード（表示専用） |
| `backtest.py` | バックテスト雛形 |
| `tests/` | pytestテスト（ネットワーク不要、ロジックのみ検証） |

## 禁止事項（実装していないこと）

以下は本システムには一切実装されていません。

- 証券会社APIへの発注（Interactive Brokers等含む）
- 成行・指値注文の自動発注
- 自動売買・信用取引の自動化
- レバレッジ取引
- 「必ず儲かる」等の断定的表現

`record-buy` / `record` コマンドは、人間が実際に証券会社で購入した"後"に
金額を手入力するための記録用コマンドであり、発注機能ではありません。
