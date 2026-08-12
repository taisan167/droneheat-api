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
（記録した時点でそのStageは完了扱いになり、以後は追加投入候補が0円になります。
VIXが42→28→33のように上下しても、一度完了したStageの投入候補額が
再び提示されることはありません＝資金の二重計上を防止しています）。

Stage単位の一括記録（`record-buy`、ticker未指定）は同一Stageを重複記録できません
（二重登録防止のためエラーになります）。銘柄別に内訳を追加したい場合は
`portfolio.py record --ticker ... --stage ...` を使ってください（同一Stageでも
銘柄が異なれば複数回記録できます）。

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

## Stage0〜4の意味（重要）

Stage番号（0〜4）は連番ですが、**危険度が単調に増していくスケールではありません**。

- **Stage0〜3（`BUY_STAGE`）**: 暴落そのものの深刻さを表す指標。VIX・NASDAQ100等の
  下落率から「今どれくらい危険な調整局面か」を判定します。
- **Stage4（`RECOVERY_SIGNAL`）**: Stage3よりさらに危険という意味ではなく、
  **暴落後に複数の反転シグナル（VIXピーク比-20%・5日線上抜け・前日高値上抜け・
  RSI回復・SOX下げ止まり等）が確認できた**ことを示す、別軸の指標です。

レポート・ダッシュボードでは常に `[BUY_STAGE]` / `[RECOVERY_SIGNAL]` を明示し、
Stage番号の大小だけで危険度を誤解しないようにしています。
なお、この2つを完全に独立した型として分離する設計変更（Stage番号の連番からの脱却）は
影響範囲が大きいため本バージョンでは見送り、Issueとして起票しています。

## データ不足時の挙動（DATA INCOMPLETE）

VIXやNASDAQ100など、Stage判定に必須のデータが取得できなかった場合
（レート制限・タイムアウト・empty dataframe・ティッカー不存在など）、
**憶測でStageを判定することはありません**。レポートは

```
DATA INCOMPLETE
市場判定保留
```

として出力され、投入候補額は常に0円になります（`main.py run` は終了コード1で終了）。
52週高値の計算に必要な252営業日分のデータが無い個別銘柄・指数についても、
下落率を0%などにフォールバックせず `N/A` として扱います（誤った下落率を作らないため）。

一部の監視銘柄だけデータ取得に失敗した場合は、その銘柄のみ
`data_available=False` として扱われ（スコア0・Dランク）、他の銘柄の評価には影響しません。

**本番実行（`python vix_crash_monitor/main.py run`）が実データ取得に失敗した場合、
合成データ（テスト用のダミーデータ）へ自動フォールバックすることは一切ありません。**
合成データはテストコード（`tests/`）の中でのみ使用され、本番のCLIパスからは
到達できない設計になっています。

## タイムゾーン

- 市場データ（VIX/NASDAQ100等の取得・前営業日判定）は `America/New_York` 基準。
  yfinanceの日次データは実際の米国取引所営業日のみを持つため、「前営業日」は
  単純な24時間前ではなく正しい直前の取引日として扱われます。
- レポート等のユーザー向け表示は `Asia/Tokyo`（JST）で表示されます
  （`timeutil.py` で一元管理）。

## 資金管理の安全設計

- **不変条件の検証**: `deployed_amount <= total_budget`、`remaining_amount >= 0`、
  `銘柄別累積投入額 <= total_budget × 25%` などを状態の読み込み・書き込みのたびに検証し、
  違反時は処理を停止して明確なエラーを表示します（黙って補正しません）。
- **atomic write + バックアップ**: `state.json` への書き込みは一時ファイル経由の
  atomic writeで行い、上書き前に必ず `data/state.backup.json` へバックアップします。
  `state.json` が破損している場合はバックアップからの復旧を試み、それも失敗した場合は
  明確なエラーで停止します。
- **入力検証**: `record-buy` / `record` は、負の金額・0円・残資金超過・存在しないStage・
  同一Stageの重複記録（stageレベル一括記録の場合）・不正なticker表記・不正な数値形式を
  すべて拒否します。

## 個別銘柄スコアの安全設計

単純な下落率の大きさだけで高スコア・Sランクにはしません。当日-10%以上の急落・
出来高2倍以上・NASDAQ100比で10pt以上の下落など、個別要因を示す兆候が検出された場合は
`INDIVIDUAL_RISK` / `NEWS_CHECK_REQUIRED` 等のフラグを優先し、ランクを強制的に
C/Dへ引き下げます（`reports/latest.json` の各銘柄に `flags` として機械可読タグを出力）。

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
| `timeutil.py` | タイムゾーン管理（市場データ=America/New_York、表示=Asia/Tokyo） |
| `data_fetch.py` | 市場・個別銘柄データ取得（yfinance、ロジックと分離、52週高値データ不足はN/A扱い） |
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
