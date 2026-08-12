"""投入済み資金の状態管理 (state.json)。

システムが自動で「買った」と判断することは絶対にない。
ここで扱う投入額は、すべて人間がCLI経由で明示的に記録した金額のみ。

【安全設計】
  - すべての書き込みはatomic write（一時ファイル→os.replace）で行い、
    書き込み途中のクラッシュでファイルが壊れないようにする。
  - 上書き前に必ず1世代分のバックアップ(state.backup.json)を保存する。
  - 資金関連の不変条件（deployed<=total、remaining>=0、
    ticker累積投入額<=総予算の上限比率 等）を毎回の読み込み・書き込みで検証し、
    違反時は処理を停止して明確なエラーを送出する（黙って補正しない）。
"""
from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

from vix_crash_monitor.config import Config

VALID_STAGES = (1, 2, 3, 4)
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")  # 例: NVDA, BRK.B, 2330.TW


class PortfolioStateError(ValueError):
    """資金の不変条件違反・入力検証エラー・state.json破損など、
    処理を安全に継続できない場合に送出する。"""


def validate_ticker(ticker: str) -> str:
    """ティッカー表記の簡易検証（不正な入力を弾く）。存在確認はしない。"""
    if not isinstance(ticker, str) or not ticker.strip():
        raise PortfolioStateError("ticker が空です")
    normalized = ticker.strip().upper()
    if not TICKER_PATTERN.match(normalized):
        raise PortfolioStateError(
            f"ticker の形式が不正です: {ticker!r}（英大文字・数字・'.'・'-'のみ、1〜10文字）"
        )
    return normalized


def validate_stage(stage: int | None) -> int | None:
    if stage is None:
        return None
    if not isinstance(stage, int) or isinstance(stage, bool):
        raise PortfolioStateError(f"stage は整数で指定してください: {stage!r}")
    if stage not in VALID_STAGES:
        raise PortfolioStateError(f"存在しないStageです: {stage}（有効値: {VALID_STAGES}）")
    return stage


def validate_amount(amount: int) -> int:
    if not isinstance(amount, int) or isinstance(amount, bool):
        raise PortfolioStateError(f"amount は整数（円）で指定してください: {amount!r}")
    if amount <= 0:
        raise PortfolioStateError(f"amount は正の整数である必要があります: {amount}")
    return amount


@dataclass
class Purchase:
    timestamp: str
    amount: int
    stage: int | None = None
    ticker: str | None = None
    note: str = ""


@dataclass
class PortfolioState:
    total_budget: int
    deployed_amount: int = 0
    remaining_amount: int = 0
    completed_stages: list = field(default_factory=list)
    purchases: list = field(default_factory=list)  # list[Purchase-as-dict]

    def __post_init__(self):
        if self.remaining_amount == 0 and self.deployed_amount == 0:
            self.remaining_amount = self.total_budget

    # ---- 集計ヘルパー ----
    def ticker_deployed_amount(self, ticker: str) -> int:
        return sum(
            p["amount"] for p in self.purchases if p.get("ticker") == ticker
        )

    def sector_deployed_amount(self, sector_map: dict) -> dict:
        totals: dict = {}
        for p in self.purchases:
            ticker = p.get("ticker")
            if not ticker:
                continue
            sector = sector_map.get(ticker, "OTHER")
            totals[sector] = totals.get(sector, 0) + p["amount"]
        return totals

    def is_stage_completed(self, stage: int) -> bool:
        return stage in self.completed_stages

    def to_dict(self) -> dict:
        return {
            "total_budget": self.total_budget,
            "deployed_amount": self.deployed_amount,
            "remaining_amount": self.remaining_amount,
            "completed_stages": sorted(self.completed_stages),
            "purchases": self.purchases,
        }


def validate_invariants(state: PortfolioState, config: Config) -> None:
    """資金関連の不変条件を検証する。違反時は PortfolioStateError を送出する。

    検証項目（見直しレビュー項目11）:
      - deployed_amount <= total_budget
      - remaining_amount >= 0
      - deployed_amount + remaining_amount == total_budget
      - 銘柄別累積投入額 <= total_budget * max_single_ticker_ratio
    """
    if state.deployed_amount < 0:
        raise PortfolioStateError(f"不変条件違反: deployed_amount が負です（{state.deployed_amount}）")
    if state.deployed_amount > state.total_budget:
        raise PortfolioStateError(
            f"不変条件違反: deployed_amount({state.deployed_amount:,}円) が"
            f" total_budget({state.total_budget:,}円) を超えています"
        )
    if state.remaining_amount < 0:
        raise PortfolioStateError(f"不変条件違反: remaining_amount が負です（{state.remaining_amount:,}円）")
    if state.deployed_amount + state.remaining_amount != state.total_budget:
        raise PortfolioStateError(
            "不変条件違反: deployed_amount + remaining_amount が total_budget と一致しません "
            f"({state.deployed_amount:,} + {state.remaining_amount:,} != {state.total_budget:,})"
        )

    ticker_cap = round(state.total_budget * config.max_single_ticker_ratio)
    ticker_totals: dict = {}
    for p in state.purchases:
        ticker = p.get("ticker")
        if ticker:
            ticker_totals[ticker] = ticker_totals.get(ticker, 0) + p["amount"]
    for ticker, total in ticker_totals.items():
        if total > ticker_cap:
            raise PortfolioStateError(
                f"不変条件違反: {ticker} の累積投入額({total:,}円) が"
                f" 1銘柄上限({ticker_cap:,}円 = 総予算の{config.max_single_ticker_ratio*100:.0f}%) を超えています"
            )


def _state_from_raw(raw: dict, config: Config) -> PortfolioState:
    state = PortfolioState(
        total_budget=raw.get("total_budget", config.crash_buying_budget),
        deployed_amount=raw.get("deployed_amount", 0),
        remaining_amount=raw.get(
            "remaining_amount",
            raw.get("total_budget", config.crash_buying_budget) - raw.get("deployed_amount", 0),
        ),
        completed_stages=list(raw.get("completed_stages", [])),
        purchases=list(raw.get("purchases", [])),
    )
    # config側で予算が変更されていた場合は追従（すでに投入済みの金額は保持）
    if state.total_budget != config.crash_buying_budget:
        state.total_budget = config.crash_buying_budget
        state.remaining_amount = state.total_budget - state.deployed_amount
    return state


def load_state(config: Config) -> PortfolioState:
    """state.jsonを読み込む。

    - ファイルが無ければ新規（総予算＝config.crash_buying_budget）。
    - JSONが破損している場合はstate.backup.jsonへの復旧を試みる。
      バックアップも読めない場合は明確なエラーで停止する（憶測で補正しない）。
    - 読み込んだ状態は不変条件を検証し、違反時はエラーで停止する。
    """
    path = config.state_file()
    backup_path = path.with_name(path.stem + ".backup" + path.suffix)

    if not path.exists():
        state = PortfolioState(
            total_budget=config.crash_buying_budget,
            deployed_amount=0,
            remaining_amount=config.crash_buying_budget,
            completed_stages=[],
            purchases=[],
        )
        validate_invariants(state, config)
        return state

    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        if backup_path.exists():
            try:
                with open(backup_path, "r", encoding="utf-8") as f:
                    raw = json.load(f)
            except json.JSONDecodeError:
                raise PortfolioStateError(
                    f"{path} が破損しており、バックアップ({backup_path})も読み込めませんでした。"
                    "手動での復旧が必要です。処理を停止します。"
                ) from e
        else:
            raise PortfolioStateError(
                f"{path} が破損しており、バックアップファイルも存在しません。"
                "手動での復旧が必要です。処理を停止します。"
            ) from e

    state = _state_from_raw(raw, config)
    validate_invariants(state, config)
    return state


def save_state(state: PortfolioState, config: Config) -> None:
    """state.jsonをatomicに書き込む。書き込み前に既存ファイルをバックアップする。"""
    validate_invariants(state, config)

    path = config.state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    backup_path = path.with_name(path.stem + ".backup" + path.suffix)

    if path.exists():
        shutil.copy2(path, backup_path)

    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)  # atomic rename


def record_purchase(
    state: PortfolioState,
    config: Config,
    amount: int,
    stage: int | None = None,
    ticker: str | None = None,
    note: str = "",
) -> PortfolioState:
    """人間がCLIで明示的に「実際に買った」と入力した金額のみを記録する。

    このシステム自身が発注・約定を検知することは無い。

    入力検証（見直しレビュー項目12）:
      - amount: 正の整数であること、残資金を超えないこと
      - stage: 1〜4のいずれか、または未指定(None)
      - ticker: 指定する場合は簡易フォーマット検証（英大文字+数字/.-、1〜10文字）
      - 同一Stageのstageレベル一括記録（ticker未指定）の重複記録を拒否する
        （銘柄別記録はticker違いで複数回記録できる＝重複とはみなさない）
    """
    amount = validate_amount(amount)
    stage = validate_stage(stage)
    if ticker is not None:
        ticker = validate_ticker(ticker)

    if amount > state.remaining_amount:
        raise PortfolioStateError(
            f"記録しようとしている金額（{amount:,}円）が残り投資可能額（{state.remaining_amount:,}円）を超えています"
        )

    if stage is not None and ticker is None and stage in state.completed_stages:
        raise PortfolioStateError(
            f"Stage {stage} は既に記録済みです（重複登録防止）。"
            "銘柄別に追加記録する場合は --ticker を指定してください。"
        )

    if ticker is not None:
        ticker_cap = round(state.total_budget * config.max_single_ticker_ratio)
        projected = state.ticker_deployed_amount(ticker) + amount
        if projected > ticker_cap:
            raise PortfolioStateError(
                f"{ticker} の累積投入額が1銘柄上限（{ticker_cap:,}円 = 総予算の"
                f"{config.max_single_ticker_ratio*100:.0f}%）を超えます"
                f"（既存{state.ticker_deployed_amount(ticker):,}円 + 今回{amount:,}円 = {projected:,}円）"
            )

    purchase = Purchase(
        timestamp=datetime.now(timezone.utc).isoformat(),
        amount=amount,
        stage=stage,
        ticker=ticker,
        note=note,
    )
    state.purchases.append(asdict(purchase))
    state.deployed_amount += amount
    state.remaining_amount -= amount

    if stage is not None and stage not in state.completed_stages:
        state.completed_stages.append(stage)

    validate_invariants(state, config)
    return state
