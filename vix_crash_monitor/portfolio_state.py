"""投入済み資金の状態管理 (state.json)。

システムが自動で「買った」と判断することは絶対にない。
ここで扱う投入額は、すべて人間がCLI経由で明示的に記録した金額のみ。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone

from vix_crash_monitor.config import Config


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


def load_state(config: Config) -> PortfolioState:
    path = config.state_file()
    if not path.exists():
        state = PortfolioState(
            total_budget=config.crash_buying_budget,
            deployed_amount=0,
            remaining_amount=config.crash_buying_budget,
            completed_stages=[],
            purchases=[],
        )
        return state

    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

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


def save_state(state: PortfolioState, config: Config) -> None:
    path = config.state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state.to_dict(), f, ensure_ascii=False, indent=2)


def record_purchase(
    state: PortfolioState,
    amount: int,
    stage: int | None = None,
    ticker: str | None = None,
    note: str = "",
) -> PortfolioState:
    """人間がCLIで明示的に「実際に買った」と入力した金額のみを記録する。

    このシステム自身が発注・約定を検知することは無い。
    """
    if amount <= 0:
        raise ValueError("amount must be positive")
    if amount > state.remaining_amount:
        raise ValueError(
            f"記録しようとしている金額（{amount}円）が残り投資可能額（{state.remaining_amount}円）を超えています"
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

    return state
