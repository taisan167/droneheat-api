"""判定結果の履歴保存 (data/history.csv)。

後から「あのときの判定は妥当だったか」を検証できるようにするための
単純な追記ログ。バックテストの実績検証にも利用できる。
"""
from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import MarketSnapshot, StageResult
from vix_crash_monitor.portfolio_state import PortfolioState

HEADER = [
    "timestamp",
    "vix",
    "vix_change_pct",
    "nasdaq_drawdown_pct",
    "sox_drawdown_pct",
    "market_stage",
    "suggested_allocation",
    "deployed_amount",
    "remaining_amount",
]


def append_history(
    config: Config,
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    suggested_total: int,
    state: PortfolioState,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now()
    path: Path = config.history_file()
    path.parent.mkdir(parents=True, exist_ok=True)

    write_header = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(HEADER)
        writer.writerow(
            [
                now.isoformat(),
                round(snapshot.vix, 2),
                round(snapshot.vix_change_pct, 2),
                round(snapshot.nasdaq_drawdown_pct, 2),
                round(snapshot.sox_drawdown_pct, 2) if snapshot.sox_drawdown_pct is not None else "",
                stage_result.stage,
                suggested_total,
                state.deployed_amount,
                state.remaining_amount,
            ]
        )
