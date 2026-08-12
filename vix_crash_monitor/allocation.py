"""投入候補額を銘柄別に配分するロジック。

ルール:
  - 購入候補（ランクS/A/B）の上位から config.allocation_weights の比率で配分する
    （既定 40% / 30% / 20% / 10%、対象が4銘柄未満の場合は残りに再配分）。
  - ランクC（様子見）・D（個別リスクあり）には配分しない。
  - 1銘柄の累積投入額が総予算の max_single_ticker_ratio を超えないようにする
    （既定25% = 200万円なら50万円）。超過分はカットし警告を出す（他銘柄への
    自動繰り越しは行わない＝人間が再配分を判断する）。
  - 同一セクター（テーマ）への投入が総投資額の max_sector_ratio を超える場合は
    警告を出す（ブロックはしない。最終判断は人間）。
"""
from __future__ import annotations

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import Candidate, StockScore
from vix_crash_monitor.portfolio_state import PortfolioState

ELIGIBLE_RANKS = {"S", "A", "B"}


def _normalized_weights(weights: list[float], n: int) -> list[float]:
    top = weights[:n]
    total = sum(top)
    if total <= 0:
        return [1.0 / n] * n if n else []
    return [w / total for w in top]


def allocate_budget(
    scores: list[StockScore],
    suggested_total: int,
    state: PortfolioState,
    config: Config,
) -> tuple[list[Candidate], list[str]]:
    """全銘柄分のCandidateリスト（配分対象外は0円）とセクター集中警告を返す。"""

    eligible = [s for s in scores if s.rank in ELIGIBLE_RANKS]
    top = eligible[: len(config.allocation_weights)]
    weights = _normalized_weights(config.allocation_weights, len(top))

    ticker_cap = round(config.crash_buying_budget * config.max_single_ticker_ratio)
    sector_map = config.ticker_sector_map()

    candidates: list[Candidate] = []
    allocated_tickers = {s.ticker for s in top}

    for score, weight in zip(top, weights):
        raw_amount = round(suggested_total * weight, -2) if suggested_total else 0  # 100円単位に丸め
        already = state.ticker_deployed_amount(score.ticker)
        room = max(0, ticker_cap - already)
        amount = min(raw_amount, room)

        warnings = list(score.warnings)
        if amount < raw_amount:
            warnings.append(
                f"1銘柄上限（総予算の{config.max_single_ticker_ratio*100:.0f}%＝{ticker_cap:,}円）に到達のため"
                f"{raw_amount:,}円→{amount:,}円に減額"
            )

        candidates.append(
            Candidate(
                ticker=score.ticker,
                sector=score.sector,
                score=score.score,
                rank=score.rank,
                suggested_amount=int(amount),
                warnings=warnings,
                stock_data=score.stock_data,
            )
        )

    for score in scores:
        if score.ticker in allocated_tickers:
            continue
        candidates.append(
            Candidate(
                ticker=score.ticker,
                sector=score.sector,
                score=score.score,
                rank=score.rank,
                suggested_amount=0,
                warnings=list(score.warnings),
                stock_data=score.stock_data,
            )
        )

    sector_warnings = _check_sector_concentration(candidates, state, config, sector_map)
    return candidates, sector_warnings


def _check_sector_concentration(
    candidates: list[Candidate],
    state: PortfolioState,
    config: Config,
    sector_map: dict,
) -> list[str]:
    existing_sector_totals = state.sector_deployed_amount(sector_map)
    new_sector_totals: dict = {}
    for c in candidates:
        if c.suggested_amount > 0:
            new_sector_totals[c.sector] = new_sector_totals.get(c.sector, 0) + c.suggested_amount

    total_invested = state.deployed_amount + sum(new_sector_totals.values())
    if total_invested <= 0:
        return []

    warnings = []
    sectors = set(existing_sector_totals) | set(new_sector_totals)
    for sector in sectors:
        sector_total = existing_sector_totals.get(sector, 0) + new_sector_totals.get(sector, 0)
        ratio = sector_total / total_invested
        if ratio > config.max_sector_ratio:
            warnings.append(
                f"セクター集中警告: {sector} への投入が総投資額の{ratio*100:.1f}%"
                f"（上限{config.max_sector_ratio*100:.0f}%）に達しています"
            )
    return warnings
