"""レポート生成（テキスト表示 / ChatGPT連携用JSON・Markdown）。

【重要】本レポートは投資"判断支援"のための情報整理のみを行う。
発注・自動売買は一切行わない。最終判断は必ず人間が行うこと。
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import Candidate, MarketSnapshot, StageResult
from vix_crash_monitor.portfolio_state import PortfolioState

MARKET_STATUS_MAP = {
    0: "WAITING",
    1: "FIRST_BUY_ZONE",
    2: "STRONG_CORRECTION",
    3: "PANIC_ZONE",
    4: "REVERSAL_CONFIRMED",
}

DISCLAIMER = (
    "本レポートは投資判断支援情報であり、投資助言・売買推奨ではありません。\n"
    "自動発注・自動売買は一切行っていません。最終的な購入判断は必ずご自身で行ってください。"
)


def market_status(stage_result: StageResult) -> str:
    if stage_result.pre_alert:
        return "PRE_ALERT"
    return MARKET_STATUS_MAP.get(stage_result.stage, "UNKNOWN")


def build_text_report(
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    state: PortfolioState,
    suggested_total: int,
    candidates: list[Candidate],
    sector_warnings: list[str],
    now: datetime,
) -> str:
    lines = []
    lines.append("=" * 48)
    lines.append("MARKET CRASH BUY MONITOR")
    lines.append(now.strftime("%Y-%m-%d %H:%M"))
    lines.append("=" * 48)

    lines.append("VIX")
    lines.append(f"{snapshot.vix:.1f}")
    lines.append(f"前日比 {snapshot.vix_change_pct:+.1f}%")

    lines.append("NASDAQ100")
    lines.append(f"52週高値比 {snapshot.nasdaq_drawdown_pct:.1f}%")

    if snapshot.sox_drawdown_pct is not None:
        lines.append("SOX")
        lines.append(f"52週高値比 {snapshot.sox_drawdown_pct:.1f}%")

    if snapshot.sp500_drawdown_pct is not None:
        lines.append("S&P500")
        lines.append(f"52週高値比 {snapshot.sp500_drawdown_pct:.1f}%")

    lines.append("-" * 48)
    lines.append("MARKET STAGE")
    if stage_result.pre_alert:
        lines.append("PRE-ALERT")
    else:
        lines.append(f"STAGE {stage_result.stage}")
    lines.append(stage_result.status_label_jp)
    lines.append("-" * 48)

    lines.append("TOTAL BUDGET")
    lines.append(f"{state.total_budget:,}円")
    lines.append("投入済み")
    lines.append(f"{state.deployed_amount:,}円")
    lines.append("今回投入候補")
    lines.append(f"{suggested_total:,}円")
    lines.append("残り")
    lines.append(f"{state.remaining_amount:,}円")
    lines.append("-" * 48)

    allocated = [c for c in candidates if c.suggested_amount > 0]
    if allocated:
        lines.append("BUY CANDIDATES")
        for i, c in enumerate(allocated, start=1):
            lines.append(f"{i}. {c.ticker}")
            lines.append(f"Score：{c.score}")
            lines.append(f"Rank：{c.rank}")
            if c.stock_data is not None and c.stock_data.data_available:
                lines.append(f"高値比 {c.stock_data.drawdown_from_52w_high_pct:.0f}%")
                if c.stock_data.rsi14 is not None:
                    lines.append(f"RSI {c.stock_data.rsi14:.0f}")
            lines.append("投入候補")
            lines.append(f"{c.suggested_amount:,}円")
            for w in c.warnings:
                lines.append("WARNING：")
                lines.append(w)
            lines.append("-" * 48)
    else:
        lines.append("BUY CANDIDATES")
        lines.append("該当銘柄なし（Stage未到達、または投入候補額0円）")
        lines.append("-" * 48)

    watch_only = [c for c in candidates if c.suggested_amount == 0 and c.rank in ("C", "D")]
    if watch_only:
        lines.append("WATCH / RISK LIST（配分対象外）")
        for c in watch_only:
            lines.append(f"{c.ticker}  Score:{c.score}  Rank:{c.rank}")
            for w in c.warnings:
                lines.append(f"  - {w}")
        lines.append("-" * 48)

    if sector_warnings:
        lines.append("SECTOR CONCENTRATION WARNINGS")
        for w in sector_warnings:
            lines.append(w)
        lines.append("-" * 48)

    lines.append(DISCLAIMER)
    lines.append("=" * 48)
    return "\n".join(lines)


def build_json_report(
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    state: PortfolioState,
    suggested_total: int,
    candidates: list[Candidate],
    sector_warnings: list[str],
    now: datetime,
) -> dict:
    allocated = [c for c in candidates if c.suggested_amount > 0]
    return {
        "generated_at": now.isoformat(),
        "market_stage": stage_result.stage,
        "market_status": market_status(stage_result),
        "pre_alert": stage_result.pre_alert,
        "status_label": stage_result.status_label_jp,
        "total_budget": state.total_budget,
        "deployed_amount": state.deployed_amount,
        "suggested_new_allocation": suggested_total,
        "remaining_budget_after_candidate_allocation": state.remaining_amount - suggested_total,
        "remaining_budget_before_candidate_allocation": state.remaining_amount,
        "vix": round(snapshot.vix, 2),
        "vix_change_pct": round(snapshot.vix_change_pct, 2),
        "nasdaq_drawdown": round(snapshot.nasdaq_drawdown_pct, 2),
        "sox_drawdown": round(snapshot.sox_drawdown_pct, 2) if snapshot.sox_drawdown_pct is not None else None,
        "sp500_drawdown": round(snapshot.sp500_drawdown_pct, 2) if snapshot.sp500_drawdown_pct is not None else None,
        "completed_stages": sorted(state.completed_stages),
        "sector_warnings": sector_warnings,
        "candidates": [
            {
                "ticker": c.ticker,
                "sector": c.sector,
                "score": c.score,
                "rank": c.rank,
                "suggested_amount": c.suggested_amount,
                "warning": len(c.warnings) > 0,
                "warning_details": c.warnings,
            }
            for c in allocated
        ],
        "watchlist_all": [
            {
                "ticker": c.ticker,
                "sector": c.sector,
                "score": c.score,
                "rank": c.rank,
                "suggested_amount": c.suggested_amount,
                "warning_details": c.warnings,
            }
            for c in candidates
        ],
        "disclaimer": DISCLAIMER,
    }


def build_chatgpt_prompt(
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    suggested_total: int,
    candidates: list[Candidate],
    now: datetime,
) -> str:
    allocated = [c for c in candidates if c.suggested_amount > 0]

    lines = ["以下は現在の市場監視結果です。", ""]
    lines.append(f"日時：{now.strftime('%Y-%m-%d %H:%M')}")
    lines.append(f"VIX：{snapshot.vix:.1f}（前日比{snapshot.vix_change_pct:+.1f}%）")
    lines.append(f"NASDAQ100高値比：{snapshot.nasdaq_drawdown_pct:.1f}%")
    if snapshot.sox_drawdown_pct is not None:
        lines.append(f"SOX高値比：{snapshot.sox_drawdown_pct:.1f}%")
    if snapshot.sp500_drawdown_pct is not None:
        lines.append(f"S&P500高値比：{snapshot.sp500_drawdown_pct:.1f}%")
    lines.append("")
    lines.append("現在の判定：")
    lines.append("PRE-ALERT" if stage_result.pre_alert else f"STAGE {stage_result.stage}")
    lines.append(stage_result.status_label_jp)
    lines.append("")
    lines.append("今回の投入候補額：")
    lines.append(f"{suggested_total:,}円")
    lines.append("")
    lines.append("候補：")
    if allocated:
        for i, c in enumerate(allocated, start=1):
            lines.append(f"{i}. {c.ticker}")
            lines.append(f"Score {c.score}")
            lines.append(f"{c.suggested_amount:,}円")
            if c.warnings:
                lines.append(f"警告: {'; '.join(c.warnings)}")
    else:
        lines.append("該当なし")
    lines.append("")
    lines.append("このデータをもとに、")
    lines.append("1. 今は本当に買い場か")
    lines.append("2. まだ待つべきか")
    lines.append("3. 個別悪材料はないか")
    lines.append("4. 配分は妥当か")
    lines.append("5. 投入額を減らすべきか")
    lines.append("を分析してください。")
    lines.append("")
    lines.append("※本データは投資助言ではありません。最終判断はご自身で行ってください。")
    return "\n".join(lines)


def write_reports(
    config: Config,
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    state: PortfolioState,
    suggested_total: int,
    candidates: list[Candidate],
    sector_warnings: list[str],
    now: datetime | None = None,
) -> tuple[str, dict, str]:
    now = now or datetime.now()
    reports_dir: Path = config.reports_dir()
    reports_dir.mkdir(parents=True, exist_ok=True)

    text_report = build_text_report(snapshot, stage_result, state, suggested_total, candidates, sector_warnings, now)
    json_report = build_json_report(snapshot, stage_result, state, suggested_total, candidates, sector_warnings, now)
    chatgpt_prompt = build_chatgpt_prompt(snapshot, stage_result, suggested_total, candidates, now)

    with open(reports_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(json_report, f, ensure_ascii=False, indent=2)

    with open(reports_dir / "chatgpt_prompt.md", "w", encoding="utf-8") as f:
        f.write(chatgpt_prompt)

    return text_report, json_report, chatgpt_prompt
