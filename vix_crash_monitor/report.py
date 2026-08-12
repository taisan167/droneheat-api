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
from vix_crash_monitor.timeutil import format_display

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

STAGE4_NOTE = (
    "※Stage4は「Stage3よりさらに危険」という意味ではありません。暴落そのものの深さを表す"
    "指標(Stage0〜3, BUY_STAGE)とは別軸で、暴落後に複数の反転シグナルが確認できたことを示す"
    "指標(RECOVERY_SIGNAL)です。"
)

MULTI_STAGE_NOTE = (
    "※これは複数Stage分の未実行配分を合算した「配分上限の目安」であり、"
    "一括投入を推奨するものではありません。実際に何回に分けて投入するかはご自身で判断してください。"
)


def market_status(stage_result: StageResult) -> str:
    if stage_result.data_incomplete:
        return "DATA_INCOMPLETE"
    if stage_result.pre_alert:
        return "PRE_ALERT"
    return MARKET_STATUS_MAP.get(stage_result.stage, "UNKNOWN")


def build_data_incomplete_report(
    state: PortfolioState,
    reason: str,
    now: datetime | None = None,
) -> tuple[str, dict]:
    """VIXやNASDAQ100など主要データが取得できなかった場合のレポート。

    憶測でStage判定を行わず、"DATA INCOMPLETE" であることを明示する
    （見直しレビュー項目7）。投入候補額は常に0円。
    """
    now = now or datetime.now()
    lines = [
        "=" * 48,
        "MARKET CRASH BUY MONITOR",
        format_display(now),
        "=" * 48,
        "DATA INCOMPLETE",
        "市場判定保留",
        "",
        f"理由: {reason}",
        "",
        "主要な市場データ（VIX・NASDAQ100）が取得できなかったため、",
        "Stage判定・投入候補額の計算を行っていません。",
        "投入候補額: 0円（判定保留中は常に0円）",
        "-" * 48,
        "TOTAL BUDGET",
        f"{state.total_budget:,}円",
        "投入済み",
        f"{state.deployed_amount:,}円",
        "残り",
        f"{state.remaining_amount:,}円",
        "-" * 48,
        DISCLAIMER,
        "=" * 48,
    ]
    text_report = "\n".join(lines)

    json_report = {
        "generated_at": now.isoformat(),
        "market_stage": None,
        "market_status": "DATA_INCOMPLETE",
        "data_incomplete": True,
        "reason": reason,
        "status_label": "市場判定保留（データ不足）",
        "total_budget": state.total_budget,
        "deployed_amount": state.deployed_amount,
        "suggested_new_allocation": 0,
        "remaining_budget_after_candidate_allocation": state.remaining_amount,
        "remaining_budget_before_candidate_allocation": state.remaining_amount,
        "vix": None,
        "nasdaq_drawdown": None,
        "candidates": [],
        "watchlist_all": [],
        "disclaimer": DISCLAIMER,
    }
    return text_report, json_report


def _suggested_allocation_label(target_stages: list[int]) -> str:
    """今回投入候補額の見出し。複数Stage分の合算時は"合計上限"であることを明示し、
    一括投入の指示だと誤解されないようにする（見直しレビュー項目2）。
    """
    if len(target_stages) <= 1:
        return "今回投入候補"
    lo, hi = min(target_stages), max(target_stages)
    return f"今回投入候補（Stage{lo}〜{hi}の未実行配分合計上限）"


def build_text_report(
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    state: PortfolioState,
    suggested_total: int,
    candidates: list[Candidate],
    sector_warnings: list[str],
    now: datetime,
    target_stages: list[int] | None = None,
) -> str:
    target_stages = target_stages or []
    lines = []
    lines.append("=" * 48)
    lines.append("MARKET CRASH BUY MONITOR")
    lines.append(format_display(now))
    lines.append("=" * 48)

    lines.append("VIX")
    lines.append(f"{snapshot.vix:.1f}")
    lines.append(f"前日比 {snapshot.vix_change_pct:+.1f}%")

    lines.append("NASDAQ100")
    lines.append(f"52週高値比 {snapshot.nasdaq_drawdown_pct:.1f}%" if snapshot.nasdaq_drawdown_pct is not None else "52週高値比 N/A（データ不足）")

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
        lines.append(f"STAGE {stage_result.stage}  [{stage_result.category}]")
    lines.append(stage_result.status_label_jp)
    if stage_result.stage == 4:
        lines.append(STAGE4_NOTE)
    lines.append("-" * 48)

    lines.append("TOTAL BUDGET")
    lines.append(f"{state.total_budget:,}円")
    lines.append("投入済み")
    lines.append(f"{state.deployed_amount:,}円")
    lines.append(_suggested_allocation_label(target_stages))
    lines.append(f"{suggested_total:,}円")
    if len(target_stages) > 1:
        lines.append(MULTI_STAGE_NOTE)
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
                dd = c.stock_data.drawdown_from_52w_high_pct
                lines.append(f"高値比 {dd:.0f}%" if dd is not None else "高値比 N/A（52週データ不足）")
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
    target_stages: list[int] | None = None,
) -> dict:
    target_stages = target_stages or []
    allocated = [c for c in candidates if c.suggested_amount > 0]
    return {
        "generated_at": now.isoformat(),
        "market_stage": stage_result.stage,
        "market_stage_category": stage_result.category,  # BUY_STAGE / RECOVERY_SIGNAL / NONE
        "market_status": market_status(stage_result),
        "pre_alert": stage_result.pre_alert,
        "data_incomplete": stage_result.data_incomplete,
        "status_label": stage_result.status_label_jp,
        "total_budget": state.total_budget,
        "deployed_amount": state.deployed_amount,
        "suggested_new_allocation": suggested_total,
        "suggested_allocation_target_stages": target_stages,
        "suggested_allocation_is_combined_upper_bound": len(target_stages) > 1,
        "remaining_budget_after_candidate_allocation": state.remaining_amount - suggested_total,
        "remaining_budget_before_candidate_allocation": state.remaining_amount,
        "vix": round(snapshot.vix, 2),
        "vix_change_pct": round(snapshot.vix_change_pct, 2),
        "nasdaq_drawdown": round(snapshot.nasdaq_drawdown_pct, 2) if snapshot.nasdaq_drawdown_pct is not None else None,
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
                "flags": c.flags,
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
                "flags": c.flags,
            }
            for c in candidates
        ],
        "disclaimer": DISCLAIMER,
        "stage4_note": STAGE4_NOTE if stage_result.stage == 4 else None,
    }


def build_chatgpt_prompt(
    snapshot: MarketSnapshot,
    stage_result: StageResult,
    suggested_total: int,
    candidates: list[Candidate],
    now: datetime,
    target_stages: list[int] | None = None,
) -> str:
    target_stages = target_stages or []
    allocated = [c for c in candidates if c.suggested_amount > 0]

    lines = ["以下は現在の市場監視結果です。", ""]
    lines.append(f"日時：{format_display(now)}")
    lines.append(f"VIX：{snapshot.vix:.1f}（前日比{snapshot.vix_change_pct:+.1f}%）")
    if snapshot.nasdaq_drawdown_pct is not None:
        lines.append(f"NASDAQ100高値比：{snapshot.nasdaq_drawdown_pct:.1f}%")
    else:
        lines.append("NASDAQ100高値比：N/A（データ不足）")
    if snapshot.sox_drawdown_pct is not None:
        lines.append(f"SOX高値比：{snapshot.sox_drawdown_pct:.1f}%")
    if snapshot.sp500_drawdown_pct is not None:
        lines.append(f"S&P500高値比：{snapshot.sp500_drawdown_pct:.1f}%")
    lines.append("")
    lines.append("現在の判定：")
    lines.append("PRE-ALERT" if stage_result.pre_alert else f"STAGE {stage_result.stage}（{stage_result.category}）")
    lines.append(stage_result.status_label_jp)
    if stage_result.stage == 4:
        lines.append(STAGE4_NOTE)
    lines.append("")
    lines.append(_suggested_allocation_label(target_stages) + "：")
    lines.append(f"{suggested_total:,}円")
    if len(target_stages) > 1:
        lines.append(MULTI_STAGE_NOTE)
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
    target_stages: list[int] | None = None,
) -> tuple[str, dict, str]:
    now = now or datetime.now()
    target_stages = target_stages or []
    reports_dir: Path = config.reports_dir()
    reports_dir.mkdir(parents=True, exist_ok=True)

    text_report = build_text_report(
        snapshot, stage_result, state, suggested_total, candidates, sector_warnings, now, target_stages
    )
    json_report = build_json_report(
        snapshot, stage_result, state, suggested_total, candidates, sector_warnings, now, target_stages
    )
    chatgpt_prompt = build_chatgpt_prompt(snapshot, stage_result, suggested_total, candidates, now, target_stages)

    with open(reports_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(json_report, f, ensure_ascii=False, indent=2)

    with open(reports_dir / "chatgpt_prompt.md", "w", encoding="utf-8") as f:
        f.write(chatgpt_prompt)

    return text_report, json_report, chatgpt_prompt


def write_data_incomplete_report(config: Config, state: PortfolioState, reason: str, now: datetime | None = None) -> tuple[str, dict]:
    """DATA_INCOMPLETE時のレポートを生成しファイルに書き出す。"""
    now = now or datetime.now()
    reports_dir: Path = config.reports_dir()
    reports_dir.mkdir(parents=True, exist_ok=True)

    text_report, json_report = build_data_incomplete_report(state, reason, now)

    with open(reports_dir / "latest.json", "w", encoding="utf-8") as f:
        json.dump(json_report, f, ensure_ascii=False, indent=2)

    return text_report, json_report
