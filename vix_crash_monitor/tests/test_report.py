from datetime import datetime, timezone

from vix_crash_monitor.allocation import allocate_budget
from vix_crash_monitor.models import NewsCheckResult, StockData, StockScore
from vix_crash_monitor.portfolio_state import load_state
from vix_crash_monitor.report import (
    build_data_incomplete_report,
    build_json_report,
    build_text_report,
    market_status,
)
from vix_crash_monitor.stage_logic import compute_suggested_allocation, determine_stage
from vix_crash_monitor.tests.conftest import make_snapshot


def _score(ticker, sector, score, rank):
    return StockScore(
        ticker=ticker,
        sector=sector,
        score=score,
        rank=rank,
        components={"drawdown": score},
        stock_data=StockData(
            ticker=ticker,
            price=100.0,
            prev_close=101.0,
            day_change_pct=-1.0,
            high_52w=150.0,
            ma200=120.0,
            rsi14=30.0,
            avg_volume_30d=1_000_000,
            volume_today=1_200_000,
        ),
        news=NewsCheckResult(),
        warnings=[],
    )


def test_multi_stage_suggestion_wording_is_not_a_directive(tmp_config):
    """Stage0→3への飛び越し時、レポートの文言が「一括投入推奨」と断定しないこと。
    「未実行配分合計上限」であることを明示すること（見直しレビュー項目2）。
    """
    state = load_state(tmp_config)
    snap = make_snapshot(vix=45.0, nasdaq_price=14625, nasdaq_52w_high=19500)  # -25%
    stage_result = determine_stage(snap, tmp_config, completed_stages=[])
    assert stage_result.stage == 3
    suggested_total, target_stages = compute_suggested_allocation(stage_result, tmp_config, [])
    assert suggested_total == 1_200_000
    assert target_stages == [1, 2, 3]

    scores = [_score("NVDA", "GPU", 90, "S")]
    candidates, sector_warnings = allocate_budget(scores, suggested_total, state, tmp_config)

    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    text = build_text_report(
        snap, stage_result, state, suggested_total, candidates, sector_warnings, now, target_stages
    )

    assert "Stage1〜3の未実行配分合計上限" in text
    assert "一括投入を推奨するものではありません" in text
    assert "1,200,000円" in text

    json_report = build_json_report(
        snap, stage_result, state, suggested_total, candidates, sector_warnings, now, target_stages
    )
    assert json_report["suggested_allocation_is_combined_upper_bound"] is True
    assert json_report["suggested_allocation_target_stages"] == [1, 2, 3]


def test_single_stage_suggestion_has_no_combined_upper_bound_note(tmp_config):
    state = load_state(tmp_config)
    snap = make_snapshot(vix=26.0, nasdaq_price=17400, nasdaq_52w_high=19500)  # stage1のみ
    stage_result = determine_stage(snap, tmp_config, completed_stages=[])
    suggested_total, target_stages = compute_suggested_allocation(stage_result, tmp_config, [])
    assert target_stages == [1]

    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    text = build_text_report(snap, stage_result, state, suggested_total, [], [], now, target_stages)
    assert "未実行配分合計上限" not in text

    json_report = build_json_report(snap, stage_result, state, suggested_total, [], [], now, target_stages)
    assert json_report["suggested_allocation_is_combined_upper_bound"] is False


def test_stage4_report_includes_recovery_vs_buy_stage_clarification(tmp_config):
    state = load_state(tmp_config)
    snap = make_snapshot(
        vix=32.0,
        nasdaq_price=14200,
        nasdaq_52w_high=19500,
        vix_recent_peak=45.0,
        nasdaq_5dma=14000,
        nasdaq_prev_day_high=14100,
        nasdaq_rsi=38,
        sox_price=4000,
        sox_52w_high=5200,
    )
    stage_result = determine_stage(snap, tmp_config, completed_stages=[1, 2, 3])
    assert stage_result.stage == 4

    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    text = build_text_report(snap, stage_result, state, 0, [], [], now, [])
    assert "Stage3よりさらに危険" not in text.split("※")[0]  # 本文中に誤解を招く直接表現はない
    assert "反転" in text
    assert "[RECOVERY_SIGNAL]" in text

    json_report = build_json_report(snap, stage_result, state, 0, [], [], now, [])
    assert json_report["market_stage_category"] == "RECOVERY_SIGNAL"
    assert json_report["stage4_note"] is not None


def test_data_incomplete_report_text_and_json(tmp_config):
    state = load_state(tmp_config)
    now = datetime(2026, 4, 1, tzinfo=timezone.utc)
    text, json_report = build_data_incomplete_report(state, "NASDAQ100データ取得失敗", now)
    assert "DATA INCOMPLETE" in text
    assert "市場判定保留" in text
    assert "0円" in text
    assert json_report["market_status"] == "DATA_INCOMPLETE"
    assert json_report["data_incomplete"] is True
    assert json_report["suggested_new_allocation"] == 0
    assert json_report["candidates"] == []


def test_market_status_data_incomplete():
    from vix_crash_monitor.models import StageResult

    result = StageResult(
        stage=None, category="NONE", status_code="DATA_INCOMPLETE", status_label_jp="", data_incomplete=True
    )
    assert market_status(result) == "DATA_INCOMPLETE"
