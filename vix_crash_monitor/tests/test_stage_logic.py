from vix_crash_monitor.stage_logic import (
    compute_reversal_signals,
    compute_suggested_allocation,
    determine_stage,
    is_pre_alert,
)
from vix_crash_monitor.tests.conftest import make_snapshot


def test_stage0_waiting(config):
    snap = make_snapshot(vix=18.0, nasdaq_price=19000, nasdaq_52w_high=19500)
    result = determine_stage(snap, config)
    assert result.stage == 0
    assert result.status_code == "WAITING"
    assert not result.pre_alert


def test_pre_alert_when_vix_spikes_but_below_25(config):
    snap = make_snapshot(vix=22.0, vix_prev_close=18.0)  # +22.2%
    assert is_pre_alert(snap, config)
    result = determine_stage(snap, config)
    assert result.pre_alert is True
    assert result.stage == 0
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    assert amount == 0
    assert stages == []


def test_pre_alert_not_triggered_above_vix_ceiling(config):
    # VIXが25以上ならPRE-ALERTの定義対象外（Stage1以上の判定に回る）
    snap = make_snapshot(vix=26.0, vix_prev_close=20.0, nasdaq_price=17000, nasdaq_52w_high=19500)
    assert is_pre_alert(snap, config) is False


def test_stage1_conditions(config):
    snap = make_snapshot(vix=26.0, nasdaq_price=17400, nasdaq_52w_high=19500)  # -10.8%
    result = determine_stage(snap, config)
    assert result.stage == 1
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    assert amount == 200_000  # 10% of 2,000,000
    assert stages == [1]


def test_stage1_requires_both_conditions(config):
    # VIXは条件を満たすがNASDAQ下落率が不足
    snap = make_snapshot(vix=26.0, nasdaq_price=18500, nasdaq_52w_high=19500)  # -5.1%
    result = determine_stage(snap, config)
    assert result.stage == 0


def test_stage2_conditions_and_cumulative_allocation(config):
    snap = make_snapshot(vix=31.4, nasdaq_price=16340, nasdaq_52w_high=19500)  # -16.2%
    result = determine_stage(snap, config)
    assert result.stage == 2
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    # Stage1未完了分(20万) + Stage2分(40万) = 60万
    assert amount == 600_000
    assert stages == [1, 2]


def test_stage2_after_stage1_completed_only_adds_stage2(config):
    snap = make_snapshot(vix=31.4, nasdaq_price=16340, nasdaq_52w_high=19500)
    result = determine_stage(snap, config)
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[1])
    assert amount == 400_000
    assert stages == [2]


def test_already_completed_stage_gives_zero(config):
    snap = make_snapshot(vix=26.0, nasdaq_price=17400, nasdaq_52w_high=19500)
    result = determine_stage(snap, config)
    assert result.stage == 1
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[1])
    assert amount == 0
    assert stages == []


def test_stage3_or_condition_vix_only(config):
    # NASDAQ下落は浅いがVIXが40以上 → Stage3（OR条件）
    snap = make_snapshot(vix=41.0, nasdaq_price=18500, nasdaq_52w_high=19500)  # -5.1%
    result = determine_stage(snap, config)
    assert result.stage == 3


def test_stage3_or_condition_drawdown_only(config):
    snap = make_snapshot(vix=22.0, nasdaq_price=15600, nasdaq_52w_high=19500)  # -20%
    result = determine_stage(snap, config)
    assert result.stage == 3


def test_stage4_not_confirmed_without_completed_stage3(config):
    # Stage3条件を満たしていても、Stage3が過去に完了記録されていなければStage4評価はしない
    snap = make_snapshot(
        vix=45.0,
        nasdaq_price=14000,
        nasdaq_52w_high=19500,
        vix_recent_peak=60.0,  # -25% from peak
        nasdaq_5dma=13800,
        nasdaq_prev_day_high=13900,
        nasdaq_rsi=35,
        sox_price=3800,
        sox_52w_high=5200,
    )
    result = determine_stage(snap, config, completed_stages=[1, 2])
    assert result.stage == 3


def test_stage4_confirmed_with_multiple_reversal_signals(config):
    snap = make_snapshot(
        vix=32.0,
        nasdaq_price=14200,
        nasdaq_52w_high=19500,  # still deep drawdown -> stage3 OR via vix? vix=32 <40, dd=-27%>=20 -> stage3
        vix_recent_peak=45.0,  # (32-45)/45 = -28.9% <= -20% ✓
        nasdaq_5dma=14000,  # price above 5dma ✓
        nasdaq_prev_day_high=14100,  # price above prev high ✓
        nasdaq_rsi=38,  # recovering ✓
        sox_price=4000,
        sox_52w_high=5200,  # sox_dd = -23.1%, nasdaq_dd=-27.2% -> sox_dd >= nasdaq_dd-3 -> stabilizing ✓
    )
    result = determine_stage(snap, config, completed_stages=[1, 2, 3])
    assert result.stage == 4
    assert result.status_code == "REVERSAL_CONFIRMED"


def test_reversal_signals_require_multiple_confirmations(config):
    snap = make_snapshot(vix=30.0, vix_recent_peak=31.0)  # only ~3% down from peak, not enough
    signals = compute_reversal_signals(snap)
    assert signals.vix_down_20pct_from_peak is False


def test_pure_vix_drop_alone_is_not_enough_for_stage4(config):
    # VIXが下がっただけ（他シグナルは満たさない）ではStage4に昇格しない
    snap = make_snapshot(
        vix=32.0,
        nasdaq_price=14200,
        nasdaq_52w_high=19500,
        vix_recent_peak=45.0,  # VIX low signal satisfied
        nasdaq_5dma=14500,  # price below 5dma -> NOT satisfied
        nasdaq_prev_day_high=14500,  # NOT satisfied
        nasdaq_rsi=20,  # still deeply oversold, not "recovering" -> NOT satisfied
        sox_price=3500,
        sox_52w_high=5200,  # sox_dd much worse than nasdaq_dd -> NOT stabilizing
    )
    result = determine_stage(snap, config, completed_stages=[1, 2, 3])
    assert result.stage == 3


# ---- 見直しレビュー項目1: 資金二重計上チェック ----

def test_no_double_allocation_on_stage_re_entry(config):
    """Stage1到達→record-buy記録→Stage0に戻る→再びStage1に該当、しても
    投入候補は再表示されない（人間が記録済みのStageは二重計上しない）。"""
    stage1_snap = make_snapshot(vix=26.0, nasdaq_price=17400, nasdaq_52w_high=19500)  # -10.8%
    result = determine_stage(stage1_snap, config)
    assert result.stage == 1
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    assert amount == 200_000
    assert stages == [1]

    # 人間がrecord-buyで記録した想定 → completed_stages=[1]
    completed_stages = [1]

    waiting_snap = make_snapshot(vix=18.0, nasdaq_price=19000, nasdaq_52w_high=19500)
    waiting_result = determine_stage(waiting_snap, config, completed_stages=completed_stages)
    assert waiting_result.stage == 0

    stage1_again = determine_stage(stage1_snap, config, completed_stages=completed_stages)
    assert stage1_again.stage == 1
    amount2, stages2 = compute_suggested_allocation(stage1_again, config, completed_stages)
    assert amount2 == 0
    assert stages2 == []


# ---- 見直しレビュー項目2: Stage飛び越しテスト ----

def test_stage_skip_from_zero_to_three_combines_all_unexecuted_stages(config):
    """Stage0から一気にStage3に到達した場合、Stage1+2+3の未実行配分合計
    (10%+20%+30%=60%=120万円)が提示される。ただし一括購入の指示ではない
    （文言の検証はreport.pyのテストで行う）。"""
    snap = make_snapshot(vix=45.0, nasdaq_price=14625, nasdaq_52w_high=19500)  # -25%
    result = determine_stage(snap, config, completed_stages=[])
    assert result.stage == 3
    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    assert amount == 1_200_000
    assert stages == [1, 2, 3]


# ---- 見直しレビュー項目3: Stage低下後の挙動 ----

def test_completed_stages_not_resuggested_after_vix_round_trip(config):
    """VIX 42 → 28 → 33 と変動しても、既に完了したStage1・2は再提示されない。"""
    high_vix_snap = make_snapshot(vix=42.0, nasdaq_price=14625, nasdaq_52w_high=19500)  # -25%, stage3
    result1 = determine_stage(high_vix_snap, config, completed_stages=[])
    assert result1.stage == 3
    amount1, stages1 = compute_suggested_allocation(result1, config, completed_stages=[])
    assert stages1 == [1, 2, 3]
    completed_stages = [1, 2, 3]  # 人間が全額記録した想定

    low_vix_snap = make_snapshot(vix=28.0, nasdaq_price=18800, nasdaq_52w_high=19500)  # -3.6%
    result2 = determine_stage(low_vix_snap, config, completed_stages=completed_stages)
    assert result2.stage == 0

    mid_vix_snap = make_snapshot(vix=33.0, nasdaq_price=16380, nasdaq_52w_high=19500)  # -16%, stage2条件
    result3 = determine_stage(mid_vix_snap, config, completed_stages=completed_stages)
    assert result3.stage == 2
    amount3, stages3 = compute_suggested_allocation(result3, config, completed_stages)
    assert amount3 == 0
    assert stages3 == []


# ---- 見直しレビュー項目4: BUY_STAGE / RECOVERY_SIGNAL カテゴリ ----

def test_stage_category_buy_stage_for_0_to_3(config):
    for vix, price, note in [
        (18.0, 19000, "stage0"),
        (26.0, 17400, "stage1"),
        (31.4, 16340, "stage2"),
        (45.0, 14000, "stage3"),
    ]:
        snap = make_snapshot(vix=vix, nasdaq_price=price, nasdaq_52w_high=19500)
        result = determine_stage(snap, config)
        assert result.category == "BUY_STAGE", note


def test_stage_category_recovery_signal_for_stage4(config):
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
    result = determine_stage(snap, config, completed_stages=[1, 2, 3])
    assert result.stage == 4
    assert result.category == "RECOVERY_SIGNAL"


# ---- 見直しレビュー項目5/7: データ不足 → DATA_INCOMPLETE ----

def test_data_incomplete_when_nasdaq_52w_high_missing(config):
    """NASDAQ100の52週高値がNone（データ不足）の場合、憶測でStage判定しない。"""
    snap = make_snapshot(vix=45.0, nasdaq_price=14000, nasdaq_52w_high=None)
    result = determine_stage(snap, config)
    assert result.stage is None
    assert result.data_incomplete is True
    assert result.status_code == "DATA_INCOMPLETE"
    assert result.category == "NONE"

    amount, stages = compute_suggested_allocation(result, config, completed_stages=[])
    assert amount == 0
    assert stages == []


def test_market_snapshot_drawdown_none_when_52w_high_missing():
    snap = make_snapshot(nasdaq_52w_high=None)
    assert snap.nasdaq_drawdown_pct is None
    assert snap.has_required_data is False
