import json

import pytest

from vix_crash_monitor.portfolio_state import (
    PortfolioState,
    PortfolioStateError,
    load_state,
    record_purchase,
    save_state,
    validate_invariants,
)


def test_initial_state_has_full_budget(tmp_config):
    state = load_state(tmp_config)
    assert state.total_budget == 2_000_000
    assert state.deployed_amount == 0
    assert state.remaining_amount == 2_000_000
    assert state.completed_stages == []


def test_record_purchase_updates_state(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=200_000, stage=1, ticker=None, note="stage1 batch")
    assert state.deployed_amount == 200_000
    assert state.remaining_amount == 1_800_000
    assert state.completed_stages == [1]
    assert len(state.purchases) == 1


def test_record_purchase_persists_and_reloads(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    save_state(state, tmp_config)

    reloaded = load_state(tmp_config)
    assert reloaded.deployed_amount == 100_000
    assert reloaded.ticker_deployed_amount("NVDA") == 100_000
    assert reloaded.completed_stages == [1]


def test_record_purchase_rejects_over_remaining_budget(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=3_000_000, stage=1)


def test_record_purchase_rejects_non_positive_amount(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=0, stage=1)


def test_record_purchase_rejects_negative_amount(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=-1000, stage=1)


def test_record_purchase_rejects_non_integer_amount(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount="200000", stage=1)  # type: ignore[arg-type]


def test_record_purchase_rejects_nonexistent_stage(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=100_000, stage=5)


def test_record_purchase_rejects_invalid_ticker(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=100_000, ticker="")
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=100_000, ticker="12345")
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=100_000, ticker="nv da!")


def test_stage_marked_complete_only_once(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=50_000, stage=1, ticker="NVDA")
    record_purchase(state, tmp_config, amount=50_000, stage=1, ticker="AMD")
    assert state.completed_stages == [1]  # 重複しない
    assert state.deployed_amount == 100_000


def test_duplicate_stage_level_record_rejected(tmp_config):
    """Stage単位の一括記録（ticker未指定）を同一Stageで2回行うと拒否される"""
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=200_000, stage=1, ticker=None)
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=100_000, stage=1, ticker=None)


def test_ticker_level_records_allowed_multiple_times_same_stage(tmp_config):
    """銘柄別記録はticker違いなら同一Stageで複数回記録できる"""
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="AMD")
    assert state.deployed_amount == 200_000
    assert state.completed_stages == [1]


def test_sector_deployed_amount(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    record_purchase(state, tmp_config, amount=50_000, stage=1, ticker="AMD")
    sector_map = {"NVDA": "GPU", "AMD": "GPU"}
    totals = state.sector_deployed_amount(sector_map)
    assert totals["GPU"] == 150_000


def test_ticker_cap_enforced_at_record_time(tmp_config):
    """1銘柄の累積投入額は総予算の25%（50万円）を超えて記録できない"""
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=480_000, stage=1, ticker="NVDA")
    with pytest.raises(PortfolioStateError):
        record_purchase(state, tmp_config, amount=50_000, stage=2, ticker="NVDA")


# ---- 不変条件 (invariants) ----

def test_invariants_pass_for_clean_state(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=200_000, stage=1, ticker="NVDA")
    validate_invariants(state, tmp_config)  # raises on failure


def test_invariants_reject_deployed_exceeds_total(tmp_config):
    state = PortfolioState(total_budget=2_000_000, deployed_amount=2_500_000, remaining_amount=-500_000)
    with pytest.raises(PortfolioStateError):
        validate_invariants(state, tmp_config)


def test_invariants_reject_negative_remaining(tmp_config):
    state = PortfolioState(total_budget=2_000_000, deployed_amount=2_000_000, remaining_amount=-1)
    with pytest.raises(PortfolioStateError):
        validate_invariants(state, tmp_config)


def test_invariants_reject_inconsistent_totals(tmp_config):
    state = PortfolioState(total_budget=2_000_000, deployed_amount=100_000, remaining_amount=1_000_000)
    with pytest.raises(PortfolioStateError):
        validate_invariants(state, tmp_config)


def test_invariants_reject_ticker_over_cap(tmp_config):
    state = PortfolioState(
        total_budget=2_000_000,
        deployed_amount=600_000,
        remaining_amount=1_400_000,
        purchases=[{"amount": 600_000, "ticker": "NVDA", "stage": 1, "timestamp": "x", "note": ""}],
    )
    with pytest.raises(PortfolioStateError):
        validate_invariants(state, tmp_config)


# ---- state.json破損時の挙動 ----

def test_load_state_recovers_from_backup_when_corrupted(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    save_state(state, tmp_config)  # backup created on first overwrite is of the (empty) prior version

    record_purchase(state, tmp_config, amount=50_000, stage=2, ticker="AMD")
    save_state(state, tmp_config)  # now backup holds the 1-purchase version

    path = tmp_config.state_file()
    path.write_text("{ this is not valid json", encoding="utf-8")

    reloaded = load_state(tmp_config)
    assert reloaded.deployed_amount == 100_000  # バックアップ(1件目)から復旧


def test_load_state_raises_when_no_backup_available(tmp_config):
    path = tmp_config.state_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ not valid json at all", encoding="utf-8")

    with pytest.raises(PortfolioStateError):
        load_state(tmp_config)


def test_save_state_creates_backup_file(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    save_state(state, tmp_config)

    record_purchase(state, tmp_config, amount=50_000, stage=2, ticker="AMD")
    save_state(state, tmp_config)

    backup_path = tmp_config.state_file().with_name("state.backup.json")
    assert backup_path.exists()
    backup_data = json.loads(backup_path.read_text(encoding="utf-8"))
    assert backup_data["deployed_amount"] == 100_000  # 直前の世代


def test_save_state_is_atomic_no_tmp_file_left_behind(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, tmp_config, amount=100_000, stage=1, ticker="NVDA")
    save_state(state, tmp_config)

    tmp_path = tmp_config.state_file().with_suffix(".json.tmp")
    assert not tmp_path.exists()
    assert tmp_config.state_file().exists()
