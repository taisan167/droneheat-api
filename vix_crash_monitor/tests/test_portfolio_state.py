import pytest

from vix_crash_monitor.portfolio_state import load_state, record_purchase, save_state


def test_initial_state_has_full_budget(tmp_config):
    state = load_state(tmp_config)
    assert state.total_budget == 2_000_000
    assert state.deployed_amount == 0
    assert state.remaining_amount == 2_000_000
    assert state.completed_stages == []


def test_record_purchase_updates_state(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, amount=200_000, stage=1, ticker=None, note="stage1 batch")
    assert state.deployed_amount == 200_000
    assert state.remaining_amount == 1_800_000
    assert state.completed_stages == [1]
    assert len(state.purchases) == 1


def test_record_purchase_persists_and_reloads(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, amount=100_000, stage=1, ticker="NVDA")
    save_state(state, tmp_config)

    reloaded = load_state(tmp_config)
    assert reloaded.deployed_amount == 100_000
    assert reloaded.ticker_deployed_amount("NVDA") == 100_000
    assert reloaded.completed_stages == [1]


def test_record_purchase_rejects_over_remaining_budget(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(ValueError):
        record_purchase(state, amount=3_000_000, stage=1)


def test_record_purchase_rejects_non_positive_amount(tmp_config):
    state = load_state(tmp_config)
    with pytest.raises(ValueError):
        record_purchase(state, amount=0, stage=1)


def test_stage_marked_complete_only_once(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, amount=50_000, stage=1, ticker="NVDA")
    record_purchase(state, amount=50_000, stage=1, ticker="AMD")
    assert state.completed_stages == [1]  # 重複しない
    assert state.deployed_amount == 100_000


def test_sector_deployed_amount(tmp_config):
    state = load_state(tmp_config)
    record_purchase(state, amount=100_000, stage=1, ticker="NVDA")
    record_purchase(state, amount=50_000, stage=1, ticker="AMD")
    sector_map = {"NVDA": "GPU", "AMD": "GPU"}
    totals = state.sector_deployed_amount(sector_map)
    assert totals["GPU"] == 150_000
