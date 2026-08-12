from vix_crash_monitor.allocation import allocate_budget
from vix_crash_monitor.models import StockData, StockScore
from vix_crash_monitor.portfolio_state import load_state


def _stub_stock_data(ticker: str) -> StockData:
    return StockData(
        ticker=ticker,
        price=100.0,
        prev_close=101.0,
        day_change_pct=-1.0,
        high_52w=150.0,
        ma200=120.0,
        rsi14=30.0,
        avg_volume_30d=1_000_000,
        volume_today=1_200_000,
    )


def _score(ticker, sector, score, rank, warnings=None):
    from vix_crash_monitor.models import NewsCheckResult

    return StockScore(
        ticker=ticker,
        sector=sector,
        score=score,
        rank=rank,
        components={"drawdown": score},
        stock_data=_stub_stock_data(ticker),
        news=NewsCheckResult(),
        warnings=warnings or [],
    )


def test_allocation_follows_weight_table(tmp_config):
    state = load_state(tmp_config)
    scores = [
        _score("NVDA", "GPU", 86, "S"),
        _score("AVGO", "SEMICONDUCTOR", 80, "A"),
        _score("AMD", "GPU", 75, "A"),
        _score("TSM", "SEMICONDUCTOR", 71, "A"),
    ]
    candidates, warnings = allocate_budget(scores, 400_000, state, tmp_config)
    allocated = {c.ticker: c.suggested_amount for c in candidates if c.suggested_amount > 0}
    assert allocated["NVDA"] == 160_000
    assert allocated["AVGO"] == 120_000
    assert allocated["AMD"] == 80_000
    assert allocated["TSM"] == 40_000


def test_rank_c_and_d_get_no_allocation(tmp_config):
    state = load_state(tmp_config)
    scores = [
        _score("NVDA", "GPU", 86, "S"),
        _score("XXXX", "OTHER", 40, "C"),
        _score("YYYY", "OTHER", 10, "D"),
    ]
    candidates, _ = allocate_budget(scores, 200_000, state, tmp_config)
    by_ticker = {c.ticker: c.suggested_amount for c in candidates}
    assert by_ticker["NVDA"] == 200_000
    assert by_ticker["XXXX"] == 0
    assert by_ticker["YYYY"] == 0


def test_single_ticker_cap_enforced(tmp_config):
    from vix_crash_monitor.portfolio_state import record_purchase

    state = load_state(tmp_config)
    # 既に上限(50万円)近くまで投入済み
    record_purchase(state, tmp_config, amount=480_000, stage=1, ticker="NVDA")

    scores = [_score("NVDA", "GPU", 90, "S")]
    candidates, _ = allocate_budget(scores, 200_000, state, tmp_config)
    nvda = next(c for c in candidates if c.ticker == "NVDA")
    # 上限50万円 - 既存48万円 = 残り2万円までしか追加できない
    assert nvda.suggested_amount == 20_000
    assert any("上限" in w for w in nvda.warnings)


def test_sector_concentration_warning(tmp_config):
    state = load_state(tmp_config)
    scores = [
        _score("NVDA", "GPU", 90, "S"),
        _score("AMD", "GPU", 85, "S"),
    ]
    candidates, warnings = allocate_budget(scores, 200_000, state, tmp_config)
    assert any("GPU" in w for w in warnings)


def test_no_sector_warning_when_diversified(tmp_config):
    state = load_state(tmp_config)
    scores = [
        _score("NVDA", "GPU", 90, "S"),
        _score("AVGO", "SEMICONDUCTOR", 85, "A"),
        _score("ASML", "SEMI_EQUIPMENT", 80, "A"),
        _score("MSFT", "BIG_TECH", 75, "A"),
    ]
    candidates, warnings = allocate_budget(scores, 200_000, state, tmp_config)
    assert warnings == []
