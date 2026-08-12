from vix_crash_monitor.models import StockData
from vix_crash_monitor.scoring import score_stock
from vix_crash_monitor.tests.conftest import make_snapshot


def _healthy_dip_stock(ticker="NVDA") -> StockData:
    return StockData(
        ticker=ticker,
        price=100.0,
        prev_close=102.0,
        day_change_pct=-2.0,
        high_52w=118.0,  # -15.3% drawdown, in line with (not much worse than) the market
        ma200=115.0,  # below 200MA
        rsi14=28.0,  # oversold recovery zone
        avg_volume_30d=1_000_000,
        volume_today=1_400_000,  # 1.4x volume, not a spike
        pe_ratio=22.0,
        revenue_growth_yoy=25.0,
    )


def test_data_unavailable_stock_gets_zero_score_and_d_rank():
    market = make_snapshot()
    stock = StockData(
        ticker="ZZZZ",
        price=0,
        prev_close=0,
        day_change_pct=0,
        high_52w=0,
        ma200=None,
        rsi14=None,
        avg_volume_30d=None,
        volume_today=None,
        data_available=False,
    )
    result = score_stock(stock, market, "OTHER")
    assert result.score == 0
    assert result.rank == "D"


def test_healthy_dip_scores_high_and_can_reach_s(config):
    market = make_snapshot(nasdaq_price=17550, nasdaq_52w_high=19500)  # -10% market drawdown
    stock = _healthy_dip_stock()
    result = score_stock(stock, market, "GPU")
    assert result.score >= 70
    assert result.rank in ("S", "A")
    assert not result.news.has_critical_flag


def test_pure_price_drop_without_fundamentals_is_not_s_rank():
    """52週高値からの下落率だけが突出していても、他の裏付けが弱ければSにしない"""
    market = make_snapshot(nasdaq_price=17550, nasdaq_52w_high=19500)
    stock = StockData(
        ticker="RISKY",
        price=50.0,
        prev_close=52.0,
        day_change_pct=-3.8,
        high_52w=140.0,  # -64% drawdown (極端な下落)
        ma200=None,
        rsi14=None,
        avg_volume_30d=None,
        volume_today=None,
        pe_ratio=None,
        revenue_growth_yoy=None,
    )
    result = score_stock(stock, market, "OTHER")
    assert result.rank != "S"


def test_relative_underperformance_flagged_and_capped_rank():
    # NASDAQ100は-10%だが、この銘柄は当日-12%かつ市場より大幅に弱い
    market = make_snapshot(nasdaq_price=17550, nasdaq_52w_high=19500)
    stock = StockData(
        ticker="WEAK",
        price=80.0,
        prev_close=91.0,
        day_change_pct=-12.0,
        high_52w=140.0,  # -42.9% drawdown, market only -10% -> underperformance flag
        ma200=100.0,
        rsi14=25.0,
        avg_volume_30d=1_000_000,
        volume_today=2_500_000,  # 2.5x volume spike
        pe_ratio=15.0,
        revenue_growth_yoy=10.0,
    )
    result = score_stock(stock, market, "OTHER")
    assert result.news.relative_underperformance is True
    assert result.news.single_day_crash is True
    assert result.news.volume_spike is True
    assert result.rank == "D"
    assert any("ニュース確認必要" in w for w in result.warnings)


# ---- 見直しレビュー項目10: スコア異常チェック ----

def test_extreme_drop_with_individual_red_flags_is_not_s_ranked():
    """NASDAQ100-12%に対し、架空銘柄A(-50%, RSI20, 出来高4倍, 当日-20%)は
    下落率だけ見れば突出しているが、個別リスクフラグにより高ランクにしない。
    INDIVIDUAL_RISK / NEWS_CHECK_REQUIRED フラグが立つこと。"""
    market = make_snapshot(nasdaq_price=17160, nasdaq_52w_high=19500)  # -12.0%
    stock = StockData(
        ticker="FAKE_A",
        price=50.0,
        prev_close=62.5,
        day_change_pct=-20.0,
        high_52w=100.0,  # -50%
        ma200=90.0,
        rsi14=20.0,
        avg_volume_30d=1_000_000,
        volume_today=4_000_000,  # 4x volume
        pe_ratio=None,
        revenue_growth_yoy=None,
    )
    result = score_stock(stock, market, "OTHER")
    assert result.rank not in ("S", "A")
    assert result.rank == "D"
    assert "INDIVIDUAL_RISK" in result.flags
    assert "NEWS_CHECK_REQUIRED" in result.flags
    assert "SINGLE_DAY_CRASH" in result.flags
    assert "VOLUME_SPIKE" in result.flags


def test_missing_52w_high_does_not_inflate_score():
    """52週高値データが無い(None)場合、下落率不明として加点せず、
    誤って高スコア・高ランクにしない（見直しレビュー項目5・10）。"""
    market = make_snapshot(nasdaq_price=17550, nasdaq_52w_high=19500)
    stock = StockData(
        ticker="NEWCO",
        price=50.0,
        prev_close=51.0,
        day_change_pct=-2.0,
        high_52w=None,  # データ不足（新規上場等）
        ma200=None,
        rsi14=None,
        avg_volume_30d=1_000_000,
        volume_today=1_100_000,
        pe_ratio=None,
        revenue_growth_yoy=None,
    )
    result = score_stock(stock, market, "OTHER")
    assert result.components["drawdown"] == 0
    assert result.rank != "S"
    assert "FIFTY_TWO_WEEK_HIGH_UNAVAILABLE" in result.flags
    assert any("52週高値データ不足" in w for w in result.warnings)
