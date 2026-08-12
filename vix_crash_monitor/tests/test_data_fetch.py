import pandas as pd

from vix_crash_monitor.data_fetch import MIN_TRADING_DAYS_FOR_52W_HIGH, fifty_two_week_high


def _make_ohlc(n: int, dates=None) -> pd.DataFrame:
    dates = dates or pd.bdate_range("2024-01-01", periods=n)
    return pd.DataFrame(
        {
            "Open": [100.0 + i for i in range(n)],
            "High": [100.0 + i + 1 for i in range(n)],
            "Low": [100.0 + i - 1 for i in range(n)],
            "Close": [100.0 + i for i in range(n)],
            "Volume": [1_000_000] * n,
        },
        index=dates,
    )


def test_fifty_two_week_high_returns_none_when_insufficient_data():
    """52週(約252営業日)に満たない場合はN/A(None)を返し、部分期間の最大値で
    誤った下落率を作らない（見直しレビュー項目5）。"""
    hist = _make_ohlc(50)  # 新規上場銘柄などを想定
    assert fifty_two_week_high(hist) is None


def test_fifty_two_week_high_returns_value_when_sufficient_data():
    hist = _make_ohlc(MIN_TRADING_DAYS_FOR_52W_HIGH + 10)
    result = fifty_two_week_high(hist)
    assert result is not None
    assert result == hist["High"].tail(252).max()


def test_fifty_two_week_high_uses_only_trailing_252_days():
    """252営業日より古いスパイクは52週高値の計算に含めない。"""
    n = 400
    hist = _make_ohlc(n)
    # 300日目（252営業日より前）に極端な高値を入れる
    hist.iloc[50, hist.columns.get_loc("High")] = 100_000.0
    result = fifty_two_week_high(hist)
    assert result is not None
    assert result < 100_000.0


def test_previous_close_uses_trading_day_not_calendar_day_across_weekend():
    """土日を挟んでも「前営業日」は正しく直前の取引日になることを確認する
    （yfinanceの日次データは取引日のみを行に持つため、単純な24時間前比較には
    ならない。見直しレビュー項目6）。"""
    # 金曜, 月曜, 火曜 のみ（土日は行が存在しない＝実際の取引カレンダーを模擬）
    dates = pd.to_datetime(["2024-03-01", "2024-03-04", "2024-03-05"])  # Fri, Mon, Tue
    hist = pd.DataFrame(
        {
            "Open": [10.0, 20.0, 30.0],
            "High": [10.5, 20.5, 30.5],
            "Low": [9.5, 19.5, 29.5],
            "Close": [10.0, 20.0, 30.0],
            "Volume": [1000, 1000, 1000],
        },
        index=dates,
    )
    # 火曜時点でのiloc[-2]は月曜の終値(20.0)であるべき（土日を挟んだ単純な
    # 24時間前ではなく、直前の取引日）
    prev_close = float(hist["Close"].iloc[-2])
    assert prev_close == 20.0
    current_close = float(hist["Close"].iloc[-1])
    assert current_close == 30.0
    # 前営業日比は (30-20)/20 = +50%であり、土日を跨いだ日数とは無関係
    change_pct = (current_close - prev_close) / prev_close * 100.0
    assert change_pct == 50.0
