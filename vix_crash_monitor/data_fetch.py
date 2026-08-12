"""市場データ・個別銘柄データの取得層。

判断ロジック(stage_logic.py / scoring.py)から意図的に分離している。
これにより、実データ取得(yfinance)とバックテスト用の過去データ取得を
差し替えても、ロジック側は一切変更不要になる（Phase18: バックテスト対応）。

ネットワークに接続できない環境では例外 DataFetchError を送出するので、
呼び出し側で捕捉してユーザーにエラーを表示すること。
"""
from __future__ import annotations

import pandas as pd

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import MarketSnapshot, StockData
from vix_crash_monitor.timeutil import now_market

# 52週高値を計算するために必要な最低営業日数。252営業日(約1年)に満たない
# データしか無い場合（例: 新規上場銘柄や祝日の多い期間）は52週高値を
# "N/A"(None)として扱い、実データ不足なのに誤った下落率を出さないようにする。
# 米国市場は年間で概ね250営業日前後のため、多少の休場を許容して200営業日を閾値とする。
MIN_TRADING_DAYS_FOR_52W_HIGH = 200


class DataFetchError(RuntimeError):
    pass


def _get_yfinance():
    try:
        import yfinance as yf  # 遅延importでyfinance未インストール環境でも他モジュールは使える
    except ImportError as e:  # pragma: no cover
        raise DataFetchError(
            "yfinanceがインストールされていません。 pip install -r vix_crash_monitor/requirements.txt を実行してください。"
        ) from e
    return yf


def compute_rsi(close: pd.Series, period: int = 14) -> float | None:
    if len(close) < period + 1:
        return None
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / period, min_periods=period, adjust=False).mean()
    last_gain = avg_gain.iloc[-1]
    last_loss = avg_loss.iloc[-1]
    if last_loss == 0:
        return 100.0
    rs = last_gain / last_loss
    return float(100 - (100 / (1 + rs)))


def fifty_two_week_high(hist: pd.DataFrame) -> float | None:
    """直近252営業日（当日含む）の高値。データ不足の場合はNone("N/A")を返す。

    見直しレビュー項目5: データ不足時にNoneを返さず0や部分期間の最大値を
    返すと、実際より下落率が浅く(または深く)見える誤った値を生成しうるため、
    明示的にN/A扱いにする。
    """
    if len(hist) < MIN_TRADING_DAYS_FOR_52W_HIGH:
        return None
    return float(hist["High"].tail(252).max())


def _history(yf, ticker: str, period: str = "1y") -> pd.DataFrame:
    try:
        hist = yf.Ticker(ticker).history(period=period, interval="1d")
    except Exception as e:  # ネットワーク断・レート制限等、yfinance側の例外を統一的に扱う
        raise DataFetchError(f"{ticker} の価格データ取得中にエラーが発生しました: {e}") from e
    if hist is None or hist.empty:
        raise DataFetchError(f"{ticker} の価格データを取得できませんでした")
    return hist


def fetch_market_snapshot(config: Config, breadth_improving: bool | None = None) -> MarketSnapshot:
    """VIX / NASDAQ100 / SOX / S&P500 を取得しMarketSnapshotを構築する。

    VIXとNASDAQ100はStage判定に必須のため取得失敗時は例外を送出する。
    SOX/S&P500は補助指標のため取得できなくてもNoneのまま処理を続ける。
    """
    yf = _get_yfinance()
    indices = config.market_indices

    # yfinanceの日次ヒストリカルデータは実際の米国取引所営業日のみを行に持つため
    # （土日・祝日は行自体が存在しない）、iloc[-2]は単純な24時間前ではなく
    # 正しい「直前の取引日」の終値になる（レビュー項目6）。
    vix_hist = _history(yf, indices["vix"])
    vix = float(vix_hist["Close"].iloc[-1])
    vix_prev_close = float(vix_hist["Close"].iloc[-2]) if len(vix_hist) > 1 else vix
    vix_recent_peak = float(vix_hist["Close"].tail(30).max())

    ndx_hist = _history(yf, indices["nasdaq100"])
    nasdaq_price = float(ndx_hist["Close"].iloc[-1])
    nasdaq_52w_high = fifty_two_week_high(ndx_hist)  # データ不足ならNone("N/A")
    nasdaq_5dma = float(ndx_hist["Close"].tail(5).mean()) if len(ndx_hist) >= 5 else None
    nasdaq_prev_day_high = float(ndx_hist["High"].iloc[-2]) if len(ndx_hist) > 1 else None
    nasdaq_rsi = compute_rsi(ndx_hist["Close"])

    sox_price = sox_52w_high = None
    try:
        sox_hist = _history(yf, indices["sox"])
        sox_price = float(sox_hist["Close"].iloc[-1])
        sox_52w_high = fifty_two_week_high(sox_hist)
    except DataFetchError:
        pass

    sp500_price = sp500_52w_high = None
    try:
        sp_hist = _history(yf, indices["sp500"])
        sp500_price = float(sp_hist["Close"].iloc[-1])
        sp500_52w_high = fifty_two_week_high(sp_hist)
    except DataFetchError:
        pass

    return MarketSnapshot(
        timestamp=now_market(),
        vix=vix,
        vix_prev_close=vix_prev_close,
        nasdaq_price=nasdaq_price,
        nasdaq_52w_high=nasdaq_52w_high,
        nasdaq_5dma=nasdaq_5dma,
        nasdaq_prev_day_high=nasdaq_prev_day_high,
        sox_price=sox_price,
        sox_52w_high=sox_52w_high,
        sp500_price=sp500_price,
        sp500_52w_high=sp500_52w_high,
        vix_recent_peak=vix_recent_peak,
        nasdaq_rsi=nasdaq_rsi,
        breadth_improving=breadth_improving,
    )


def fetch_stock_data(ticker: str) -> StockData:
    """個別銘柄データを取得する。失敗した場合は data_available=False で返す
    （呼び出し側で「データ取得不可」として扱い、自動買い推奨に使わない）。
    """
    yf = _get_yfinance()
    try:
        hist = _history(yf, ticker)
        price = float(hist["Close"].iloc[-1])
        prev_close = float(hist["Close"].iloc[-2]) if len(hist) > 1 else price
        day_change_pct = (price - prev_close) / prev_close * 100.0 if prev_close else 0.0

        high_52w = fifty_two_week_high(hist)  # データ不足ならNone("N/A")、誤った下落率にしない
        ma200 = float(hist["Close"].tail(200).mean()) if len(hist) >= 200 else None
        rsi14 = compute_rsi(hist["Close"])

        avg_volume_30d = float(hist["Volume"].tail(30).mean()) if len(hist) >= 5 else None
        volume_today = float(hist["Volume"].iloc[-1])

        pe_ratio = None
        revenue_growth_yoy = None
        earnings_within_2d = None
        try:
            info = yf.Ticker(ticker).get_info()
            pe_ratio = info.get("trailingPE")
            revenue_growth_yoy = info.get("revenueGrowth")
            if revenue_growth_yoy is not None:
                revenue_growth_yoy = float(revenue_growth_yoy) * 100.0
        except Exception:
            pass  # ファンダメンタルは取得できなくても致命的ではない

        try:
            cal = yf.Ticker(ticker).calendar
            if isinstance(cal, dict):
                edates = cal.get("Earnings Date")
                if edates:
                    edate = edates[0] if isinstance(edates, list) else edates
                    today = pd.Timestamp.utcnow().normalize()
                    edate_ts = pd.Timestamp(edate)
                    if abs((edate_ts.tz_localize(None) - today.tz_localize(None)).days) <= 2:
                        earnings_within_2d = True
        except Exception:
            pass

        return StockData(
            ticker=ticker,
            price=price,
            prev_close=prev_close,
            day_change_pct=day_change_pct,
            high_52w=high_52w,
            ma200=ma200,
            rsi14=rsi14,
            avg_volume_30d=avg_volume_30d,
            volume_today=volume_today,
            pe_ratio=pe_ratio,
            revenue_growth_yoy=revenue_growth_yoy,
            earnings_within_2d=earnings_within_2d,
            data_available=True,
        )
    except DataFetchError:
        return StockData(
            ticker=ticker,
            price=0.0,
            prev_close=0.0,
            day_change_pct=0.0,
            high_52w=None,
            ma200=None,
            rsi14=None,
            avg_volume_30d=None,
            volume_today=None,
            data_available=False,
        )


def fetch_watchlist_data(tickers: list[str]) -> dict[str, StockData]:
    return {t: fetch_stock_data(t) for t in tickers}
