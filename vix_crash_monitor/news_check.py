"""個別悪材料チェック（市場暴落と個別企業悪化の区別）。

機械的に判定できる項目（相対下落率・当日騰落率・出来高・決算直後）は
StockDataとMarketSnapshotから計算する。
ガイダンス下方修正・会計問題・規制問題・大型訴訟・主要顧客喪失・
CEO辞任などはニュースソースが無ければ判定不能なため、常に
「ニュース確認必要」として明示し、自動的に買い推奨はしない。

将来ニュースAPIを接続する場合は fetch_news_flags() を実装して
check_bad_news() に渡すことで、unverifiable_items の一部を
機械判定に置き換えられるように設計している。
"""
from __future__ import annotations

from vix_crash_monitor.models import MarketSnapshot, NewsCheckResult, StockData

RELATIVE_UNDERPERFORMANCE_THRESHOLD_PT = 10.0
SINGLE_DAY_CRASH_THRESHOLD_PCT = -10.0
VOLUME_SPIKE_RATIO = 2.0


def check_bad_news(stock: StockData, market: MarketSnapshot) -> NewsCheckResult:
    if not stock.data_available:
        return NewsCheckResult(needs_news_check=True)

    stock_drawdown = stock.drawdown_from_52w_high_pct
    market_drawdown = market.nasdaq_drawdown_pct
    relative_underperformance = (stock_drawdown - market_drawdown) <= -RELATIVE_UNDERPERFORMANCE_THRESHOLD_PT

    single_day_crash = stock.day_change_pct <= SINGLE_DAY_CRASH_THRESHOLD_PCT

    volume_spike = False
    if stock.volume_ratio is not None:
        volume_spike = stock.volume_ratio >= VOLUME_SPIKE_RATIO

    earnings_recent = bool(stock.earnings_within_2d)

    return NewsCheckResult(
        relative_underperformance=relative_underperformance,
        single_day_crash=single_day_crash,
        volume_spike=volume_spike,
        earnings_recent=earnings_recent,
        needs_news_check=True,  # 定性的な悪材料はニュースソース未接続のため常に要確認
    )
