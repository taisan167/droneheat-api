"""個別銘柄の100点満点スコアリングとランク判定(S/A/B/C/D)。

配点内訳（合計100点）:
  - 52週高値からの下落率        : 最大25点
  - RSI（売られすぎ度合い）      : 最大15点
  - 200日移動平均乖離            : 最大15点
  - 市場全体に対する相対下落率    : 最大15点（個別要因での下落は0点+警告）
  - 出来高                       : 最大10点
  - ファンダメンタル（PER・成長率）: 最大20点

単純な下落率の大きさだけでSランクにはしない。Sランクは
「個別要因の警告が無い」かつ「相対的に市場より底堅い」かつ
「ファンダメンタルデータが確認できる」ことを追加条件とする。
"""
from __future__ import annotations

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import MarketSnapshot, StockData, StockScore
from vix_crash_monitor.news_check import check_bad_news

RELATIVE_UNDERPERFORMANCE_THRESHOLD_PT = 10.0


def _drawdown_score(drawdown_pct: float | None) -> int:
    if drawdown_pct is None:
        return 0  # 52週高値データが無い場合は加点しない（誤った高スコアを作らない）
    dd = abs(min(0.0, drawdown_pct))
    return max(0, min(25, round(dd * 0.9)))


def _rsi_score(rsi: float | None) -> int:
    if rsi is None:
        return 7
    if rsi <= 20:
        return 12  # 極端な売られすぎはパニック的で支持線が不確実なためやや控えめ
    if rsi <= 30:
        return 15  # 売られすぎからの反発が期待しやすいゾーン
    if rsi <= 40:
        return 10
    if rsi <= 50:
        return 5
    return 0


def _ma200_score(deviation_pct: float | None) -> int:
    if deviation_pct is None:
        return 7
    below = max(0.0, -deviation_pct)
    if below <= 0:
        return 0
    if below >= 30:
        return 8  # 200日線から極端に乖離＝トレンド崩壊の可能性、割引
    if below >= 15:
        return 15
    if below >= 5:
        return 10
    return 5


def _relative_score(stock_drawdown_pct: float | None, market_drawdown_pct: float | None) -> tuple[int, bool]:
    if stock_drawdown_pct is None or market_drawdown_pct is None:
        return 7, False  # 比較不能: 中立点（過大評価も個別リスク誤判定もしない）
    diff = stock_drawdown_pct - market_drawdown_pct  # 負値=市場より下げがきつい(個別要因の疑い)
    if diff <= -RELATIVE_UNDERPERFORMANCE_THRESHOLD_PT:
        return 0, True
    upper = 5.0
    lower = -RELATIVE_UNDERPERFORMANCE_THRESHOLD_PT
    ratio = (diff - lower) / (upper - lower)
    score = max(0, min(15, round(15 * ratio)))
    return score, False


def _volume_score(volume_ratio: float | None, single_day_crash: bool) -> int:
    if volume_ratio is None:
        return 5
    if single_day_crash and volume_ratio >= 2.0:
        return 0  # 急落+出来高急増＝投げ売りの可能性、加点しない
    if 1.3 <= volume_ratio < 2.0:
        return 10
    if volume_ratio < 1.3:
        return 5
    return 3


def _fundamental_score(stock: StockData) -> int:
    pe_score = None
    growth_score = None
    if stock.pe_ratio is not None:
        if stock.pe_ratio <= 0:
            pe_score = 2
        elif stock.pe_ratio <= 25:
            pe_score = 10
        elif stock.pe_ratio <= 40:
            pe_score = 6
        else:
            pe_score = 3
    if stock.revenue_growth_yoy is not None:
        g = stock.revenue_growth_yoy
        if g >= 20:
            growth_score = 10
        elif g >= 5:
            growth_score = 6
        elif g >= 0:
            growth_score = 3
        else:
            growth_score = 0

    parts = [s for s in (pe_score, growth_score) if s is not None]
    if not parts:
        return 10  # データ未取得: 中立点（加点も減点もしない）
    if len(parts) == 1:
        return min(20, parts[0] * 2)
    return pe_score + growth_score


def assign_rank(score: int, has_critical_flag: bool, single_day_crash: bool, volume_spike: bool, components: dict) -> str:
    if has_critical_flag:
        # 個別悪材料が明確に検出された場合は無条件でランクを下げる
        return "D" if (single_day_crash or volume_spike) else "C"

    if score >= 80:
        # Sランクの追加条件：相対的に市場より底堅く、ファンダメンタルが確認でき、
        # 複数の指標が実際に加点している（単一要因の急落だけで高スコアになるのを防ぐ）
        contributing = sum(1 for v in components.values() if v > 0)
        if components.get("relative", 0) > 0 and components.get("fundamental", 0) >= 10 and contributing >= 5:
            return "S"
        return "A"
    if score >= 65:
        return "A"
    if score >= 50:
        return "B"
    if score >= 35:
        return "C"
    return "D"


def score_stock(stock: StockData, market: MarketSnapshot, sector: str) -> StockScore:
    news = check_bad_news(stock, market)

    if not stock.data_available:
        return StockScore(
            ticker=stock.ticker,
            sector=sector,
            score=0,
            rank="D",
            components={},
            stock_data=stock,
            news=news,
            warnings=["価格データ取得不可のため評価できません"],
            flags=["DATA_UNAVAILABLE"],
        )

    relative_score, _underperform_flag = _relative_score(
        stock.drawdown_from_52w_high_pct, market.nasdaq_drawdown_pct
    )

    components = {
        "drawdown": _drawdown_score(stock.drawdown_from_52w_high_pct),
        "rsi": _rsi_score(stock.rsi14),
        "ma200": _ma200_score(stock.ma200_deviation_pct),
        "relative": relative_score,
        "volume": _volume_score(stock.volume_ratio, news.single_day_crash),
        "fundamental": _fundamental_score(stock),
    }
    total = max(0, min(100, sum(components.values())))

    rank = assign_rank(
        total,
        has_critical_flag=news.has_critical_flag,
        single_day_crash=news.single_day_crash,
        volume_spike=news.volume_spike,
        components=components,
    )

    warnings = news.warnings()
    flags = news.flags()
    if not stock.has_52w_high_data:
        warnings.append("52週高値データ不足のため下落率評価不可（N/A扱い）")
        flags.append("FIFTY_TWO_WEEK_HIGH_UNAVAILABLE")

    return StockScore(
        ticker=stock.ticker,
        sector=sector,
        score=total,
        rank=rank,
        components=components,
        stock_data=stock,
        news=news,
        warnings=warnings,
        flags=flags,
    )


def score_watchlist(
    stock_data: dict[str, StockData], market: MarketSnapshot, config: Config
) -> list[StockScore]:
    sector_map = config.ticker_sector_map()
    scores = [
        score_stock(data, market, sector_map.get(ticker, "OTHER"))
        for ticker, data in stock_data.items()
    ]
    scores.sort(key=lambda s: (-s.score, s.ticker))
    return scores
