"""共通データモデル定義。

投資判断ロジック・データ取得・レポート生成のすべてがこのモジュールの
データクラスを介してやり取りする。ここには判断ロジックは書かない。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketSnapshot:
    """市場全体のスナップショット（1時点分）。バックテストでも同じ形を使う。"""

    timestamp: datetime

    vix: float
    vix_prev_close: float

    nasdaq_price: float
    nasdaq_52w_high: float
    nasdaq_5dma: float | None = None
    nasdaq_prev_day_high: float | None = None

    sox_price: float | None = None
    sox_52w_high: float | None = None

    sp500_price: float | None = None
    sp500_52w_high: float | None = None

    # Stage4反転判定用：直近(既定30営業日)のVIXピーク
    vix_recent_peak: float | None = None

    # NASDAQ100のRSI(14) 、市場全体の売られすぎ/反転判定に使用
    nasdaq_rsi: float | None = None

    # 騰落レシオ等、市場の値上がり銘柄数の改善を示す任意指標(-1.0〜1.0目安、加点用)
    breadth_improving: bool | None = None

    @property
    def vix_change_pct(self) -> float:
        if not self.vix_prev_close:
            return 0.0
        return (self.vix - self.vix_prev_close) / self.vix_prev_close * 100.0

    @property
    def nasdaq_drawdown_pct(self) -> float:
        if not self.nasdaq_52w_high:
            return 0.0
        return (self.nasdaq_price - self.nasdaq_52w_high) / self.nasdaq_52w_high * 100.0

    @property
    def sox_drawdown_pct(self) -> float | None:
        if not self.sox_price or not self.sox_52w_high:
            return None
        return (self.sox_price - self.sox_52w_high) / self.sox_52w_high * 100.0

    @property
    def sp500_drawdown_pct(self) -> float | None:
        if not self.sp500_price or not self.sp500_52w_high:
            return None
        return (self.sp500_price - self.sp500_52w_high) / self.sp500_52w_high * 100.0

    @property
    def vix_drop_from_peak_pct(self) -> float | None:
        """直近VIXピークからの低下率（Stage4反転シグナルの一つ）"""
        if not self.vix_recent_peak:
            return None
        return (self.vix - self.vix_recent_peak) / self.vix_recent_peak * 100.0


@dataclass
class ReversalSignals:
    """Stage4（反転確認）判定用の個別シグナル"""

    vix_down_20pct_from_peak: bool = False
    nasdaq_above_5dma: bool = False
    nasdaq_above_prev_high: bool = False
    rsi_recovering_from_oversold: bool = False
    sox_stabilizing: bool = False
    breadth_improving_bonus: bool = False

    @property
    def required_signal_count(self) -> int:
        """必須5条件のうち満たした数（加点条件は含めない）"""
        return sum(
            [
                self.vix_down_20pct_from_peak,
                self.nasdaq_above_5dma,
                self.nasdaq_above_prev_high,
                self.rsi_recovering_from_oversold,
                self.sox_stabilizing,
            ]
        )

    @property
    def total_signal_score(self) -> int:
        """加点(騰落銘柄数改善)を含めた合計スコア"""
        return self.required_signal_count + (1 if self.breadth_improving_bonus else 0)

    def as_dict(self) -> dict:
        return {
            "vix_down_20pct_from_peak": self.vix_down_20pct_from_peak,
            "nasdaq_above_5dma": self.nasdaq_above_5dma,
            "nasdaq_above_prev_high": self.nasdaq_above_prev_high,
            "rsi_recovering_from_oversold": self.rsi_recovering_from_oversold,
            "sox_stabilizing": self.sox_stabilizing,
            "breadth_improving_bonus": self.breadth_improving_bonus,
        }


@dataclass
class StageResult:
    """市場Stage判定結果"""

    stage: int  # 0-4
    status_code: str  # WAITING / STAGE1 / STAGE2 / STAGE3 / REVERSAL_CONFIRMED
    status_label_jp: str  # 日本語表示ラベル
    pre_alert: bool = False
    reversal_signals: ReversalSignals | None = None
    reasons: list[str] = field(default_factory=list)


@dataclass
class StockData:
    """個別銘柄の生データ（データ取得層から渡される）"""

    ticker: str
    price: float
    prev_close: float
    day_change_pct: float

    high_52w: float
    ma200: float | None
    rsi14: float | None

    avg_volume_30d: float | None
    volume_today: float | None

    pe_ratio: float | None = None
    revenue_growth_yoy: float | None = None
    earnings_within_2d: bool | None = None  # 決算発表直後かどうか(判明時のみ)

    data_available: bool = True  # データ取得に失敗した場合 False

    @property
    def drawdown_from_52w_high_pct(self) -> float:
        if not self.high_52w:
            return 0.0
        return (self.price - self.high_52w) / self.high_52w * 100.0

    @property
    def ma200_deviation_pct(self) -> float | None:
        if not self.ma200:
            return None
        return (self.price - self.ma200) / self.ma200 * 100.0

    @property
    def volume_ratio(self) -> float | None:
        if not self.avg_volume_30d or self.volume_today is None:
            return None
        return self.volume_today / self.avg_volume_30d


@dataclass
class NewsCheckResult:
    """個別悪材料チェック結果"""

    relative_underperformance: bool = False  # NASDAQ100より10pt以上下落
    single_day_crash: bool = False  # 当日-10%以上
    volume_spike: bool = False  # 出来高2倍以上
    earnings_recent: bool = False  # 決算直後

    # ニュースAPI未接続のため機械的に判定できない項目 → 常に "要確認"
    needs_news_check: bool = True
    unverifiable_items: list[str] = field(
        default_factory=lambda: [
            "ガイダンス下方修正",
            "会計問題",
            "規制問題",
            "大型訴訟",
            "主要顧客喪失",
            "CEO辞任等の経営イベント",
        ]
    )

    @property
    def has_critical_flag(self) -> bool:
        return self.relative_underperformance or self.single_day_crash or self.volume_spike

    def warnings(self) -> list[str]:
        w = []
        if self.relative_underperformance:
            w.append("NASDAQ100より下落幅が大きい（個別要因の可能性）")
        if self.single_day_crash:
            w.append("当日-10%以上の急落")
        if self.volume_spike:
            w.append("出来高が平均の2倍以上")
        if self.earnings_recent:
            w.append("決算発表直後（内容要確認）")
        if self.needs_news_check:
            w.append("ニュース確認必要（自動判定不可の悪材料項目あり）")
        return w


@dataclass
class StockScore:
    """個別銘柄スコアリング結果"""

    ticker: str
    sector: str
    score: int  # 0-100
    rank: str  # S/A/B/C/D
    components: dict  # 各項目の内訳スコア
    stock_data: StockData
    news: NewsCheckResult
    warnings: list[str] = field(default_factory=list)


@dataclass
class Candidate:
    """配分後の購入候補（レポート出力用）"""

    ticker: str
    sector: str
    score: int
    rank: str
    suggested_amount: int
    warnings: list[str] = field(default_factory=list)
    stock_data: StockData | None = None
