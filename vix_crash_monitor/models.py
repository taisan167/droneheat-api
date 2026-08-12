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

    timestamp: datetime  # tz-aware。市場データはAmerica/New_York基準で取得する（timeutil.py参照）

    vix: float
    vix_prev_close: float

    nasdaq_price: float
    # 直近252営業日のデータが無い場合はNone（"N/A"）とする。0.0など実在しうる値に
    # フォールバックしない＝誤った下落率を作らないため（見直しレビュー項目5）。
    nasdaq_52w_high: float | None
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
    def nasdaq_drawdown_pct(self) -> float | None:
        """NASDAQ100の52週高値比の下落率。52週高値がN/A(None)の場合はNoneを返す
        （0%と誤解されるような値を作らない。呼び出し側はNoneを"データ不足"として扱うこと）。
        """
        if self.nasdaq_52w_high is None or self.nasdaq_52w_high <= 0:
            return None
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
    def has_required_data(self) -> bool:
        """Stage判定に最低限必要なデータ（VIX・NASDAQ100下落率）が揃っているか。
        Falseの場合、Stage判定は行わず DATA_INCOMPLETE として扱うこと。
        """
        return self.nasdaq_drawdown_pct is not None

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
    """市場Stage判定結果

    【重要】stage番号(0〜4)は便宜上の連番だが、危険度が単調に増していく
    スケールではない。Stage0〜3は「暴落の深刻さ」を表す BUY_STAGE、
    Stage4は暴落そのものではなく「暴落後に反転の兆候が複数確認できた」
    ことを表す RECOVERY_SIGNAL であり、Stage3より"危険"という意味ではない。
    category フィールドでこれを明示する（詳細な設計分離は別Issueで検討）。
    """

    stage: int | None  # 0-4 / データ不足時はNone
    category: str  # "BUY_STAGE" | "RECOVERY_SIGNAL" | "NONE"
    status_code: str  # WAITING / STAGE1 / STAGE2 / STAGE3 / REVERSAL_CONFIRMED / DATA_INCOMPLETE
    status_label_jp: str  # 日本語表示ラベル
    pre_alert: bool = False
    data_incomplete: bool = False
    reversal_signals: ReversalSignals | None = None
    reasons: list[str] = field(default_factory=list)


@dataclass
class StockData:
    """個別銘柄の生データ（データ取得層から渡される）"""

    ticker: str
    price: float
    prev_close: float
    day_change_pct: float

    # 直近252営業日のデータが無い場合はNone（"N/A"）。誤った下落率を作らないため。
    high_52w: float | None
    ma200: float | None
    rsi14: float | None

    avg_volume_30d: float | None
    volume_today: float | None

    pe_ratio: float | None = None
    revenue_growth_yoy: float | None = None
    earnings_within_2d: bool | None = None  # 決算発表直後かどうか(判明時のみ)

    data_available: bool = True  # データ取得に失敗した場合 False

    @property
    def drawdown_from_52w_high_pct(self) -> float | None:
        """52週高値データが無い(None)場合はNoneを返す（0%として扱わない）。"""
        if not self.high_52w:
            return None
        return (self.price - self.high_52w) / self.high_52w * 100.0

    @property
    def has_52w_high_data(self) -> bool:
        return self.high_52w is not None and self.high_52w > 0

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

    def flags(self) -> list[str]:
        """機械可読タグ（ダッシュボード/JSON連携向け）。単純な下落率の大きさだけで
        高ランクにしないよう、これらのフラグをランク判定で優先する（見直しレビュー項目10）。
        """
        f = []
        if self.has_critical_flag:
            f.append("INDIVIDUAL_RISK")
        if self.relative_underperformance:
            f.append("RELATIVE_UNDERPERFORMANCE")
        if self.single_day_crash:
            f.append("SINGLE_DAY_CRASH")
        if self.volume_spike:
            f.append("VOLUME_SPIKE")
        if self.earnings_recent:
            f.append("EARNINGS_RECENT")
        if self.needs_news_check:
            f.append("NEWS_CHECK_REQUIRED")
        return f


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
    flags: list[str] = field(default_factory=list)  # 機械可読タグ（INDIVIDUAL_RISK等）


@dataclass
class Candidate:
    """配分後の購入候補（レポート出力用）"""

    ticker: str
    sector: str
    score: int
    rank: str
    suggested_amount: int
    warnings: list[str] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    stock_data: StockData | None = None
