"""タイムゾーンの明示的な管理。

方針（レビュー項目9）:
  - 市場データ（VIX/NASDAQ100等の価格データ・「前営業日」の判定）は
    America/New_York（米国市場のタイムゾーン）を基準に扱う。
  - ユーザー向け表示（レポートのヘッダー日時等）は Asia/Tokyo を基準に表示する。
  - 「前日」が日本時間と米国市場時間で混同されないよう、変換は必ずこのモジュール
    経由で行い、naiveなdatetime.now()を直接使わない。

なお、yfinanceの日次ヒストリカルデータは米国取引所の実際の営業日のみを
インデックスに持つため（土日・祝日は行が存在しない）、「前営業日」の
判定自体はDataFrameの行を1つ前に辿ることで自動的に正しくなる
（単純な24時間前・カレンダー日ベースの比較ではない）。本モジュールは
その上でタイムスタンプの表示・保存に使うタイムゾーンを一貫させるためのもの。
"""
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

MARKET_TZ = ZoneInfo("America/New_York")
DISPLAY_TZ = ZoneInfo("Asia/Tokyo")
UTC = ZoneInfo("UTC")


def now_market() -> datetime:
    """現在時刻をAmerica/New_York（市場データ基準）で取得する。"""
    return datetime.now(MARKET_TZ)


def now_display() -> datetime:
    """現在時刻をAsia/Tokyo（ユーザー表示基準）で取得する。"""
    return datetime.now(DISPLAY_TZ)


def to_display_tz(dt: datetime) -> datetime:
    """任意のdatetimeをAsia/Tokyo表示用に変換する。naiveな場合はUTCとみなす。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(DISPLAY_TZ)


def to_market_tz(dt: datetime) -> datetime:
    """任意のdatetimeをAmerica/New_York基準に変換する。naiveな場合はUTCとみなす。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(MARKET_TZ)


def format_display(dt: datetime, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """Asia/Tokyo基準でフォーマットし、JST表記を明示する。"""
    return f"{to_display_tz(dt).strftime(fmt)} JST"
