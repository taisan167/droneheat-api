"""設定ファイル(config.yaml)の読み込み。

このモジュールは設定値をPythonオブジェクトとして提供するだけで、
投資判断ロジックは一切含まない（ロジックは stage_logic.py 等に分離）。
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

BASE_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = BASE_DIR / "config.yaml"


@dataclass
class StageThreshold:
    vix_min: float = 0.0
    nasdaq_drawdown_min_pct: float = 0.0
    allocation_ratio: float = 0.0
    min_reversal_signals: int = 3


@dataclass
class Config:
    raw: dict = field(default_factory=dict)

    # ---- portfolio ----
    @property
    def crash_buying_budget(self) -> int:
        return int(self.raw["portfolio"]["crash_buying_budget"])

    @property
    def max_single_ticker_ratio(self) -> float:
        return float(self.raw["portfolio"]["max_single_ticker_ratio"])

    @property
    def max_sector_ratio(self) -> float:
        return float(self.raw["portfolio"]["max_sector_ratio"])

    # ---- stage thresholds ----
    def stage_threshold(self, stage: int) -> StageThreshold:
        d = self.raw["stage_thresholds"][f"stage{stage}"]
        return StageThreshold(
            vix_min=float(d.get("vix_min", 0)),
            nasdaq_drawdown_min_pct=float(d.get("nasdaq_drawdown_min_pct", 0)),
            allocation_ratio=float(d.get("allocation_ratio", 0)),
            min_reversal_signals=int(d.get("min_reversal_signals", 3)),
        )

    # ---- pre-alert ----
    @property
    def pre_alert_vix_change_threshold_pct(self) -> float:
        return float(self.raw["pre_alert"]["vix_change_threshold_pct"])

    @property
    def pre_alert_vix_ceiling(self) -> float:
        return float(self.raw["pre_alert"]["vix_ceiling"])

    # ---- indices ----
    @property
    def market_indices(self) -> dict:
        return dict(self.raw["market_indices"])

    # ---- allocation ----
    @property
    def allocation_weights(self) -> list:
        return list(self.raw["allocation_weights"])

    @property
    def rank_thresholds(self) -> dict:
        return dict(self.raw["rank_thresholds"])

    # ---- watchlist ----
    @property
    def watchlist(self) -> dict:
        return copy.deepcopy(self.raw["watchlist"])

    def ticker_sector_map(self) -> dict:
        mapping = {}
        for sector, tickers in self.watchlist.items():
            for t in tickers:
                mapping[t] = sector
        return mapping

    def all_tickers(self) -> list:
        tickers = []
        for tickers_in_sector in self.watchlist.values():
            tickers.extend(tickers_in_sector)
        return tickers

    # ---- paths ----
    # 相対パスはvix_crash_monitor/配下として解決する。絶対パスが指定された場合は
    # そのまま使う（テストで一時ディレクトリに差し替える場合などに利用）。
    def _resolve_path(self, key: str) -> Path:
        p = Path(self.raw["paths"][key])
        return p if p.is_absolute() else BASE_DIR / p

    def state_file(self) -> Path:
        return self._resolve_path("state_file")

    def history_file(self) -> Path:
        return self._resolve_path("history_file")

    def reports_dir(self) -> Path:
        return self._resolve_path("reports_dir")


def load_config(path: Path | str | None = None) -> Config:
    p = Path(path) if path else DEFAULT_CONFIG_PATH
    with open(p, "r", encoding="utf-8") as f:
        raw: dict[str, Any] = yaml.safe_load(f)
    return Config(raw=raw)
