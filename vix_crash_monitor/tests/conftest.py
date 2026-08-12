import copy
from datetime import datetime, timezone

import pytest

from vix_crash_monitor.config import load_config
from vix_crash_monitor.models import MarketSnapshot


@pytest.fixture
def config():
    return load_config()


@pytest.fixture
def tmp_config(config, tmp_path):
    """状態ファイル等を一時ディレクトリに向けたConfigコピー"""
    cfg = copy.deepcopy(config)
    cfg.raw["paths"]["state_file"] = str(tmp_path / "state.json")
    cfg.raw["paths"]["history_file"] = str(tmp_path / "history.csv")
    cfg.raw["paths"]["reports_dir"] = str(tmp_path / "reports")
    return cfg


def make_snapshot(**overrides) -> MarketSnapshot:
    defaults = dict(
        timestamp=datetime(2026, 4, 1, tzinfo=timezone.utc),
        vix=18.0,
        vix_prev_close=17.5,
        nasdaq_price=19000.0,
        nasdaq_52w_high=19500.0,  # -2.5% drawdown
        nasdaq_5dma=18900.0,
        nasdaq_prev_day_high=19100.0,
        sox_price=5000.0,
        sox_52w_high=5200.0,
        sp500_price=5500.0,
        sp500_52w_high=5600.0,
        vix_recent_peak=20.0,
        nasdaq_rsi=45.0,
    )
    defaults.update(overrides)
    return MarketSnapshot(**defaults)
