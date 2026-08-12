"""バックテスト雛形（Phase10）。

目的: 「このルールを2020年コロナショックや2022年NASDAQ下落時に適用したら
どうなったか」を検証できるようにする。

判定ロジック(stage_logic.py)とデータ取得処理(ここでのヒストリカル取得)を
分離しているため、本番運用中のロジック変更がそのままバックテストにも
反映される。逆にバックテスト専用のデータ取得を差し替えても
stage_logic.py 側の変更は不要。

【注意】これは雛形（スケルトン）です。ここでの「購入シミュレーション」は
過去データに対する機械的な検証用であり、実際の発注とは一切関係ありません。

使い方:
  python vix_crash_monitor/backtest.py --start 2020-01-01 --end 2020-12-31
  python vix_crash_monitor/backtest.py --start 2022-01-01 --end 2022-12-31
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from vix_crash_monitor.config import Config, load_config
from vix_crash_monitor.data_fetch import compute_rsi, fifty_two_week_high
from vix_crash_monitor.models import MarketSnapshot
from vix_crash_monitor.stage_logic import compute_suggested_allocation, determine_stage


def _download(ticker: str, start: str, end: str):
    import yfinance as yf

    hist = yf.Ticker(ticker).history(start=start, end=end, interval="1d")
    if hist is None or hist.empty:
        raise RuntimeError(f"{ticker} の過去データを取得できませんでした（期間: {start}〜{end}）")
    return hist


def load_historical_snapshots(config: Config, start: str, end: str) -> list[MarketSnapshot]:
    """指定期間のMarketSnapshotを日次で構築する（データ取得層。ロジックは含まない）。

    52週高値・200日線などの計算に十分なバッファを確保するため、
    実際のダウンロード開始日は `start` の約400日前とする。
    """
    buffer_start = (pd.Timestamp(start) - pd.Timedelta(days=400)).strftime("%Y-%m-%d")
    indices = config.market_indices

    vix_hist = _download(indices["vix"], buffer_start, end)
    ndx_hist = _download(indices["nasdaq100"], buffer_start, end)
    try:
        sox_hist = _download(indices["sox"], buffer_start, end)
    except RuntimeError:
        sox_hist = None

    dates = ndx_hist.loc[start:end].index
    snapshots: list[MarketSnapshot] = []

    for date in dates:
        vix_window = vix_hist.loc[:date]
        ndx_window = ndx_hist.loc[:date]
        if len(vix_window) < 2 or len(ndx_window) < 5:
            continue

        vix = float(vix_window["Close"].iloc[-1])
        vix_prev = float(vix_window["Close"].iloc[-2])
        vix_peak = float(vix_window["Close"].tail(30).max())

        nasdaq_price = float(ndx_window["Close"].iloc[-1])
        nasdaq_52w_high = fifty_two_week_high(ndx_window)  # データ不足期間はNone("N/A")
        nasdaq_5dma = float(ndx_window["Close"].tail(5).mean())
        nasdaq_prev_high = float(ndx_window["High"].iloc[-2])
        nasdaq_rsi = compute_rsi(ndx_window["Close"])

        sox_price = sox_52w_high = None
        if sox_hist is not None:
            sox_window = sox_hist.loc[:date]
            if len(sox_window) > 0:
                sox_price = float(sox_window["Close"].iloc[-1])
                sox_52w_high = fifty_two_week_high(sox_window)

        snapshots.append(
            MarketSnapshot(
                timestamp=date.to_pydatetime(),
                vix=vix,
                vix_prev_close=vix_prev,
                nasdaq_price=nasdaq_price,
                nasdaq_52w_high=nasdaq_52w_high,
                nasdaq_5dma=nasdaq_5dma,
                nasdaq_prev_day_high=nasdaq_prev_high,
                sox_price=sox_price,
                sox_52w_high=sox_52w_high,
                vix_recent_peak=vix_peak,
                nasdaq_rsi=nasdaq_rsi,
            )
        )

    return snapshots


def run_backtest(snapshots: list[MarketSnapshot], config: Config) -> pd.DataFrame:
    """Stage判定ロジックを日次で適用し、Stageの推移と累積投入額（シミュレーション）を返す。

    銘柄別スコアリング・配分は対象外（市場全体のタイミングルール検証が目的）。
    """
    completed_stages: list[int] = []
    cumulative_deployed = 0
    rows = []

    for snapshot in snapshots:
        stage_result = determine_stage(snapshot, config, completed_stages=completed_stages)
        suggested_total, target_stages = compute_suggested_allocation(
            stage_result, config, completed_stages
        )

        if suggested_total > 0:
            cumulative_deployed += suggested_total
            for s in target_stages:
                if s not in completed_stages:
                    completed_stages.append(s)

        rows.append(
            {
                "date": snapshot.timestamp.strftime("%Y-%m-%d"),
                "vix": round(snapshot.vix, 2),
                "vix_change_pct": round(snapshot.vix_change_pct, 2),
                "nasdaq_drawdown_pct": round(snapshot.nasdaq_drawdown_pct, 2)
                if snapshot.nasdaq_drawdown_pct is not None
                else None,
                "stage": stage_result.stage,  # データ不足期間はNone（DATA_INCOMPLETE）
                "stage_category": stage_result.category,
                "pre_alert": stage_result.pre_alert,
                "data_incomplete": stage_result.data_incomplete,
                "status": stage_result.status_label_jp,
                "simulated_new_allocation": suggested_total,
                "simulated_cumulative_deployed": cumulative_deployed,
            }
        )

    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="VIX暴落買いルールのバックテスト雛形")
    parser.add_argument("--start", required=True, help="開始日 YYYY-MM-DD（例: 2020-01-01）")
    parser.add_argument("--end", required=True, help="終了日 YYYY-MM-DD（例: 2020-12-31）")
    parser.add_argument("--config", default=None)
    parser.add_argument(
        "--output", default=None, help="結果CSVの出力先（省略時は vix_crash_monitor/reports/backtest_result.csv）"
    )
    args = parser.parse_args()

    config = load_config(args.config)
    print(f"バックテスト対象期間: {args.start} 〜 {args.end}")
    snapshots = load_historical_snapshots(config, args.start, args.end)
    print(f"取得日数: {len(snapshots)}")

    df = run_backtest(snapshots, config)

    output = Path(args.output) if args.output else config.reports_dir() / "backtest_result.csv"
    output.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output, index=False)

    print(f"結果を保存しました: {output}")
    active = df[df["stage"].notna() & (df["stage"] > 0)]
    print(active.to_string(index=False))
    incomplete_days = int(df["data_incomplete"].sum())
    if incomplete_days:
        print(f"\n(注: データ不足でDATA_INCOMPLETEとなった日数: {incomplete_days})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
