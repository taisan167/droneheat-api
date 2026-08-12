#!/usr/bin/env python3
"""VIX暴落買い監視システム CLI。

【重要】このシステムは投資判断"支援"専用です。
証券会社への注文・自動売買・API発注は一切行いません。
実際に購入したかどうかは、人間が record-buy コマンドで明示的に記録した
場合のみ「投入済み」として扱われます。

使い方:
  python vix_crash_monitor/main.py run
      市場データを取得し、Stage判定・銘柄スコアリング・配分計算を行い、
      レポート（テキスト / reports/latest.json / reports/chatgpt_prompt.md）
      を出力する。

  python vix_crash_monitor/main.py record-buy --stage 1 --amount 200000
      Stage単位でまとめて「実際に買った」ことを記録する（銘柄別内訳なし）。
      銘柄別に記録したい場合は portfolio.py の record コマンドを使う。

  python vix_crash_monitor/main.py status
      現在の投入済み資金状況を表示する。
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vix_crash_monitor import data_fetch
from vix_crash_monitor.allocation import allocate_budget
from vix_crash_monitor.config import load_config
from vix_crash_monitor.history import append_history
from vix_crash_monitor.portfolio_state import load_state, record_purchase, save_state
from vix_crash_monitor.report import write_reports
from vix_crash_monitor.scoring import score_watchlist
from vix_crash_monitor.stage_logic import compute_suggested_allocation, determine_stage


def cmd_run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = load_state(config)

    try:
        snapshot = data_fetch.fetch_market_snapshot(config)
    except data_fetch.DataFetchError as e:
        print(f"市場データ取得エラー: {e}", file=sys.stderr)
        return 1

    stage_result = determine_stage(snapshot, config, completed_stages=state.completed_stages)
    suggested_total, target_stages = compute_suggested_allocation(stage_result, config, state.completed_stages)

    stock_data = data_fetch.fetch_watchlist_data(config.all_tickers())
    scores = score_watchlist(stock_data, snapshot, config)

    candidates, sector_warnings = allocate_budget(scores, suggested_total, state, config)

    text_report, json_report, _ = write_reports(
        config, snapshot, stage_result, state, suggested_total, candidates, sector_warnings
    )
    append_history(config, snapshot, stage_result, suggested_total, state)

    print(text_report)
    if target_stages:
        print(f"\n(対象Stage: {target_stages} / 未完了分のみ集計)")
    return 0


def cmd_record_buy(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = load_state(config)
    try:
        record_purchase(state, amount=args.amount, stage=args.stage, ticker=None, note=args.note or "")
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    save_state(state, config)
    print(f"記録しました: Stage {args.stage} / {args.amount:,}円")
    print(f"投入済み合計: {state.deployed_amount:,}円 / 残り: {state.remaining_amount:,}円")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = load_state(config)
    print(f"総予算: {state.total_budget:,}円")
    print(f"投入済み: {state.deployed_amount:,}円")
    print(f"残り: {state.remaining_amount:,}円")
    print(f"完了Stage: {sorted(state.completed_stages)}")
    print(f"購入記録件数: {len(state.purchases)}")
    for p in state.purchases:
        ticker = p.get("ticker") or "-"
        stage = p.get("stage")
        stage_str = f"Stage{stage}" if stage is not None else "-"
        print(f"  {p['timestamp']}  {stage_str}  {ticker}  {p['amount']:,}円  {p.get('note','')}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VIX暴落買い監視システム（投資判断支援・自動売買なし）")
    parser.add_argument("--config", default=None, help="config.yamlのパス（省略時は同梱デフォルト）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="監視を実行しレポートを出力する")
    p_run.set_defaults(func=cmd_run)

    p_record = sub.add_parser("record-buy", help="Stage単位で実際の購入を記録する（人間が明示的に実行）")
    p_record.add_argument("--stage", type=int, required=True, choices=[1, 2, 3, 4])
    p_record.add_argument("--amount", type=int, required=True, help="実際に投入した金額（円）")
    p_record.add_argument("--note", default="", help="メモ（任意）")
    p_record.set_defaults(func=cmd_record_buy)

    p_status = sub.add_parser("status", help="投入済み資金の状況を表示する")
    p_status.set_defaults(func=cmd_status)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
