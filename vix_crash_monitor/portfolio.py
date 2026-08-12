#!/usr/bin/env python3
"""銘柄別の購入記録CLI。

【重要】このコマンドはシステムが「買った」と自動判定するものではありません。
実際に証券会社で購入した後、人間が金額を入力して記録するためのものです。

使い方:
  python vix_crash_monitor/portfolio.py record --ticker NVDA --amount 100000 --stage 1
  python vix_crash_monitor/portfolio.py status
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vix_crash_monitor.config import load_config
from vix_crash_monitor.portfolio_state import load_state, record_purchase, save_state


def cmd_record(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = load_state(config)
    try:
        record_purchase(
            state, amount=args.amount, stage=args.stage, ticker=args.ticker, note=args.note or ""
        )
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    save_state(state, config)
    stage_str = f"Stage {args.stage}" if args.stage is not None else "(Stage未指定)"
    print(f"記録しました: {args.ticker}  {args.amount:,}円  {stage_str}")
    print(f"{args.ticker} 累積投入額: {state.ticker_deployed_amount(args.ticker):,}円")
    print(f"投入済み合計: {state.deployed_amount:,}円 / 残り: {state.remaining_amount:,}円")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    state = load_state(config)
    sector_map = config.ticker_sector_map()
    totals: dict = {}
    for p in state.purchases:
        ticker = p.get("ticker")
        if ticker:
            totals[ticker] = totals.get(ticker, 0) + p["amount"]

    print(f"投入済み合計: {state.deployed_amount:,}円 / 残り: {state.remaining_amount:,}円")
    print("銘柄別累積投入額:")
    for ticker, amount in sorted(totals.items(), key=lambda kv: -kv[1]):
        sector = sector_map.get(ticker, "OTHER")
        print(f"  {ticker} ({sector}): {amount:,}円")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="銘柄別購入記録CLI（自動発注なし・人間による手入力専用）")
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)

    p_record = sub.add_parser("record", help="銘柄別の実際の購入を記録する")
    p_record.add_argument("--ticker", required=True)
    p_record.add_argument("--amount", type=int, required=True)
    p_record.add_argument("--stage", type=int, choices=[1, 2, 3, 4], default=None)
    p_record.add_argument("--note", default="")
    p_record.set_defaults(func=cmd_record)

    p_status = sub.add_parser("status", help="銘柄別の投入状況を表示する")
    p_status.set_defaults(func=cmd_status)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
