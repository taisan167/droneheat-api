"""VIX暴落買い監視システム ダッシュボード（Streamlit）。

【重要】表示専用の判断支援ダッシュボードです。ここから発注は行えません。

起動方法:
  streamlit run vix_crash_monitor/dashboard.py

reports/latest.json と data/state.json を読み込んで表示するだけなので、
事前に `python vix_crash_monitor/main.py run` を実行してレポートを
生成しておく必要があります。
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from vix_crash_monitor.config import load_config

STAGE_COLORS = {
    0: "#6b7280",  # Neutral (gray)
    1: "#eab308",  # 注意 (yellow)
    2: "#f97316",  # 警戒 (orange)
    3: "#dc2626",  # 強い警戒 (red)
    4: "#2563eb",  # 反転確認 (blue)
}
STAGE_TEXT = {
    0: "Stage 0 ／ Neutral（待機）",
    1: "Stage 1 ／ 注意（第1回買い候補）",
    2: "Stage 2 ／ 警戒（第2回買い候補）",
    3: "Stage 3 ／ 強い警戒（暴落買い候補）",
    4: "Stage 4 ／ 反転確認",
}


def load_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    st.set_page_config(page_title="VIX暴落買い監視ダッシュボード", layout="wide")
    st.title("VIX暴落買い監視ダッシュボード")
    st.caption(
        "本ダッシュボードは投資判断支援のための表示専用画面です。自動発注機能はありません。"
        "最終的な購入判断は必ずご自身で行ってください。"
    )

    config = load_config()
    latest_path = config.reports_dir() / "latest.json"
    report = load_json(latest_path)

    if report is None:
        st.warning(
            "レポートが見つかりません。先に `python vix_crash_monitor/main.py run` を実行してください。"
        )
        return

    stage = report.get("market_stage", 0)
    pre_alert = report.get("pre_alert", False)
    color = STAGE_COLORS.get(stage, "#6b7280")

    if pre_alert:
        st.markdown(
            f"<div style='padding:16px;border-radius:8px;background-color:#f59e0b22;"
            f"border:2px solid #f59e0b;'><h3 style='margin:0;color:#f59e0b;'>PRE-ALERT</h3>"
            f"<p style='margin:4px 0 0 0;'>{report.get('status_label','')}</p></div>",
            unsafe_allow_html=True,
        )
    else:
        label = STAGE_TEXT.get(stage, f"Stage {stage}")
        st.markdown(
            f"<div style='padding:16px;border-radius:8px;background-color:{color}22;"
            f"border:2px solid {color};'><h3 style='margin:0;color:{color};'>{label}</h3>"
            f"<p style='margin:4px 0 0 0;'>{report.get('status_label','')}</p></div>",
            unsafe_allow_html=True,
        )

    st.markdown("&nbsp;", unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    col1.metric("VIX", f"{report['vix']:.1f}", f"{report['vix_change_pct']:+.1f}%")
    col2.metric("NASDAQ100 高値比", f"{report['nasdaq_drawdown']:.1f}%")
    sox_dd = report.get("sox_drawdown")
    col3.metric("SOX 高値比", f"{sox_dd:.1f}%" if sox_dd is not None else "N/A")

    st.subheader("資金管理")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("総予算", f"{report['total_budget']:,}円")
    c2.metric("投入済み", f"{report['deployed_amount']:,}円")
    c3.metric("今回投入候補", f"{report['suggested_new_allocation']:,}円")
    c4.metric("残り", f"{report['remaining_budget_after_candidate_allocation']:,}円")

    st.subheader("銘柄ランキング")
    candidates = report.get("watchlist_all") or report.get("candidates") or []
    if candidates:
        rows = [
            {
                "ティッカー": c["ticker"],
                "セクター": c["sector"],
                "スコア": c["score"],
                "ランク": c["rank"],
                "投入候補額": f"{c['suggested_amount']:,}円",
                "警告": "; ".join(c.get("warning_details", [])) or "-",
            }
            for c in candidates
        ]
        st.dataframe(rows, use_container_width=True)
    else:
        st.info("銘柄データがありません。")

    sector_warnings = report.get("sector_warnings") or []
    if sector_warnings:
        st.subheader("警告")
        for w in sector_warnings:
            st.warning(w)

    st.caption(report.get("disclaimer", ""))


if __name__ == "__main__":
    main()
