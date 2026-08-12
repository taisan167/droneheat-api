"""市場Stage判定ロジック（純粋関数）。

- 外部通信・状態保存を一切行わない、テスト容易・バックテスト再利用可能な純粋ロジック層。
- 入力: MarketSnapshot（市場データ） + 投入済みStage一覧
- 出力: StageResult（現在のStageと投入候補計算に必要な情報）

Stageは単純にVIXだけで決めない。VIX・VIX前日比・NASDAQ100/SOX/S&P500の
下落率を総合して判定する（config.yamlのしきい値を参照）。
"""
from __future__ import annotations

from vix_crash_monitor.config import Config
from vix_crash_monitor.models import MarketSnapshot, ReversalSignals, StageResult

STATUS_LABELS = {
    0: ("WAITING", "まだ暴落買い条件ではありません"),
    1: ("STAGE1", "第1回分割買い候補"),
    2: ("STAGE2", "強い調整局面。第2回分割買い候補"),
    3: ("STAGE3", "市場パニック級。長期投資候補を精査"),
    4: ("REVERSAL_CONFIRMED", "反転確認候補（Stage3までの暴落度合いとは別軸の指標）"),
}

# Stage番号は連番だが危険度が単調に増すスケールではない。
# 0-3は「暴落の深刻さ」を表すBUY_STAGE、4は暴落そのものではなく
# 「複数の反転シグナルが確認できた」ことを表すRECOVERY_SIGNALであり、
# Stage3より危険ということではない（見直しレビュー項目4）。
STAGE_CATEGORY = {0: "BUY_STAGE", 1: "BUY_STAGE", 2: "BUY_STAGE", 3: "BUY_STAGE", 4: "RECOVERY_SIGNAL"}


def is_pre_alert(snapshot: MarketSnapshot, config: Config) -> bool:
    """VIXが25未満でも前営業日比+20%以上急上昇していればPRE-ALERT。投入候補額は常に0円。"""
    if snapshot.vix >= config.pre_alert_vix_ceiling:
        return False
    return snapshot.vix_change_pct >= config.pre_alert_vix_change_threshold_pct


def _meets_stage(snapshot: MarketSnapshot, config: Config, stage: int) -> bool:
    th = config.stage_threshold(stage)
    nasdaq_dd = abs(snapshot.nasdaq_drawdown_pct)

    if stage in (1, 2):
        return snapshot.vix >= th.vix_min and nasdaq_dd >= th.nasdaq_drawdown_min_pct
    if stage == 3:
        # Stage3のみ「または」条件（VIX>=40 または NASDAQ100が-20%以上）
        return snapshot.vix >= th.vix_min or nasdaq_dd >= th.nasdaq_drawdown_min_pct
    raise ValueError(f"unsupported stage for threshold check: {stage}")


def compute_reversal_signals(snapshot: MarketSnapshot) -> ReversalSignals:
    """Stage4（反転確認）用の個別シグナルを算出する。

    単純に「VIXが下がっただけ」では反転確認としない。複数シグナルの
    組み合わせで判断する（config.stage4.min_reversal_signals が必要数）。
    """
    vix_down_20 = False
    if snapshot.vix_drop_from_peak_pct is not None:
        vix_down_20 = snapshot.vix_drop_from_peak_pct <= -20.0

    above_5dma = False
    if snapshot.nasdaq_5dma is not None:
        above_5dma = snapshot.nasdaq_price > snapshot.nasdaq_5dma

    above_prev_high = False
    if snapshot.nasdaq_prev_day_high is not None:
        above_prev_high = snapshot.nasdaq_price > snapshot.nasdaq_prev_day_high

    rsi_recovering = False
    if snapshot.nasdaq_rsi is not None:
        # 極端な売られすぎ(<30)からの回復途上(30-55のレンジ)を「回復」とみなす
        rsi_recovering = 30.0 <= snapshot.nasdaq_rsi <= 55.0

    sox_stabilizing = False
    if snapshot.sox_drawdown_pct is not None:
        # SOXの下落率がNASDAQ100より極端に悪化していない = 下げ止まりの簡易判定
        sox_stabilizing = snapshot.sox_drawdown_pct >= snapshot.nasdaq_drawdown_pct - 3.0

    breadth_bonus = bool(snapshot.breadth_improving)

    return ReversalSignals(
        vix_down_20pct_from_peak=vix_down_20,
        nasdaq_above_5dma=above_5dma,
        nasdaq_above_prev_high=above_prev_high,
        rsi_recovering_from_oversold=rsi_recovering,
        sox_stabilizing=sox_stabilizing,
        breadth_improving_bonus=breadth_bonus,
    )


def determine_stage(
    snapshot: MarketSnapshot,
    config: Config,
    completed_stages: list[int] | None = None,
) -> StageResult:
    """現在の市場Stageを判定する。

    completed_stagesはStage4の反転確認判定を行うために使う
    （Stage3まで到達済みの場合のみStage4の反転確認評価を行う）。

    VIXまたはNASDAQ100の52週高値等、判定に必須のデータが欠けている場合は
    stage=None・status_code="DATA_INCOMPLETE"を返す。憶測でStageを判定
    しない（見直しレビュー項目7）。
    """
    completed_stages = completed_stages or []

    if not snapshot.has_required_data:
        return StageResult(
            stage=None,
            category="NONE",
            status_code="DATA_INCOMPLETE",
            status_label_jp="市場判定保留（主要データ取得不可のためStage判定を行いません）",
            pre_alert=False,
            data_incomplete=True,
            reasons=["NASDAQ100の52週高値データ、または市場データが取得できませんでした"],
        )

    reasons: list[str] = []

    pre_alert = is_pre_alert(snapshot, config)
    if pre_alert:
        reasons.append(
            f"VIX前日比+{snapshot.vix_change_pct:.1f}%（急落予備警報しきい値+{config.pre_alert_vix_change_threshold_pct:.0f}%以上）"
        )

    # Stage判定は高い方から評価（複数条件を満たしうるため）
    if _meets_stage(snapshot, config, 3):
        code, label = STATUS_LABELS[3]
        reasons.append(
            f"VIX={snapshot.vix:.1f}（>=40 または）NASDAQ100高値比{snapshot.nasdaq_drawdown_pct:.1f}%（<=-20%）"
        )

        # Stage3に到達済み、かつ過去にStage3が完了記録されている場合のみ
        # Stage4（反転確認）の評価対象とする。単純なVIX低下だけでは昇格しない。
        if 3 in completed_stages:
            reversal = compute_reversal_signals(snapshot)
            th4 = config.stage_threshold(4)
            if reversal.total_signal_score >= th4.min_reversal_signals:
                code4, label4 = STATUS_LABELS[4]
                reasons.append(
                    f"反転シグナル{reversal.total_signal_score}件確認（必要{th4.min_reversal_signals}件以上）"
                )
                return StageResult(
                    stage=4,
                    category=STAGE_CATEGORY[4],
                    status_code=code4,
                    status_label_jp=label4,
                    pre_alert=False,
                    reversal_signals=reversal,
                    reasons=reasons,
                )
            reasons.append(
                f"反転シグナル{reversal.total_signal_score}件（必要{th4.min_reversal_signals}件未満のため反転未確認）"
            )
            return StageResult(
                stage=3,
                category=STAGE_CATEGORY[3],
                status_code=code,
                status_label_jp=label,
                pre_alert=False,
                reversal_signals=reversal,
                reasons=reasons,
            )

        return StageResult(
            stage=3, category=STAGE_CATEGORY[3], status_code=code, status_label_jp=label, pre_alert=False, reasons=reasons
        )

    if _meets_stage(snapshot, config, 2):
        code, label = STATUS_LABELS[2]
        reasons.append(
            f"VIX={snapshot.vix:.1f}(>=30) かつ NASDAQ100高値比{snapshot.nasdaq_drawdown_pct:.1f}%(<=-15%)"
        )
        return StageResult(
            stage=2, category=STAGE_CATEGORY[2], status_code=code, status_label_jp=label, pre_alert=False, reasons=reasons
        )

    if _meets_stage(snapshot, config, 1):
        code, label = STATUS_LABELS[1]
        reasons.append(
            f"VIX={snapshot.vix:.1f}(>=25) かつ NASDAQ100高値比{snapshot.nasdaq_drawdown_pct:.1f}%(<=-10%)"
        )
        return StageResult(
            stage=1, category=STAGE_CATEGORY[1], status_code=code, status_label_jp=label, pre_alert=False, reasons=reasons
        )

    code, label = STATUS_LABELS[0]
    if pre_alert:
        # PRE-ALERTはStage0の一種だが表示文言のみ差し替える。投入候補は常に0円。
        return StageResult(
            stage=0,
            category=STAGE_CATEGORY[0],
            status_code="PRE_ALERT",
            status_label_jp="市場ストレス急上昇。まだ購入せず監視強化",
            pre_alert=True,
            reasons=reasons,
        )
    return StageResult(
        stage=0, category=STAGE_CATEGORY[0], status_code=code, status_label_jp=label, pre_alert=False, reasons=reasons
    )


def compute_suggested_allocation(
    stage_result: StageResult,
    config: Config,
    completed_stages: list[int],
) -> tuple[int, list[int]]:
    """今回投入候補の総額（＝未実行Stage配分の合計"上限"）と、対象となる
    Stage番号一覧を返す。

    - DATA_INCOMPLETE（stage=None）・PRE-ALERT・Stage0では常に0円。
    - すでに完了済みのStageは再度候補にしない（Stage 1完了済みなら
      再度Stage1条件に該当しても追加投入候補は0円＝資金の二重計上防止）。
    - Stage2/3に直接到達した場合は、未完了の下位Stage分もまとめて候補にする
      （例: 一気にStage3まで急落した場合、Stage1+2+3の合計を提示）。

    【重要】この戻り値は「一括で今すぐ全額投入すべき」という指示ではなく、
    未実行Stage分を合算した"配分上限の目安"に過ぎない。実際に何回に分けて
    投入するかは人間が判断すること（呼び出し側の表示文言でも明示する）。
    """
    if stage_result.data_incomplete or stage_result.stage is None:
        return 0, []
    if stage_result.pre_alert or stage_result.stage == 0:
        return 0, []

    budget = config.crash_buying_budget
    target_stages = [s for s in range(1, stage_result.stage + 1) if s not in completed_stages]

    if stage_result.stage == 4:
        target_stages = [4] if 4 not in completed_stages else []

    total = 0
    for s in target_stages:
        ratio = config.stage_threshold(s).allocation_ratio
        total += round(budget * ratio)

    return total, target_stages
