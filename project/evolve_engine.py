#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
双色球自进化预测引擎  evolve_engine.py
============================================================
架构：
  ┌─────────────────────────────────────────────────────────┐
  │  数据层   official_draws.json (1001期真实开奖)           │
  ├─────────────────────────────────────────────────────────┤
  │  特征层   13维评分组件（球级6 + 组合级7）                 │
  │    球级:  freq, gap(NegBin), pair, pagerank, markov, rqa │
  │    组合:  odd, zone, repeat, consec, persist, sum, span  │
  ├─────────────────────────────────────────────────────────┤
  │  进化层   CMA-ES — 协方差矩阵自适应进化策略               │
  │    目标:  最小化滚动150期前向验证偏差                     │
  │    适应度: −mean(红球命中数²) over predicted top-16        │
  ├─────────────────────────────────────────────────────────┤
  │  预测层   组合评分 + 候选生成 + 蓝球独立模型              │
  ├─────────────────────────────────────────────────────────┤
  │  状态层   evolution_state.json — 跨运行持久化            │
  └─────────────────────────────────────────────────────────┘

自进化循环（每次运行自动执行）：
  1. 加载真实开奖数据
  2. 评估上次预测的命中率（如有）
  3. CMA-ES权重进化（--evolve N 代，默认50代）
  4. 生成下一期预测
  5. 保存进化状态

用法：
  python3 evolve_engine.py                    # 使用当前状态快速预测
  python3 evolve_engine.py --evolve 100       # 进化100代后预测
  python3 evolve_engine.py --evolve 0         # 不进化，仅预测
  python3 evolve_engine.py --backtest 200     # 200期回测命中率报告
  python3 evolve_engine.py --reset            # 重置进化状态
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import Counter, defaultdict
from itertools import combinations
from typing import NamedTuple, Sequence

import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# 全局常量
# ══════════════════════════════════════════════════════════════════════════════
RED_N  = 33
BLUE_N = 16
REDS   = list(range(1, RED_N + 1))
BLUES  = list(range(1, BLUE_N + 1))

_DIR       = os.path.dirname(os.path.abspath(__file__))
DATA_PATH  = os.path.join(_DIR, "../outputs/official_draws.json")
STATE_PATH = os.path.join(_DIR, "../outputs/evolution_state.json")
PRED_PATH  = os.path.join(_DIR, "../outputs/evolved_prediction.json")

# 13维权重向量组件名（球级6 + 组合级7）
W_NAMES = [
    # 球级特征
    "freq",     # 长期出现频率（相对期望频率比）
    "gap",      # 负二项分布缺席危险率 h(k)
    "pair",     # 衰减加权号码对共现强度
    "pagerank", # 共现图PageRank中心性
    "markov",   # 一阶Markov条件转移概率
    "rqa",      # RQA递归定量分析得分
    # 组合级特征
    "odd_even", # 奇偶比例历史概率匹配
    "zone",     # 1/2/3区段分布历史概率
    "repeat",   # 与上期重叠数历史概率
    "consec",   # 连续号数量历史概率
    "persist",  # 近2期持续性（2期出现掩码重叠）
    "sum",      # 红球和值高斯匹配度
    "span",     # 号码跨度高斯匹配度
]
W_DIM = len(W_NAMES)  # 13

# 蓝球4维权重向量（freq, gap, markov, trend）
W_BLUE_NAMES = ["freq", "gap", "markov", "trend"]
W_BLUE_DIM   = 4
W_BLUE_INIT  = np.array([0.25, 0.25, 0.25, 0.25], dtype=np.float64)
BLUE_TOP_K   = 3   # 蓝球预测top-K命中优化（基线=3/16=18.75%）

# 初始权重（来自v6 run.py实证经验，作为CMA-ES起始点）
W_INIT = np.array([
    0.16,  # freq
    0.11,  # gap  (NegBin)
    0.11,  # pair
    0.06,  # pagerank
    0.06,  # markov
    0.03,  # rqa
    0.07,  # odd_even
    0.07,  # zone
    0.09,  # repeat
    0.04,  # consec
    0.05,  # persist
    0.09,  # sum
    0.04,  # span
], dtype=np.float64)

# 进化超参数
EVAL_WINDOW   = 150   # 前向验证窗口（期数）
CACHE_START   = 200   # 开始缓存的时间步（保证足够历史数据）
TOP_BALL_K    = 16    # 候选红球宇宙大小（预测top-K球中检查命中）
PAIR_WINDOW   = 100   # 对共现计算窗口
PAIR_DECAY    = 0.982 # 对共现时间衰减
PR_WINDOW     = 150   # PageRank计算窗口
PR_ALPHA      = 0.85  # PageRank阻尼因子
PR_ITER       = 60    # PageRank迭代次数
TRIPLET_WIN   = 80    # 三元组共现窗口（用于最终预测）
COMBO_SAMPLE  = 2000  # 最终预测候选组合采样数

# ══════════════════════════════════════════════════════════════════════════════
# 数据结构
# ══════════════════════════════════════════════════════════════════════════════
class Draw(NamedTuple):
    code: str
    reds: tuple[int, ...]
    blue: int


def load_draws() -> list[Draw]:
    """从 official_draws.json 加载并排序所有开奖记录。"""
    with open(DATA_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    draws: list[Draw] = []
    for d in raw:
        code = str(d.get("code") or d.get("issue") or "")
        # red 可能是 "01,03,..." 字符串或整数列表
        raw_red = d["red"]
        if isinstance(raw_red, str):
            reds = tuple(sorted(int(x) for x in raw_red.split(",")))
        else:
            reds = tuple(sorted(int(x) for x in raw_red))
        blue = int(d["blue"])
        draws.append(Draw(code=code, reds=reds, blue=blue))
    draws.sort(key=lambda d: d.code)
    return draws


# ══════════════════════════════════════════════════════════════════════════════
# 特征计算：球级特征（6维）
# ══════════════════════════════════════════════════════════════════════════════
def _negbin_hazard(k: int, gap_seq: list[int]) -> float:
    """
    负二项分布缺席危险率 h(k) = P(本期出现 | 已缺席k期)。
    k≤0: 返回基准概率 0.15（上期刚出现）
    样本<4: 退回 log1p 线性近似
    过散(var>mean): MOM拟合NegBin参数
    欠散/等散: 几何分布近似
    """
    if k <= 0:
        return 0.15
    if len(gap_seq) < 4:
        return min(math.log1p(k) / math.log1p(30), 1.0)
    mean_g = sum(gap_seq) / len(gap_seq)
    if mean_g <= 0:
        return 0.0
    var_g = sum((g - mean_g) ** 2 for g in gap_seq) / max(len(gap_seq) - 1, 1)
    if var_g <= mean_g:
        # 等散/欠散：几何分布 h(k) ≈ λ = 1/mean
        return min(1.0 / max(mean_g, 1.0), 1.0)
    # 过散：NegBin MOM 拟合
    p_hat = min(max(mean_g / var_g, 0.01), 0.99)
    r_hat = max(mean_g * p_hat / (1.0 - p_hat), 0.1)
    lp    = math.log(max(p_hat,       1e-10))
    l1mp  = math.log(max(1.0 - p_hat, 1e-10))
    lpmf  = r_hat * lp
    pmf_k = cdf = 0.0
    for j in range(min(k + 1, 200)):
        pj = math.exp(lpmf)
        if j < k:
            cdf += pj
        else:
            pmf_k = pj
        if j < 199:
            lpmf += l1mp + math.log(j + r_hat) - math.log(j + 1)
    return min(pmf_k / max(1.0 - cdf, 1e-10), 1.0)


def compute_ball_features(history: list[Draw]) -> np.ndarray:
    """
    计算所有33个红球的6维球级特征矩阵。
    返回: shape (RED_N, 6) — 行=球1..33，列=[freq,gap,pair,pr,markov,rqa]
    所有值已归一化到 [0,1]。
    """
    n = len(history)
    if n < 10:
        return np.full((RED_N, 6), 0.1)

    # ── 1. 频率 & NegBin缺席危险率 ──────────────────────────────────────────
    freq_cnt = np.zeros(RED_N + 1, dtype=np.float64)
    gap_curr = np.full(RED_N + 1, n, dtype=int)   # 当前缺席期数
    gap_seqs: list[list[int]] = [[] for _ in range(RED_N + 1)]
    last_age:  list[int | None] = [None] * (RED_N + 1)

    for age, draw in enumerate(reversed(history), start=1):
        for b in draw.reds:
            freq_cnt[b] += 1
            if last_age[b] is None:
                gap_curr[b] = age - 1      # 上次出现距现在的期数
            if last_age[b] is not None:
                gap_seqs[b].append(age - last_age[b] - 1)
            last_age[b] = age

    expected = n * 6.0 / RED_N
    freq_sc = freq_cnt / max(expected, 1.0)        # 相对频率（1.0 = 期望）
    freq_sc /= max(freq_sc[1:].max(), 1.0)

    gap_sc = np.array([_negbin_hazard(gap_curr[b], gap_seqs[b]) for b in range(RED_N + 1)])
    gap_max = gap_sc[1:].max()
    if gap_max > 0:
        gap_sc /= gap_max

    # ── 2. 衰减加权号码对共现 ──────────────────────────────────────────────
    win = min(PAIR_WINDOW, n)
    pair_sum = np.zeros(RED_N + 1, dtype=np.float64)
    for age, draw in enumerate(reversed(history[-win:]), start=1):
        w = PAIR_DECAY ** (age - 1)
        for a, b in combinations(draw.reds, 2):
            pair_sum[a] += w
            pair_sum[b] += w
    pair_max = pair_sum[1:].max() or 1.0
    pair_sc  = pair_sum / pair_max

    # ── 3. PageRank 共现图中心性 ───────────────────────────────────────────
    pw = min(PR_WINDOW, n)
    W_pr = np.zeros((RED_N, RED_N), dtype=np.float64)
    for age, draw in enumerate(reversed(history[-pw:]), start=1):
        w = PAIR_DECAY ** (age - 1)
        for a, b in combinations(draw.reds, 2):
            W_pr[a - 1, b - 1] += w
            W_pr[b - 1, a - 1] += w
    rs = W_pr.sum(axis=1, keepdims=True)
    rs[rs == 0] = 1.0
    P = W_pr / rs
    r = np.full(RED_N, 1.0 / RED_N)
    for _ in range(PR_ITER):
        r_new = PR_ALPHA * (P.T @ r) + (1.0 - PR_ALPHA) / RED_N
        if float(np.abs(r_new - r).sum()) < 1e-6:
            r = r_new
            break
        r = r_new
    pr_max = r.max() or 1.0
    pr_sc = np.concatenate([[0.0], r / pr_max])  # index 0 unused (balls are 1-based)

    # ── 4. Markov 一阶条件转移概率 ─────────────────────────────────────────
    markov_sc = np.zeros(RED_N + 1, dtype=np.float64)
    if n >= 2:
        trans: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
        for i in range(1, n):
            for src in history[i - 1].reds:
                for dst in history[i].reds:
                    trans[src][dst] += 1
        last_reds = set(history[-1].reds)
        for src in last_reds:
            tot = sum(trans[src].values()) + RED_N
            for dst in REDS:
                markov_sc[dst] += (trans[src].get(dst, 0) + 1) / tot
        mm = markov_sc[1:].max() or 1.0
        markov_sc /= mm
    else:
        markov_sc[:] = 0.5

    # ── 5. RQA 递归定量分析（滞后1~12期加权） ─────────────────────────────
    rqa_sc = np.zeros(RED_N + 1, dtype=np.float64)
    for lag in range(1, min(13, n)):
        w_lag = 0.9 ** (lag - 1)
        idx = n - lag - 1
        if idx >= 0:
            for b in history[idx].reds:
                rqa_sc[b] += w_lag
    rqa_max = rqa_sc[1:].max() or 1.0
    rqa_sc /= rqa_max

    # ── 组装特征矩阵 shape=(RED_N, 6) ──────────────────────────────────────
    mat = np.zeros((RED_N, 6), dtype=np.float64)
    for i, b in enumerate(REDS):
        mat[i] = [freq_sc[b], gap_sc[b], pair_sc[b], pr_sc[b], markov_sc[b], rqa_sc[b]]
    return mat


# ══════════════════════════════════════════════════════════════════════════════
# 特征计算：组合级上下文
# ══════════════════════════════════════════════════════════════════════════════
def build_combo_context(history: list[Draw]) -> dict:
    """
    统计历史分布参数，用于7维组合结构特征评分。
    返回: odd_probs, zone_probs, repeat_probs, consec_probs,
          sum_mean, sum_std, span_mean, span_std
    """
    n = len(history)
    if n < 20:
        return dict(
            odd_probs={3: 1 / 7},
            zone_probs={(2, 2, 2): 1 / 8},
            repeat_probs={2: 1 / 7},
            consec_probs={0: 0.5},
            sum_mean=100.5, sum_std=21.0,
            span_mean=26.0, span_std=5.0,
        )
    odd_c = Counter(); zone_c = Counter()
    rep_c = Counter(); con_c  = Counter()
    sums: list[float] = []; spans: list[float] = []

    for i, draw in enumerate(history):
        sr = sorted(draw.reds)
        odd_c[sum(1 for r in sr if r % 2)] += 1
        z = (sum(1 for r in sr if r <= 11),
             sum(1 for r in sr if 12 <= r <= 22),
             sum(1 for r in sr if r >= 23))
        zone_c[z] += 1
        if i > 0:
            prev_m = _mk(history[i - 1].reds)
            rep_c[bin(_mk(sr) & prev_m).count("1")] += 1
        con_c[sum(1 for j in range(5) if sr[j + 1] - sr[j] == 1)] += 1
        sums.append(float(sum(sr)))
        spans.append(float(sr[-1] - sr[0]))

    return dict(
        odd_probs={k: v / n for k, v in odd_c.items()},
        zone_probs={k: v / n for k, v in zone_c.items()},
        repeat_probs={k: v / n for k, v in rep_c.items()},
        consec_probs={k: v / n for k, v in con_c.items()},
        sum_mean=float(np.mean(sums)),
        sum_std=float(np.std(sums)),
        span_mean=float(np.mean(spans)),
        span_std=float(np.std(spans)),
    )


def _mk(reds: Sequence[int]) -> int:
    """将红球列表编码为位掩码。"""
    m = 0
    for r in reds:
        m |= (1 << r)
    return m


# ══════════════════════════════════════════════════════════════════════════════
# 评分函数
# ══════════════════════════════════════════════════════════════════════════════
def ball_scores_fast(feat_mat: np.ndarray, w: np.ndarray) -> np.ndarray:
    """
    快速球级评分（仅使用6维球级特征和前6个权重）。
    feat_mat: (RED_N, 6), w: (W_DIM,) → 返回 (RED_N,) 分数
    """
    return feat_mat @ w[:6]


def combo_score_full(
    reds: Sequence[int],
    ball_sc: np.ndarray,
    ctx: dict,
    w: np.ndarray,
    history: list[Draw],
) -> float:
    """
    完整13维组合评分（用于最终预测，而非进化内循环）。
    """
    sr = sorted(reds)
    idx = [r - 1 for r in sr]

    # 球级聚合
    bmax = ball_sc.max() or 1.0
    base = float(ball_sc[idx].mean()) / bmax

    # 奇偶
    n_odd  = sum(1 for r in sr if r % 2)
    odd_s  = ctx["odd_probs"].get(n_odd, 0.02)

    # 区段
    z = (sum(1 for r in sr if r <= 11),
         sum(1 for r in sr if 12 <= r <= 22),
         sum(1 for r in sr if r >= 23))
    zone_s = ctx["zone_probs"].get(z, 0.02)

    # 上期重叠
    if history:
        rh = bin(_mk(history[-1].reds) & _mk(sr)).count("1")
        rep_s = ctx["repeat_probs"].get(rh, 0.05)
    else:
        rep_s = 0.1

    # 连续号
    nc    = sum(1 for j in range(5) if sr[j + 1] - sr[j] == 1)
    con_s = ctx["consec_probs"].get(nc, 0.1)

    # 持续性（近2期掩码重叠）
    if len(history) >= 2:
        m2    = _mk(history[-1].reds) | _mk(history[-2].reds)
        per_s = min(bin(_mk(sr) & m2).count("1") / 4.0, 1.0)
    else:
        per_s = 0.0

    # 和值高斯匹配
    s_val = sum(sr)
    z_s   = abs(s_val - ctx["sum_mean"]) / max(ctx["sum_std"], 1.0)
    sum_s = math.exp(-0.5 * z_s ** 2)

    # 跨度高斯匹配
    sp    = sr[-1] - sr[0]
    z_sp  = abs(sp - ctx["span_mean"]) / max(ctx["span_std"], 1.0)
    spn_s = math.exp(-0.5 * z_sp ** 2)

    struct = np.array([odd_s, zone_s, rep_s, con_s, per_s, sum_s, spn_s])
    return float(w[0] * base + (w[6:13] * struct).sum())


# ══════════════════════════════════════════════════════════════════════════════
# 奖级评估
# ══════════════════════════════════════════════════════════════════════════════
PRIZE_TABLE = {
    (6, 1): (1, "一等奖"),
    (6, 0): (2, "二等奖"),
    (5, 1): (3, "三等奖"),
    (5, 0): (4, "四等奖"),
    (4, 1): (4, "四等奖"),
    (4, 0): (5, "五等奖"),
    (3, 1): (5, "五等奖"),
    (0, 1): (6, "六等奖"),
    (1, 1): (6, "六等奖"),
    (2, 1): (6, "六等奖"),
}


def evaluate_ticket(pred_reds: Sequence[int], pred_blue: int, actual: Draw) -> tuple[int, int, int]:
    """返回 (红球命中数, 蓝球命中0/1, 奖级0=未中)"""
    rh = len(set(pred_reds) & set(actual.reds))
    bh = 1 if pred_blue == actual.blue else 0
    key = (rh, bh)
    if key in PRIZE_TABLE:
        return rh, bh, PRIZE_TABLE[key][0]
    return rh, bh, 0


# ══════════════════════════════════════════════════════════════════════════════
# 蓝球预测（独立简单模型）
# ══════════════════════════════════════════════════════════════════════════════
def compute_blue_features(history: list[Draw]) -> np.ndarray:
    """
    计算16个蓝球的4维特征矩阵。
    返回: shape (BLUE_N, 4) — [freq, gap(NegBin), markov, trend]
    所有列归一化到 [0,1]。
    """
    n = len(history)
    if n < 4:
        return np.full((BLUE_N, 4), 0.25)

    # ── 1. 频率（近100期）──────────────────────────────────────────────────
    win = min(100, n)
    freq_cnt = Counter(d.blue for d in history[-win:])
    freq_sc = np.array([freq_cnt.get(b, 0) / win for b in BLUES])
    freq_max = freq_sc.max() or 1.0
    freq_sc /= freq_max

    # ── 2. NegBin缺席危险率 ─────────────────────────────────────────────────
    gap_curr: dict[int, int] = {b: n for b in BLUES}
    gap_seqs: dict[int, list[int]] = {b: [] for b in BLUES}
    last_seen: dict[int, int | None] = {b: None for b in BLUES}
    for age, draw in enumerate(reversed(history), start=1):
        b = draw.blue
        if last_seen[b] is None:
            gap_curr[b] = age - 1
        if last_seen[b] is not None:
            gap_seqs[b].append(age - last_seen[b] - 1)
        last_seen[b] = age

    gap_sc = np.array([_negbin_hazard(gap_curr[b], gap_seqs[b]) for b in BLUES])
    gap_max = gap_sc.max() or 1.0
    gap_sc /= gap_max

    # ── 3. Markov一阶转移概率 P(b | last_blue) ──────────────────────────────
    if n >= 2:
        trans = np.zeros((BLUE_N + 1, BLUE_N + 1), dtype=np.float64)
        for i in range(1, n):
            prev = history[i - 1].blue
            curr = history[i].blue
            trans[prev][curr] += 1
        last_blue = history[-1].blue
        row = trans[last_blue]
        row_sum = row.sum()
        if row_sum > 0:
            markov_sc = np.array([row[b] / row_sum for b in BLUES])
        else:
            markov_sc = np.ones(BLUE_N) / BLUE_N
    else:
        markov_sc = np.ones(BLUE_N) / BLUE_N
    markov_max = markov_sc.max() or 1.0
    markov_sc /= markov_max

    # ── 4. 指数加权趋势（衰减=0.95，近期权重更高）────────────────────────────
    decay = 0.95
    trend_acc = np.zeros(BLUE_N + 1, dtype=np.float64)
    for age, draw in enumerate(reversed(history[-50:]), start=1):
        trend_acc[draw.blue] += decay ** (age - 1)
    trend_sc = np.array([trend_acc[b] for b in BLUES])
    trend_max = trend_sc.max() or 1.0
    trend_sc /= trend_max

    return np.column_stack([freq_sc, gap_sc, markov_sc, trend_sc])  # (BLUE_N, 4)


def compute_blue_scores(history: list[Draw], w_blue: np.ndarray) -> dict[int, float]:
    """使用进化后蓝球权重计算各球评分。返回 {blue: score}（未归一化）。"""
    feat = compute_blue_features(history)   # (BLUE_N, 4)
    scores = feat @ w_blue                  # (BLUE_N,)
    return {b: float(scores[i]) for i, b in enumerate(BLUES)}


# ══════════════════════════════════════════════════════════════════════════════
# CMA-ES（协方差矩阵自适应进化策略）
# ══════════════════════════════════════════════════════════════════════════════
class CMAES:
    """
    完整 CMA-ES 实现（Hansen & Ostermeier, 2001）。
    最小化目标函数 f(x)，x ∈ ℝⁿ，权重约束 x ≥ ε。
    """

    def __init__(self, x0: np.ndarray, sigma0: float = 0.05):
        self.n     = len(x0)
        self.mean  = x0.copy().astype(np.float64)
        self.sigma = float(sigma0)
        lam        = 4 + int(3 * math.log(self.n))
        self.lam   = lam
        mu         = lam // 2

        ls = np.log(mu + 0.5) - np.log(np.arange(1, mu + 1, dtype=np.float64))
        self.w     = ls / ls.sum()
        mueff      = 1.0 / float((self.w ** 2).sum())

        n = self.n
        self.cc    = (4 + mueff / n) / (n + 4 + 2 * mueff / n)
        self.cs    = (mueff + 2) / (n + mueff + 5)
        self.c1    = 2 / ((n + 1.3) ** 2 + mueff)
        self.cmu   = min(1 - self.c1, 2 * (mueff - 2 + 1 / mueff) / ((n + 2) ** 2 + mueff))
        self.damps = 1 + 2 * max(0.0, math.sqrt((mueff - 1) / (n + 1)) - 1) + self.cs
        self.chiN  = n ** 0.5 * (1 - 1 / (4 * n) + 1 / (21 * n ** 2))

        self.pc         = np.zeros(n)
        self.ps         = np.zeros(n)
        self.C          = np.eye(n)
        self.B          = np.eye(n)
        self.D          = np.ones(n)
        self.invsqrtC   = np.eye(n)
        self.eigeneval   = 0
        self.gen         = 0
        self._ys: list[np.ndarray] = []

    def ask(self) -> list[np.ndarray]:
        """采样 λ 个候选解。"""
        self._decompose()
        xs: list[np.ndarray] = []
        self._ys = []
        for _ in range(self.lam):
            z = np.random.randn(self.n)
            y = self.sigma * (self.B @ (self.D * z))
            x = np.maximum(self.mean + y, 1e-4)   # 权重非负约束
            xs.append(x)
            self._ys.append(y)
        return xs

    def tell(self, xs: list[np.ndarray], fs: list[float]) -> None:
        """依据适应度排序更新分布参数。"""
        mu = len(self.w)
        order = np.argsort(fs)
        xs_s  = [xs[i] for i in order[:mu]]
        ys_s  = [self._ys[i] for i in order[:mu]]

        old_mean = self.mean.copy()
        self.mean = sum(self.w[i] * xs_s[i] for i in range(mu))
        self.mean = np.maximum(self.mean, 1e-4)

        y_mean = (self.mean - old_mean) / max(self.sigma, 1e-10)
        mu_e   = 1.0 / math.sqrt(max(float((self.w ** 2).sum()), 1e-10))

        # 步长控制（Cumulative Step-size Adaptation）
        self.ps = ((1 - self.cs) * self.ps
                   + math.sqrt(self.cs * (2 - self.cs)) * mu_e
                   * (self.invsqrtC @ y_mean))
        ps_norm = float(np.linalg.norm(self.ps))
        chiN_expected = self.chiN * math.sqrt(1 - (1 - self.cs) ** (2 * (self.gen + 1)))
        hsig = ps_norm / chiN_expected < 1.4 + 2 / (self.n + 1)

        # 秩一路径（Rank-One Update）
        self.pc = ((1 - self.cc) * self.pc
                   + float(hsig) * math.sqrt(self.cc * (2 - self.cc)) * mu_e * y_mean)

        # 协方差矩阵更新
        C_new = (1 - self.c1 - self.cmu) * self.C
        C_new += self.c1 * (
            np.outer(self.pc, self.pc)
            + (1 - float(hsig)) * self.cc * (2 - self.cc) * self.C
        )
        C_new += self.cmu * sum(
            self.w[i] * np.outer(ys_s[i] / self.sigma, ys_s[i] / self.sigma)
            for i in range(mu)
        )
        self.C = C_new

        # 步长更新
        delta = (self.cs / self.damps) * (ps_norm / self.chiN - 1)
        self.sigma *= math.exp(min(delta, 1.0))
        self.sigma  = float(np.clip(self.sigma, 1e-8, 5.0))
        self.gen   += 1

    @property
    def best_weights(self) -> np.ndarray:
        """当前均值向量（归一化权重）。"""
        w = np.maximum(self.mean, 0.0)
        s = w.sum()
        return w / max(s, 1e-10)

    def _decompose(self) -> None:
        if self.gen - self.eigeneval > self.lam / (10 * self.n * (self.c1 + self.cmu) + 1):
            self.eigeneval = self.gen
            self.C = np.triu(self.C) + np.triu(self.C, 1).T   # 对称化
            d2, B = np.linalg.eigh(self.C)
            self.D = np.sqrt(np.maximum(d2, 1e-20))
            self.B = B
            self.invsqrtC = B @ np.diag(1.0 / self.D) @ B.T

    # ── 状态序列化 ────────────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return dict(
            mean=self.mean.tolist(), sigma=self.sigma,
            pc=self.pc.tolist(), ps=self.ps.tolist(),
            C=self.C.tolist(), B=self.B.tolist(),
            D=self.D.tolist(), invsqrtC=self.invsqrtC.tolist(),
            eigeneval=self.eigeneval, gen=self.gen,
        )

    @classmethod
    def from_dict(cls, d: dict) -> "CMAES":
        x0    = np.array(d["mean"])
        obj   = cls(x0, sigma0=float(d["sigma"]))
        obj.pc         = np.array(d["pc"])
        obj.ps         = np.array(d["ps"])
        obj.C          = np.array(d["C"])
        obj.B          = np.array(d["B"])
        obj.D          = np.array(d["D"])
        obj.invsqrtC   = np.array(d["invsqrtC"])
        obj.eigeneval   = int(d["eigeneval"])
        obj.gen         = int(d["gen"])
        return obj


# ══════════════════════════════════════════════════════════════════════════════
# 前向验证适应度函数（带特征缓存）
# ══════════════════════════════════════════════════════════════════════════════
class WalkForwardEvaluator:
    """
    滚动前向验证器。
    一次性预计算所有时间步的特征矩阵，之后每次适应度评估仅做向量乘法。
    """

    def __init__(self, draws: list[Draw], eval_window: int = EVAL_WINDOW):
        n        = len(draws)
        self.n   = n
        self.start = max(CACHE_START, n - eval_window)
        self.end   = n
        self.actual_masks = []
        self.feat_cache: list[np.ndarray] = []

        print(f"  预计算特征缓存（时间步 {self.start}..{self.end - 1}）...")
        t0 = time.time()
        for i in range(self.start, self.end):
            self.feat_cache.append(compute_ball_features(draws[:i]))
            self.actual_masks.append(set(draws[i].reds))
            if (i - self.start + 1) % 30 == 0:
                pct = (i - self.start + 1) / (self.end - self.start) * 100
                print(f"    {pct:.0f}%  ({i - self.start + 1}/{self.end - self.start}) "
                      f"  {time.time()-t0:.1f}s", end="\r")
        print(f"\n  缓存完成：{len(self.feat_cache)} 步  用时 {time.time()-t0:.1f}s")

    def __len__(self) -> int:
        return len(self.feat_cache)

    def fitness(self, w: np.ndarray) -> float:
        """
        适应度 = −mean(hits²)，其中 hits = 预测TOP-K中命中实际红球数。
        最小化此值 ↔ 最大化红球预测精度。
        """
        total = 0.0
        for feat, actual in zip(self.feat_cache, self.actual_masks):
            scores = feat @ w[:6]                          # (RED_N,)
            top_k  = set(np.argsort(-scores)[:TOP_BALL_K] + 1)  # +1: 0-indexed → 1-indexed
            hits   = len(top_k & actual)
            total += hits * hits
        return -total / max(len(self.feat_cache), 1)

    def hit_rate_report(self, w: np.ndarray) -> dict:
        """命中率分布统计（用于回测报告）。"""
        cnt = Counter()
        for feat, actual in zip(self.feat_cache, self.actual_masks):
            scores = feat @ w[:6]
            top6  = set(np.argsort(-scores)[:6] + 1)
            top16 = set(np.argsort(-scores)[:16] + 1)
            hits6  = len(top6  & actual)
            hits16 = len(top16 & actual)
            cnt[f"top6_hit{hits6}"] += 1
            cnt[f"top16_hit{hits16}"] += 1
        N = len(self.feat_cache)
        return {k: v for k, v in cnt.items()}, N


class WalkForwardBlueEvaluator:
    """
    蓝球滚动前向验证器。
    一次性预计算所有时间步的蓝球特征矩阵，适应度 = −top-{BLUE_TOP_K}命中率。
    """

    def __init__(self, draws: list[Draw], eval_window: int = EVAL_WINDOW):
        n = len(draws)
        self.start = max(CACHE_START, n - eval_window)
        self.end   = n
        self.actual_blues: list[int] = []
        self.feat_cache: list[np.ndarray] = []

        print(f"  预计算蓝球特征缓存（时间步 {self.start}..{self.end - 1}）...")
        t0 = time.time()
        for i in range(self.start, self.end):
            self.feat_cache.append(compute_blue_features(draws[:i]))
            self.actual_blues.append(draws[i].blue)
            if (i - self.start + 1) % 30 == 0:
                pct = (i - self.start + 1) / (self.end - self.start) * 100
                print(f"    {pct:.0f}%  ({i - self.start + 1}/{self.end - self.start}) "
                      f"  {time.time()-t0:.1f}s", end="\r")
        print(f"\n  蓝球缓存完成：{len(self.feat_cache)} 步  用时 {time.time()-t0:.1f}s")

    def __len__(self) -> int:
        return len(self.feat_cache)

    def fitness(self, w: np.ndarray) -> float:
        """适应度 = −top-{BLUE_TOP_K}命中率（最小化↔最大化命中率）。"""
        hits = 0
        for feat, actual in zip(self.feat_cache, self.actual_blues):
            scores = feat @ w                                # (BLUE_N,)
            top_k  = set(np.argsort(-scores)[:BLUE_TOP_K] + 1)   # 1-indexed
            if actual in top_k:
                hits += 1
        return -hits / max(len(self.feat_cache), 1)

    def hit_rate_report(self, w: np.ndarray) -> tuple[dict[str, float], int]:
        """统计top-1/3/5命中率，对比随机基线 1/16, 3/16, 5/16。"""
        hits: dict[int, int] = {1: 0, 3: 0, 5: 0}
        for feat, actual in zip(self.feat_cache, self.actual_blues):
            scores = feat @ w
            ranked = list(np.argsort(-scores) + 1)          # 1-indexed, best first
            for k in (1, 3, 5):
                if actual in ranked[:k]:
                    hits[k] += 1
        N = len(self.feat_cache)
        return {f"top{k}_hit_rate": hits[k] / N for k in (1, 3, 5)}, N


# ══════════════════════════════════════════════════════════════════════════════
# 进化主循环
# ══════════════════════════════════════════════════════════════════════════════
def run_evolution(draws: list[Draw], n_gen: int, evaluator: WalkForwardEvaluator,
                  cma: CMAES, verbose: bool = True) -> tuple[np.ndarray, float]:
    """
    运行 n_gen 代 CMA-ES 进化。
    返回: (最优权重向量, 最优适应度)
    """
    best_w  = cma.best_weights.copy()
    best_f  = evaluator.fitness(best_w)

    print(f"\n{'═'*60}")
    print(f"  CMA-ES 权重进化  ({n_gen} 代, λ={cma.lam})")
    print(f"  起始适应度: {-best_f:.4f}  [预测top-{TOP_BALL_K}中均命中数²均值]")
    print(f"{'─'*60}")
    t0 = time.time()

    for gen in range(1, n_gen + 1):
        xs   = cma.ask()
        fs   = [evaluator.fitness(x) for x in xs]
        cma.tell(xs, fs)

        gen_best = min(fs)
        if gen_best < best_f:
            best_f = gen_best
            best_w = xs[int(np.argmin(fs))].copy()
            best_w = np.maximum(best_w, 0.0)
            s = best_w.sum()
            best_w /= max(s, 1e-10)

        if verbose and (gen % 10 == 0 or gen == n_gen):
            elapsed = time.time() - t0
            print(f"  gen {gen:4d}/{n_gen}  fitness={-gen_best:.4f}"
                  f"  best={-best_f:.4f}  σ={cma.sigma:.4f}  {elapsed:.1f}s")

    print(f"{'─'*60}")
    print(f"  进化完成！最优适应度: {-best_f:.4f}")
    return best_w, best_f


# ══════════════════════════════════════════════════════════════════════════════
# 最终预测生成
# ══════════════════════════════════════════════════════════════════════════════
def generate_prediction(draws: list[Draw], w: np.ndarray,
                        w_blue: np.ndarray | None = None) -> dict:
    """
    用进化后权重生成完整预测：
      - 使用全13维评分（球级+组合级）
      - 蒙特卡洛采样 COMBO_SAMPLE 个候选组合评分
      - 蓝球使用进化权重（若有）或等权基线
      - 给出核心6红 + 核心蓝球 + TOP8蓝球
    """
    feat_mat = compute_ball_features(draws)
    ctx      = build_combo_context(draws)
    ball_sc  = ball_scores_fast(feat_mat, w)

    # 候选宇宙：球级评分TOP-16
    top16_idx = np.argsort(-ball_sc)[:16]
    top16_balls = [int(i + 1) for i in top16_idx]

    # 所有 C(16,6) = 8008 种组合评分（若太多则采样）
    all_combos = list(combinations(top16_balls, 6))
    if len(all_combos) > COMBO_SAMPLE:
        rng = np.random.default_rng(42)
        idx_s = rng.choice(len(all_combos), COMBO_SAMPLE, replace=False)
        all_combos = [all_combos[i] for i in idx_s]

    # 评分
    scored = []
    for combo in all_combos:
        sc = combo_score_full(combo, ball_sc, ctx, w, draws)
        scored.append((sc, combo))
    scored.sort(reverse=True)

    best_combo = scored[0][1]
    best_score = scored[0][0]

    # 蓝球（使用进化权重或等权基线）
    if w_blue is None:
        w_blue = W_BLUE_INIT.copy()
    blue_scores = compute_blue_scores(draws, w_blue)
    top_blues   = sorted(blue_scores, key=lambda b: -blue_scores[b])
    core_blue   = top_blues[0]

    # 红球排名（用于显示）
    ranked_reds = [int(i + 1) for i in np.argsort(-ball_sc)]

    return dict(
        core_reds        = sorted(best_combo),
        top_reds         = ranked_reds[:16],
        core_blue        = core_blue,
        top_blues        = top_blues[:8],
        best_score       = round(float(best_score), 6),
        weights          = {W_NAMES[i]: round(float(w[i]), 4) for i in range(W_DIM)},
        weights_blue     = {W_BLUE_NAMES[i]: round(float(w_blue[i]), 4)
                            for i in range(W_BLUE_DIM)},
    )


# ══════════════════════════════════════════════════════════════════════════════
# 状态持久化
# ══════════════════════════════════════════════════════════════════════════════
def load_state() -> dict | None:
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    return None


def save_state(cma: CMAES, best_w: np.ndarray, best_f: float,
               pred: dict, last_code: str,
               cma_blue: CMAES | None = None,
               best_w_blue: np.ndarray | None = None,
               best_f_blue: float | None = None) -> None:
    state = dict(
        cma=cma.to_dict(),
        best_weights=best_w.tolist(),
        best_fitness=float(best_f),
        prediction=pred,
        last_code=last_code,
        timestamp=time.strftime("%Y-%m-%d %H:%M:%S"),
    )
    if cma_blue is not None:
        state["cma_blue"]        = cma_blue.to_dict()
        state["best_weights_blue"] = best_w_blue.tolist() if best_w_blue is not None else None
        state["best_fitness_blue"] = float(best_f_blue) if best_f_blue is not None else None
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"\n  状态已保存 → {STATE_PATH}")


def save_prediction(pred: dict, next_code: str) -> None:
    out = dict(next_issue=next_code, **pred)
    with open(PRED_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"  预测已保存 → {PRED_PATH}")


# ══════════════════════════════════════════════════════════════════════════════
# 打印工具
# ══════════════════════════════════════════════════════════════════════════════
def _fmt_reds(reds: Sequence[int]) -> str:
    return "  ".join(f"{r:02d}" for r in sorted(reds))


def print_prediction(pred: dict, next_code: str, draws: list[Draw]) -> None:
    print(f"\n{'═'*60}")
    print(f"  预测期号: 第 {next_code} 期")
    print(f"{'─'*60}")
    print(f"  核心红球（最优组合）:  {_fmt_reds(pred['core_reds'])}")
    print(f"  红球候选TOP16:         {_fmt_reds(pred['top_reds'])}")
    print(f"  核心蓝球:              {pred['core_blue']:02d}  ★")
    print(f"  蓝球TOP8:              {' '.join(f'{b:02d}' for b in pred['top_blues'])}")
    print(f"  组合评分:              {pred['best_score']:.4f}")
    print(f"{'─'*60}")
    print("  红球进化权重向量:")
    for name, val in pred["weights"].items():
        bar = "█" * int(val * 200)
        print(f"    {name:10s}  {val:.4f}  {bar}")
    if pred.get("weights_blue"):
        print("  蓝球进化权重向量:")
        for name, val in pred["weights_blue"].items():
            bar = "█" * int(val * 200)
            print(f"    {name:10s}  {val:.4f}  {bar}")
    print(f"{'─'*60}")
    print(f"  数据截至:  第 {draws[-1].code} 期  {len(draws)} 期历史")
    print(f"{'═'*60}\n")


def print_backtest_report(draws: list[Draw], evaluator: WalkForwardEvaluator,
                          w: np.ndarray) -> None:
    """输出按命中数分布的详细回测报告。"""
    cnt_dict, N = evaluator.hit_rate_report(w)

    print(f"\n{'═'*60}")
    print(f"  回测报告  ({N} 期前向验证，top-16球宇宙)")
    print(f"{'─'*60}")
    print(f"  {'命中数':^8}  {'次数':>6}  {'比例':>7}  {'累积≥':>7}")
    cum = 0
    for k in range(7, -1, -1):
        cnt = cnt_dict.get(f"top16_hit{k}", 0)
        cum += cnt
        bar  = "▓" * int(cnt / N * 50)
        print(f"  top16中{k}  {cnt:6d}  {cnt/N*100:6.1f}%  {cum/N*100:6.1f}%  {bar}")
    print(f"{'─'*60}")
    print(f"  top16中≥3: {sum(cnt_dict.get(f'top16_hit{k}',0) for k in range(3,7))/N*100:.1f}%")
    print(f"  top16中≥4: {sum(cnt_dict.get(f'top16_hit{k}',0) for k in range(4,7))/N*100:.1f}%")
    print(f"  top6中≥3:  {sum(cnt_dict.get(f'top6_hit{k}', 0) for k in range(3,7))/N*100:.1f}%")
    print(f"  top6中≥4:  {sum(cnt_dict.get(f'top6_hit{k}', 0) for k in range(4,7))/N*100:.1f}%")
    print(f"{'═'*60}\n")


# ══════════════════════════════════════════════════════════════════════════════
# 评估上次预测（如数据中有新开奖）
# ══════════════════════════════════════════════════════════════════════════════
def evaluate_last_prediction(state: dict, draws: list[Draw]) -> None:
    pred  = state.get("prediction")
    lcode = state.get("last_code")
    if not pred or not lcode:
        return
    # 找到 last_code 之后的第一期实际开奖
    for d in draws:
        if d.code > lcode:
            rh, bh, prize = evaluate_ticket(pred["core_reds"], pred["top_blues"][0], d)
            print(f"\n{'▶'*3}  上次预测复盘（第 {d.code} 期）")
            print(f"  预测核心红球: {_fmt_reds(pred['core_reds'])}")
            print(f"  实际开奖红球: {_fmt_reds(d.reds)}  +蓝{d.blue:02d}")
            print(f"  红球命中: {rh}/6   蓝球: {'✓' if bh else '✗'}   奖级: {prize if prize else '未中'}")
            break


# ══════════════════════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════════════════════
def _next_code(last_code: str) -> str:
    """简单估算下一期期号（实际以官方为准）。"""
    try:
        return str(int(last_code) + 1)
    except ValueError:
        return "unknown"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="双色球自进化预测引擎",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--evolve",   type=int, default=50,
                        metavar="N",  help="运行 N 代 CMA-ES 进化（默认50，0=仅预测）")
    parser.add_argument("--backtest", action="store_true",  help="输出回测命中率报告")
    parser.add_argument("--reset",    action="store_true",  help="清除进化状态，从初始权重重新开始")
    parser.add_argument("--seed",     type=int, default=42, help="随机种子（默认42）")
    parser.add_argument("--sigma",    type=float, default=None,
                        help="强制重置CMA-ES步长σ（如--sigma 0.15，用于逃离局部极值）")
    args = parser.parse_args()

    np.random.seed(args.seed)
    print(f"\n{'═'*60}")
    print("  双色球自进化预测引擎  (CMA-ES + NegBin + PageRank)")
    print(f"{'═'*60}")

    # ── 加载数据 ──────────────────────────────────────────────────────────────
    print(f"\n  加载开奖数据: {DATA_PATH}")
    draws = load_draws()
    print(f"  共 {len(draws)} 期  ({draws[0].code} → {draws[-1].code})")
    next_code = _next_code(draws[-1].code)

    # ── 重置状态 ──────────────────────────────────────────────────────────────
    if args.reset and os.path.exists(STATE_PATH):
        os.remove(STATE_PATH)
        print("  ✓ 进化状态已清除")

    # ── 加载或初始化 CMA-ES 状态 ──────────────────────────────────────────────
    state = load_state()
    if state and not args.reset:
        print(f"  ✓ 加载已有进化状态  第 {state.get('gen', state['cma'].get('gen',0))} 代"
              f"  上次运行: {state.get('timestamp','')}")
        cma    = CMAES.from_dict(state["cma"])
        best_w = np.array(state["best_weights"])
        best_f = float(state["best_fitness"])
        # 蓝球CMA状态
        if state.get("cma_blue"):
            cma_blue    = CMAES.from_dict(state["cma_blue"])
            best_w_blue = np.array(state["best_weights_blue"])
            best_f_blue = float(state["best_fitness_blue"])
            print(f"  ✓ 蓝球CMA-ES已加载  蓝球最优适应度: {-best_f_blue:.4f}")
        else:
            cma_blue    = CMAES(W_BLUE_INIT.copy(), sigma0=0.1)
            best_w_blue = W_BLUE_INIT.copy()
            best_f_blue = float("inf")
        # 评估上次预测
        evaluate_last_prediction(state, draws)
    else:
        print("  ✓ 初始化 CMA-ES（起始点 = v6 经验权重）")
        cma    = CMAES(W_INIT.copy(), sigma0=0.05)
        best_w = W_INIT.copy() / W_INIT.sum()
        best_f = float("inf")
        print("  ✓ 初始化蓝球 CMA-ES（等权起始点）")
        cma_blue    = CMAES(W_BLUE_INIT.copy(), sigma0=0.1)
        best_w_blue = W_BLUE_INIT.copy()
        best_f_blue = float("inf")

    # ── 强制重置σ（逃离局部极值）─────────────────────────────────────────────
    if args.sigma is not None:
        old_sigma = cma.sigma
        cma.sigma = args.sigma
        # 同时扩展协方差矩阵（防止搜索方向退化）
        cma.C        = np.eye(cma.n)
        cma.B        = np.eye(cma.n)
        cma.D        = np.ones(cma.n)
        cma.invsqrtC = np.eye(cma.n)
        cma.pc       = np.zeros(cma.n)
        cma.ps       = np.zeros(cma.n)
        print(f"  ✓ σ重置: {old_sigma:.5f} → {args.sigma}  (协方差矩阵已重置，搜索范围扩展)")

    # ── 构建特征缓存（进化或回测都需要）────────────────────────────────────────
    if args.evolve > 0 or args.backtest:
        evaluator = WalkForwardEvaluator(draws)
        if best_f == float("inf"):
            best_f = evaluator.fitness(best_w)
            print(f"  初始适应度: {-best_f:.4f}")
        # 蓝球评估器
        print()
        blue_evaluator: WalkForwardBlueEvaluator | None = WalkForwardBlueEvaluator(draws)
        if best_f_blue == float("inf"):
            best_f_blue = blue_evaluator.fitness(best_w_blue)
            print(f"  蓝球初始适应度: {-best_f_blue:.4f}  (基线top-{BLUE_TOP_K}={BLUE_TOP_K/BLUE_N*100:.1f}%)")
    else:
        evaluator      = None  # type: ignore
        blue_evaluator = None  # type: ignore

    # ── 红球 CMA-ES 进化（保留全局最优）──────────────────────────────────────
    global_best_w = best_w.copy()
    global_best_f = best_f

    if args.evolve > 0 and evaluator is not None:
        run_best_w, run_best_f = run_evolution(draws, args.evolve, evaluator, cma)
        if run_best_f < global_best_f:
            global_best_w = run_best_w
            global_best_f = run_best_f
            print(f"  ✓ 红球全局最优更新: {-global_best_f:.4f}")
        else:
            print(f"  ~ 红球未超过历史最优 {-global_best_f:.4f}，保留最优权重")

    # ── 蓝球 CMA-ES 进化（保留全局最优）──────────────────────────────────────
    global_best_w_blue = best_w_blue.copy()
    global_best_f_blue = best_f_blue

    if args.evolve > 0 and blue_evaluator is not None:
        print(f"\n{'═'*60}")
        print(f"  蓝球 CMA-ES 进化  ({args.evolve} 代, λ={cma_blue.lam})")
        print(f"  起始适应度: {-global_best_f_blue:.4f}  [top-{BLUE_TOP_K}命中率]")
        print(f"  随机基线: {BLUE_TOP_K}/{BLUE_N}={BLUE_TOP_K/BLUE_N*100:.2f}%")
        print(f"{'─'*60}")
        t0 = time.time()
        for gen in range(1, args.evolve + 1):
            xs = cma_blue.ask()
            fs = [blue_evaluator.fitness(x) for x in xs]
            cma_blue.tell(xs, fs)
            gen_best = min(fs)
            if gen_best < global_best_f_blue:
                global_best_f_blue = gen_best
                global_best_w_blue = xs[int(np.argmin(fs))].copy()
                global_best_w_blue = np.maximum(global_best_w_blue, 0.0)
                s = global_best_w_blue.sum()
                global_best_w_blue /= max(s, 1e-10)
            if gen % 10 == 0 or gen == args.evolve:
                elapsed = time.time() - t0
                print(f"  gen {gen:4d}/{args.evolve}  fitness={-gen_best:.4f}"
                      f"  best={-global_best_f_blue:.4f}  σ={cma_blue.sigma:.4f}  {elapsed:.1f}s")
        print(f"{'─'*60}")
        print(f"  蓝球进化完成！最优top-{BLUE_TOP_K}命中率: {-global_best_f_blue:.4f}"
              f"  (基线: {BLUE_TOP_K/BLUE_N*100:.2f}%)")

    # ── 蓝球命中率回测报告 ─────────────────────────────────────────────────────
    if (args.evolve > 0 or args.backtest) and blue_evaluator is not None:
        blue_rates, N_blue = blue_evaluator.hit_rate_report(global_best_w_blue)
        baseline = {1: 1/BLUE_N, 3: 3/BLUE_N, 5: 5/BLUE_N}
        print(f"\n{'═'*60}")
        print(f"  蓝球命中率报告  ({N_blue} 期前向验证)")
        print(f"{'─'*60}")
        print(f"  {'范围':^8}  {'命中率':>8}  {'基线':>8}  {'提升':>8}")
        for k in (1, 3, 5):
            hr    = blue_rates[f"top{k}_hit_rate"]
            base  = baseline[k]
            delta = hr - base
            sign  = "+" if delta >= 0 else ""
            print(f"  top-{k} 命中  {hr*100:7.2f}%  {base*100:7.2f}%  {sign}{delta*100:.2f}%")
        print(f"{'═'*60}")

    # ── 红球回测报告（用全局最优权重）─────────────────────────────────────────
    if args.backtest and evaluator is not None:
        print_backtest_report(draws, evaluator, global_best_w)

    # ── 生成预测 ──────────────────────────────────────────────────────────────
    print(f"\n  生成第 {next_code} 期预测（全13维组合评分 + 蓝球进化权重）...")
    pred = generate_prediction(draws, global_best_w, global_best_w_blue)
    print_prediction(pred, next_code, draws)

    # ── 保存状态 ──────────────────────────────────────────────────────────────
    save_state(cma, global_best_w, global_best_f, pred, draws[-1].code,
               cma_blue=cma_blue, best_w_blue=global_best_w_blue,
               best_f_blue=global_best_f_blue)
    save_prediction(pred, next_code)


if __name__ == "__main__":
    main()
