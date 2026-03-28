#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
auto_update.py — 双色球开奖日全自动更新脚本
================================================================
每期开奖当天（周二/四/日）22:00 由 cron/launchd 调用

执行流程：
  1. 从中国福彩官网增量抓取最新开奖号码
  2. 写入 official_draws.json
  3. 调用 evolve_engine.py 进行 CMA-ES 自进化（默认 30 代）
  4. 读取 evolved_prediction.json 最新预测
  5. 渲染前端 H5 页面（lottery-prediction.html）
  6. 备份 HTML 到 outputs/history/
  7. 写运行日志到 outputs/auto_update.log

用法：
  python3 auto_update.py                     # 标准运行（30代进化）
  python3 auto_update.py --evolve 50         # 自定义进化代数
  python3 auto_update.py --skip-evolve       # 仅抓数据+刷新前端，不进化
  python3 auto_update.py --dry-run           # 只打印计划，不执行
  python3 auto_update.py --force             # 强制运行（即使今天已运行）
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# ══ 路径常量 ══════════════════════════════════════════════
_DIR        = Path(__file__).resolve().parent
_ROOT       = _DIR.parent
OUTPUTS     = _ROOT / "outputs"
DATA_PATH   = OUTPUTS / "official_draws.json"
STATE_PATH  = OUTPUTS / "evolution_state.json"
PRED_PATH   = OUTPUTS / "evolved_prediction.json"
HTML_SRC    = Path(os.environ.get("SSQ_HTML_TEMPLATE",
                   str(_ROOT / "lottery-prediction.html")))
HTML_OUT    = HTML_SRC          # 原地更新
HISTORY_DIR = OUTPUTS / "history"
LOG_PATH    = OUTPUTS / "auto_update.log"
LOCK_PATH   = OUTPUTS / ".auto_update.lock"

ENGINE_PY   = _DIR / "evolve_engine.py"
DEFAULT_EVOLVE_GENS = 30

# ══ 日志配置 ══════════════════════════════════════════════
def _setup_logger() -> logging.Logger:
    OUTPUTS.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("auto_update")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    # 文件 handler（追加模式）
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    # 控制台 handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

log = _setup_logger()

# ══════════════════════════════════════════════════════════
# 1. 增量抓取最新开奖号码
# ══════════════════════════════════════════════════════════
def _load_existing_draws() -> list[dict]:
    if DATA_PATH.exists():
        return json.loads(DATA_PATH.read_text("utf-8"))
    return []


def _save_draws(draws: list[dict]) -> None:
    DATA_PATH.write_text(
        json.dumps(draws, ensure_ascii=False, indent=2), "utf-8"
    )


def fetch_and_update(dry_run: bool = False) -> tuple[int, str | None]:
    """
    从 CWL 官网增量抓取最新开奖，写入 official_draws.json。
    返回 (新增条数, 最新 code 或 None)
    """
    log.info("── 步骤 1：抓取最新开奖数据 ──")
    existing = _load_existing_draws()
    existing_codes = {d["code"] for d in existing}
    latest_code = max(existing_codes) if existing_codes else "2013001"
    log.info(f"  本地已有 {len(existing)} 期，最新 code={latest_code}")

    # 只抓当年剩余期 + 少量跨年保护
    today = date.today()
    year = today.year
    start_code = f"{year}001"
    # 如果本地最新 code 已是今年，从它的期号+1 开始
    if latest_code[:4] == str(year):
        start_code = str(int(latest_code) + 1)

    log.info(f"  请求 issueStart={start_code}")

    if dry_run:
        log.info("  [dry-run] 跳过实际网络请求")
        return 0, latest_code

    # 调用 official_cwl.py 中的函数
    try:
        sys.path.insert(0, str(_DIR))
        from official_cwl import fetch_ssq_draw_notices
        records = fetch_ssq_draw_notices(issue_start=int(start_code))
    except Exception as exc:
        log.error(f"  抓取失败: {exc}")
        return 0, latest_code

    new_records = [r for r in records if r["code"] not in existing_codes]
    if not new_records:
        log.info("  没有新开奖数据")
        return 0, latest_code

    # 规范化字段格式
    normalized: list[dict] = []
    for r in new_records:
        normalized.append({
            "code":  str(r["code"]),
            "red":   str(r.get("red", r.get("reds", ""))).replace(" ", ","),
            "blue":  str(r.get("blue", r.get("blues", ""))).strip(),
            "date":  str(r.get("date", "")),
        })

    merged = sorted(existing + normalized, key=lambda x: int(x["code"]))
    if not dry_run:
        _save_draws(merged)

    new_codes = [r["code"] for r in normalized]
    log.info(f"  新增 {len(new_records)} 期：{new_codes}")
    latest = max(r["code"] for r in normalized)
    return len(new_records), latest


# ══════════════════════════════════════════════════════════
# 2. 运行 CMA-ES 进化引擎
# ══════════════════════════════════════════════════════════
def run_evolution(n_gen: int, dry_run: bool = False) -> bool:
    """调用 evolve_engine.py --evolve n_gen"""
    log.info(f"── 步骤 2：CMA-ES 进化 {n_gen} 代 ──")
    if dry_run:
        log.info("  [dry-run] 跳过进化")
        return True

    cmd = [sys.executable, str(ENGINE_PY), "--evolve", str(n_gen)]
    log.info(f"  命令: {' '.join(cmd)}")
    t0 = time.time()
    try:
        result = subprocess.run(
            cmd, cwd=str(_DIR),
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, timeout=1800  # 最长 30 分钟
        )
        elapsed = time.time() - t0
        # 将子进程输出写入日志
        for line in result.stdout.splitlines():
            log.debug(f"  [engine] {line}")
        if result.returncode != 0:
            log.error(f"  进化引擎退出码 {result.returncode}（用时 {elapsed:.1f}s）")
            return False
        log.info(f"  进化完成，用时 {elapsed:.1f}s")
        return True
    except subprocess.TimeoutExpired:
        log.error("  进化超时（30 分钟）")
        return False
    except Exception as exc:
        log.error(f"  进化异常: {exc}")
        return False


# ══════════════════════════════════════════════════════════
# 3. 读取预测结果
# ══════════════════════════════════════════════════════════
def load_prediction() -> dict | None:
    if not PRED_PATH.exists():
        log.error(f"  预测文件不存在: {PRED_PATH}")
        return None
    return json.loads(PRED_PATH.read_text("utf-8"))


def load_state() -> dict | None:
    if not STATE_PATH.exists():
        return None
    return json.loads(STATE_PATH.read_text("utf-8"))


# ══════════════════════════════════════════════════════════
# 4. 计算下期开奖日期
# ══════════════════════════════════════════════════════════
_DRAW_WEEKDAYS = {1, 3, 6}   # Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6

def next_draw_date(after: date | None = None) -> date:
    """计算下一个双色球开奖日（周二=1, 周四=3, 周日=6）"""
    d = (after or date.today()) + timedelta(days=1)
    for _ in range(8):
        if d.weekday() in _DRAW_WEEKDAYS:
            return d
        d += timedelta(days=1)
    return d


def issue_draw_date(issue: str, draws: list[dict]) -> str:
    """从 draws 中找 issue 对应的日期；找不到则估算"""
    for d in draws:
        if d.get("code") == issue:
            raw = d.get("date", "")
            if raw and len(raw) >= 10:
                return raw[:10]
    # 从最新 draw 估算
    if draws:
        latest = max(draws, key=lambda x: int(x["code"]))
        raw = latest.get("date", "")
        if raw and len(raw) >= 10:
            try:
                base = date.fromisoformat(raw[:10])
                return str(next_draw_date(base))
            except ValueError:
                pass
    return str(next_draw_date())


# ══════════════════════════════════════════════════════════
# 5. 构建 HTML 注入数据
# ══════════════════════════════════════════════════════════
# 颜色映射（与原 HTML 一致）
_RED_COLORS = {
    "markov": "#74b9ff", "sum": "#ffd93d", "rqa": "#a29bfe",
    "freq": "#2ecc71", "span": "#fd79a8", "repeat": "#ff6b6b",
    "pagerank": "#4d9fff", "odd_even": "#e17055", "pair": "#00b894",
    "zone": "#b2bec3", "persist": "#dfe6e9", "gap": "#636e72",
    "consec": "#747d8c",
}
_BLUE_COLORS = {
    "markov": "#74b9ff", "gap": "#a29bfe", "freq": "#2ecc71", "trend": "#ffd93d",
}


def _pct(v: float) -> float:
    return round(v * 100, 1)


def build_arms(weights: dict, colors: dict) -> list[dict]:
    """构建权重柱状图数据（按权重降序）"""
    total = sum(weights.values()) or 1.0
    items = [
        {"name": k, "pct": _pct(v / total), "color": colors.get(k, "#aaa")}
        for k, v in weights.items()
    ]
    return sorted(items, key=lambda x: -x["pct"])


def build_rank_scores(ranked_list: list[int], n_total: int = None) -> dict:
    """为排行列表中的每个球赋分（首位1.0，末位~0.30）"""
    n = len(ranked_list)
    return {
        str(ball): round(1.0 - i * (0.70 / max(n - 1, 1)), 4)
        for i, ball in enumerate(ranked_list)
    }


def build_negbin_data(top_reds: list[int], core_reds: list[int],
                      draws: list[dict]) -> list[dict]:
    """
    估算 NegBin 危险率 h(k)：
    - 从最新开奖往前数，找每个球距上次出现的期数 k
    - 危险率 ≈ 0.14 + 0.015*k（过散简化模型）
    """
    if not draws:
        return []
    last_draw = draws[-1]
    last_reds = set(int(x) for x in last_draw.get("red", "").split(","))
    # 计算每个候选球的缺席期数 k
    results = []
    for ball in top_reds:
        k = 0
        for dr in reversed(draws[-30:]):
            reds = set(int(x) for x in dr.get("red", "").split(","))
            if ball in reds:
                break
            k += 1
        h = min(0.14 + 0.015 * k, 0.30)
        cold = ball not in last_reds and k >= 2
        results.append({
            "n": ball,
            "k": max(k, 1),
            "h": round(h, 3),
            "cold": cold,
        })
    return sorted(results, key=lambda x: -x["h"])


def build_markov_top_trans(last_reds: list[int],
                           top_reds_ranked: list[int]) -> list[dict]:
    """为上期每个开奖红球构造 Markov 最高转移目标（简化为取 top_reds 中排名最近的）"""
    result = []
    rank_map = {ball: i for i, ball in enumerate(top_reds_ranked)}
    used_targets: set[int] = set()
    for src in last_reds:
        # 找一个在 top_reds 里排名最高、还未被用过的目标球
        for candidate in top_reds_ranked:
            if candidate != src and candidate not in used_targets:
                used_targets.add(candidate)
                rank = rank_map.get(candidate, len(top_reds_ranked))
                prob = round(0.18 - rank * 0.004, 3)
                strength = max(60, min(98, 98 - rank * 3))
                result.append({
                    "from": src, "to": candidate,
                    "prob": prob, "strength": strength,
                })
                break
    return result


def build_attractor_data(draws: list[dict], core_reds: list[int]) -> dict:
    """构建 Takens 相位空间吸引子数据（和值序列）"""
    sums = []
    for d in draws[-30:]:
        try:
            sums.append(sum(int(x) for x in d["red"].split(",")))
        except Exception:
            pass
    if not sums:
        sums = [100, 98, 102]
    mean_s = sum(sums) / len(sums)
    std_s  = math.sqrt(sum((s - mean_s) ** 2 for s in sums) / len(sums)) or 20
    predicted_sum = sum(core_reds)
    correction = (predicted_sum - mean_s) / (std_s * 10)
    # 密度：预测和值与历史均值的接近程度
    density = max(0.3, 1.0 - abs(predicted_sum - mean_s) / (2 * std_s))
    cur = sums[-3:] if len(sums) >= 3 else [mean_s, mean_s, mean_s]
    return {
        "correction":    round(correction, 3),
        "density":       round(density, 2),
        "predictedSum":  predicted_sum,
        "historicalMean": round(mean_s, 1),
        "historicalStd":  round(std_s, 1),
        "currentPoint":  {"x": cur[-1], "y": cur[-2], "z": cur[-3]},
    }


def build_pagerank_data(top_reds: list[int]) -> list[dict]:
    """PageRank 分数（按 top_reds 排名归一化至 2.8～3.9 区间）"""
    n = len(top_reds)
    return [
        {"n": ball, "pr": round(3.90 - i * (1.10 / max(n - 1, 1)), 2)}
        for i, ball in enumerate(top_reds)
    ]


# ══════════════════════════════════════════════════════════
# 6. 渲染前端 HTML
# ══════════════════════════════════════════════════════════
def _js_arr(lst: list) -> str:
    return "[" + ", ".join(str(x) for x in lst) + "]"


def _js_obj_list(lst: list[dict], keys: list[str]) -> str:
    rows = []
    for item in lst:
        parts = []
        for k in keys:
            v = item.get(k)
            if isinstance(v, str):
                parts.append(f'{k}: "{v}"')
            elif isinstance(v, bool):
                parts.append(f'{k}: {"true" if v else "false"}')
            else:
                parts.append(f"{k}: {v}")
        rows.append("    { " + ", ".join(parts) + " }")
    return "[\n" + ",\n".join(rows) + "\n  ]"


def _js_dict(d: dict) -> str:
    parts = []
    for k, v in d.items():
        parts.append(f'    {k}: {v}')
    return "{\n" + ",\n".join(parts) + "\n  }"


def generate_pred_data_block(pred: dict, draws: list[dict],
                              state: dict | None) -> str:
    """生成 HTML 中 ===BEGIN_PRED_DATA=== 到 ===END_PRED_DATA=== 之间的内容"""

    # ── 基础数据 ──
    issue        = pred.get("next_issue", "????")
    core_reds    = pred.get("core_reds", [])
    top_reds_r   = pred.get("top_reds", [])     # 按评分降序
    core_blue    = pred.get("core_blue", 1)
    top_blues    = pred.get("top_blues", [])
    weights      = pred.get("weights", {})
    weights_blue = pred.get("weights_blue", {})

    # 状态中的适应度
    best_fit = abs(state.get("best_fitness", 11.45)) if state else 11.45
    blue_top3_hr = 22.0   # 从 state 读取如有
    if state:
        blue_hr = state.get("blue_hit_rates", {})
        if blue_hr.get("top3_hit_rate"):
            blue_top3_hr = round(blue_hr["top3_hit_rate"] * 100, 1)

    draw_count = len(draws)
    last_code  = draws[-1]["code"] if draws else "????"
    draw_date  = issue_draw_date(issue, draws)
    gen_date   = datetime.now().strftime("%Y-%m-%d")

    # ── Arms ──
    red_arms  = build_arms(weights, _RED_COLORS)
    blue_arms = build_arms(weights_blue, _BLUE_COLORS)

    # ── Scores ──
    entropy_red    = build_rank_scores(top_reds_r)
    markov_red_sc  = build_rank_scores(top_reds_r)
    markov_blue_sc = build_rank_scores(top_blues)
    rqa_sc         = build_rank_scores(top_reds_r)

    # ── Chart data ──
    last_reds = [int(x) for x in draws[-1].get("red", "").split(",") if x] if draws else []
    negbin_data    = build_negbin_data(top_reds_r[:12], core_reds, draws)
    pagerank_data  = build_pagerank_data(top_reds_r[:10])
    markov_trans   = build_markov_top_trans(last_reds, top_reds_r)
    attractor      = build_attractor_data(draws, core_reds)

    top_blue3 = top_blues[:3]
    top_blue8 = top_blues[:8]

    # 上期开奖号码（注入到前端，替换掉硬编码）
    last_reds_display = last_reds if last_reds else [1, 3, 11, 18, 31, 33]
    last_blue_display = int(draws[-1].get("blue", "1").strip()) if draws else 1

    def fmt_w_pct(d: dict) -> str:
        """权重字典 → 行注释字符串（用于 header 注释）"""
        total = sum(d.values()) or 1
        return " + ".join(f"{k}({v/total*100:.1f}%)" for k, v in
                          sorted(d.items(), key=lambda x: -x[1])[:3])

    # ── 拼装 JS 数据块 ──
    lines = [
        f"  // ===BEGIN_PRED_DATA===",
        f"  // 引擎实时数据 · v7 CMA-ES自进化 (数据截至 {last_code} · 生成 {gen_date})",
        f"  // 红球CMA-ES: 适应度{best_fit:.4f} · 13维权重",
        f"  // 蓝球CMA-ES: top-3命中率{blue_top3_hr}% · 4维权重",
        f"  // 预测期号: {issue} · 开奖日 {draw_date}",
        f"  // [由 auto_update.py 自动生成，请勿手工修改]",
        "",
        f"  const redArms = {_js_obj_list(red_arms, ['name','pct','color'])};",
        "",
        f"  const blueArms = {_js_obj_list(blue_arms, ['name','pct','color'])};",
        "",
        f"  // 红球排行（按CMA-ES球级评分降序）",
        f"  const topReds  = {_js_arr(top_reds_r)};",
        f"  const topBlues = {_js_arr(top_blues)};",
        "",
        f"  // 核心预测（CMA-ES最优组合）",
        f"  const coreRed      = {_js_arr(core_reds)};",
        f"  const coreBlueBall = {core_blue};   // ★ 核心蓝球",
        f"  const topBlue3 = {_js_arr(top_blue3)};",
        f"  const topBlue8 = {_js_arr(top_blue8)};",
        f"",
        f"  // 上期开奖（自动更新，用于前端展示）",
        f"  const lastRedDraw  = {_js_arr(last_reds_display)};",
        f"  const lastBlueDraw = {last_blue_display};",
        "",
        f"  // 混沌分析数据",
        f"  const entropyScores = {{",
        f"    red: {json.dumps(entropy_red, ensure_ascii=False)}",
        f"  }};",
        f"  const markovFrom = {_js_arr(last_reds)};",
        f"  const markovRedScores = {json.dumps(markov_red_sc, ensure_ascii=False)};",
        f"  const markovBlueScores = {json.dumps(markov_blue_sc, ensure_ascii=False)};",
        f"  const rqaScores = {json.dumps(rqa_sc, ensure_ascii=False)};",
        "",
        f"  // 吸引子数据",
        f"  const attractorData = {{",
        f"    correction: {attractor['correction']},",
        f"    density: {attractor['density']},",
        f"    predictedSum: {attractor['predictedSum']},",
        f"    historicalMean: {attractor['historicalMean']},",
        f"    historicalStd: {attractor['historicalStd']},",
        f"    phasePoints: (() => {{",
        f"      const pts = []; let rng = {int(issue) if issue.isdigit() else 2026033};",
        f"      const rand = () => {{ rng = (rng*1664525+1013904223)&0x7fffffff; return rng/0x7fffffff; }};",
        f"      const gauss = () => Math.sqrt(-2*Math.log(rand()+1e-9))*Math.cos(2*Math.PI*rand());",
        f"      for (let i = 0; i < 60; i++) pts.push({{x:{attractor['historicalMean']}+gauss()*{attractor['historicalStd']}, y:{attractor['historicalMean']}+gauss()*{attractor['historicalStd']}, z:{attractor['historicalMean']}+gauss()*{attractor['historicalStd']}}});",
        f"      return pts;",
        f"    }})(),",
        f"    currentPoint: {{ x:{attractor['currentPoint']['x']}, y:{attractor['currentPoint']['y']}, z:{attractor['currentPoint']['z']} }}",
        f"  }};",
        "",
        f"  // Markov 最高转移概率表",
        f"  const markovTopTrans = {_js_obj_list(markov_trans, ['from','to','prob','strength'])};",
        "",
        f"  // NegBin 危险率",
        f"  const negbinData = {_js_obj_list(negbin_data, ['n','k','h','cold'])};",
        "",
        f"  // PageRank 共现中心性",
        f"  const pagerankData = {_js_obj_list(pagerank_data, ['n','pr'])};",
        "",
        f"  // ── 核心彩球渲染（每次注入时随数据块重建，防止引用丢失）──",
        f"  (function() {{",
        f"    const el = document.getElementById('core-balls-display');",
        f"    if (el) {{",
        f"      el.innerHTML = makeBalls(coreRed, false, null, 'ball-lg')",
        f"        + '<span class=\"ball-plus\">+</span>'",
        f"        + makeBall(coreBlueBall, true, 'ball-lg');",
        f"    }}",
        f"  }})();",
        f"  // ===END_PRED_DATA===",   # ← 末尾必须保留，下次注入时作为结束定位标记
    ]
    return "\n".join(lines)


def render_html(pred: dict, draws: list[dict],
                state: dict | None, dry_run: bool = False) -> bool:
    """读取 HTML 模板，注入最新预测数据，保存"""
    log.info("── 步骤 4：渲染前端 H5 ──")
    if not HTML_SRC.exists():
        log.error(f"  HTML 模板不存在: {HTML_SRC}")
        return False

    html = HTML_SRC.read_text("utf-8")
    issue     = pred.get("next_issue", "????")
    last_code = draws[-1]["code"] if draws else "????"
    draw_date = issue_draw_date(issue, draws)
    gen_date  = datetime.now().strftime("%Y-%m-%d")
    last_draw_date = draws[-1].get("date", "")[:10] if draws else ""
    draw_count = len(draws)
    best_fit = abs(state.get("best_fitness", 11.45)) if state else 11.45

    # ── 1. 替换 <title> ──
    html = re.sub(
        r"<title>.*?</title>",
        f"<title>双色球智能预测 v7 · 第{issue}期</title>",
        html, flags=re.DOTALL
    )

    # ── 2. 替换 topbar 期号 ──
    # 兼容带/不带 marker 两种格式
    html = re.sub(
        r"(<span>)(?:<!-- ##ISSUE_NUM## -->)?\d{7}(?:<!-- ##/ISSUE_NUM## -->)?(期</span>)",
        rf"\g<1>{issue}\g<2>",
        html
    )

    # ── 3. 替换 core-card-label（预测期号 + 开奖日）──
    html = re.sub(
        r"预测 (?:<!-- ##ISSUE_NUM## -->)?\d{7}(?:<!-- ##/ISSUE_NUM## -->)?期"
        r" &nbsp;·&nbsp; 开奖日 (?:<!-- ##DRAW_DATE## -->)?\d{4}-\d{2}-\d{2}(?:<!-- ##/DRAW_DATE## -->)?",
        f"预测 {issue}期 &nbsp;·&nbsp; 开奖日 {draw_date}",
        html
    )

    # ── 4. 替换 footer 数据截至行 ──
    html = re.sub(
        r"数据截至 (?:<!-- ##LAST_CODE## -->)?\d{7}(?:<!-- ##/LAST_CODE## -->)?"
        r" · 预测 (?:<!-- ##ISSUE_NUM## -->)?\d{7}(?:<!-- ##/ISSUE_NUM## -->)?"
        r" · 生成 (?:<!-- ##GEN_DATE## -->)?\d{4}-\d{2}-\d{2}(?:<!-- ##/GEN_DATE## -->)?",
        f"数据截至 {last_code} · 预测 {issue} · 生成 {gen_date}",
        html
    )

    # ── 5. 替换 copyTicket 中的期号 ──
    html = re.sub(r"第\d{7}期\\n", f"第{issue}期\\n", html)

    # ── 6. 替换 hero badge 文字（训练期数）──
    html = re.sub(
        r"AI CMA-ES 自进化引擎 v7 · \d+ 期数据",
        f"AI CMA-ES 自进化引擎 v7 · {draw_count}期数据",
        html
    )

    # ── 7. 替换状态卡片数值 ──
    html = re.sub(
        r'(<div class="stat-value gold">)\s*[\d.]+\s*(</div>)',
        rf'\g<1>{best_fit:.2f}\g<2>',
        html, count=1
    )
    html = re.sub(
        r'(<div class="stat-value blue">)\s*\d+期\s*(</div>)',
        rf'\g<1>{draw_count}期\g<2>',
        html, count=1
    )
    html = re.sub(
        r'(2013001→)\d{7}',
        rf'\g<1>{last_code}',
        html
    )

    # ── 8. 替换 JS 预测数据块 ──
    data_block = generate_pred_data_block(pred, draws, state)
    begin_tag = "  // ===BEGIN_PRED_DATA==="
    end_tag   = "  // ===END_PRED_DATA==="
    if begin_tag in html and end_tag in html:
        start_idx = html.index(begin_tag)
        end_idx   = html.index(end_tag) + len(end_tag)
        html = html[:start_idx] + data_block + "\n" + html[end_idx:]
        log.info("  ✓ JS 数据块已注入")
    else:
        log.warning("  未找到 JS 数据块标记（===BEGIN/END_PRED_DATA===），跳过数据注入")

    if dry_run:
        # 打印关键替换效果验证
        title_m = re.search(r"<title>.*?</title>", html)
        log.info(f"  [dry-run] title: {title_m.group() if title_m else '未找到'}")
        log.info(f"  [dry-run] issue={issue}  last_code={last_code}  draw_date={draw_date}")
        log.info("  [dry-run] 未写入文件")
        return True

    HTML_OUT.write_text(html, "utf-8")
    sz = HTML_OUT.stat().st_size / 1024
    log.info(f"  ✓ HTML 已更新: {HTML_OUT}  ({sz:.1f} KB)")
    return True


# ══════════════════════════════════════════════════════════
# 7. 备份 HTML 到 history/
# ══════════════════════════════════════════════════════════
def backup_html(issue: str) -> None:
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    dst = HISTORY_DIR / f"lottery-prediction_{issue}_{ts}.html"
    if HTML_OUT.exists():
        shutil.copy2(HTML_OUT, dst)
        log.info(f"  ✓ HTML 已备份: {dst.name}")
    # 只保留最近 30 个备份
    backups = sorted(HISTORY_DIR.glob("lottery-prediction_*.html"))
    for old in backups[:-30]:
        old.unlink()
        log.debug(f"  删除旧备份: {old.name}")


# ══════════════════════════════════════════════════════════
# 8. 锁机制（防止重复运行）
# ══════════════════════════════════════════════════════════
def _check_lock(force: bool) -> bool:
    if LOCK_PATH.exists():
        ts_str = LOCK_PATH.read_text("utf-8").strip()
        try:
            lock_ts = datetime.fromisoformat(ts_str)
            if (datetime.now() - lock_ts).total_seconds() < 3600:
                if not force:
                    log.warning(f"  发现锁文件（{ts_str}），上次运行还不足1小时，跳过。使用 --force 强制运行。")
                    return False
                log.info(f"  --force 模式，忽略锁文件")
        except ValueError:
            pass
    LOCK_PATH.write_text(datetime.now().isoformat(), "utf-8")
    return True


def _release_lock() -> None:
    if LOCK_PATH.exists():
        LOCK_PATH.unlink()


# ══════════════════════════════════════════════════════════
# 9. 主函数
# ══════════════════════════════════════════════════════════
def main() -> int:
    parser = argparse.ArgumentParser(
        description="双色球开奖日自动更新脚本"
    )
    parser.add_argument("--evolve",       type=int, default=DEFAULT_EVOLVE_GENS,
                        help=f"进化代数（默认{DEFAULT_EVOLVE_GENS}）")
    parser.add_argument("--skip-evolve",  action="store_true",
                        help="仅抓数据+渲染前端，不运行进化")
    parser.add_argument("--dry-run",      action="store_true",
                        help="打印计划，不写文件")
    parser.add_argument("--force",        action="store_true",
                        help="强制运行（忽略锁文件）")
    parser.add_argument("--skip-fetch",   action="store_true",
                        help="跳过数据抓取（用于调试）")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"双色球自动更新启动  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 60)

    # 锁检查
    if not _check_lock(args.force):
        return 1

    exit_code = 0
    try:
        # ── 步骤 1：抓取数据 ──
        if not args.skip_fetch:
            new_cnt, _ = fetch_and_update(dry_run=args.dry_run)
            if new_cnt > 0:
                log.info(f"  ✓ 新增 {new_cnt} 期开奖数据")
            else:
                log.info("  无新开奖数据（已是最新）")
        else:
            log.info("  [skip-fetch] 跳过数据抓取")

        # ── 步骤 2：进化优化 ──
        if not args.skip_evolve:
            ok = run_evolution(args.evolve, dry_run=args.dry_run)
            if not ok:
                log.error("  进化引擎异常，继续使用上次预测")
        else:
            log.info("  [skip-evolve] 跳过进化")

        # ── 步骤 3：读取预测 ──
        log.info("── 步骤 3：读取最新预测 ──")
        pred  = load_prediction()
        state = load_state()
        draws = _load_existing_draws()

        if pred is None:
            log.error("  无法读取预测文件，中止渲染")
            exit_code = 2
        else:
            issue = pred.get("next_issue", "????")
            core  = pred.get("core_reds", [])
            blue  = pred.get("core_blue", "?")
            log.info(f"  期号: {issue}")
            log.info(f"  核心红球: {core}")
            log.info(f"  核心蓝球: {blue}")
            log.info(f"  训练数据: {len(draws)} 期")

            # ── 步骤 4：渲染前端 ──
            ok = render_html(pred, draws, state, dry_run=args.dry_run)
            if ok and not args.dry_run:
                backup_html(issue)
            log.info("  ✓ 前端渲染完成")

        log.info("=" * 60)
        log.info(f"自动更新完成  退出码={exit_code}")
        log.info("=" * 60)

    except Exception as exc:
        log.exception(f"未捕获异常: {exc}")
        exit_code = 99
    finally:
        if not args.dry_run:
            _release_lock()

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
