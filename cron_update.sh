#!/usr/bin/env bash
# ================================================================
# cron_update.sh — 服务器端定时更新脚本
# 每周二/四/日 22:00 由 cron 调用
# 抓取最新开奖 → 进化 → 更新 HTML → 热更新到 nginx 容器
# ================================================================
set -euo pipefail

PROJECT_DIR="/opt/baicaibaizhong"
LOG_FILE="${PROJECT_DIR}/outputs/auto_update.log"

cd "$PROJECT_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S')  开始自动更新..." >> "$LOG_FILE"

# 1. 运行 auto_update.py（抓取 + 进化 + 渲染 HTML）
SSQ_HTML_TEMPLATE="${PROJECT_DIR}/lottery-prediction.html" \
    python3 "${PROJECT_DIR}/project/auto_update.py" --force \
    >> "$LOG_FILE" 2>&1

# 2. 热更新 nginx 容器中的 HTML（无需重启）
docker cp "${PROJECT_DIR}/lottery-prediction.html" \
    baicaibaizhong:/usr/share/nginx/html/lottery-prediction.html

echo "$(date '+%Y-%m-%d %H:%M:%S')  自动更新完成" >> "$LOG_FILE"
