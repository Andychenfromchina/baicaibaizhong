#!/usr/bin/env bash
# ================================================================
# cron_update.sh — 服务器端定时更新脚本
# 每周二/四/日 22:00 由 cron 调用
#
# 因 CWL 官网封锁海外 IP，数据抓取由本地 Mac 的 local_fetch.sh 推送。
# 本脚本只负责：进化 → 渲染 HTML → 热更新 nginx
# ================================================================
set -euo pipefail

PROJECT_DIR="/opt/baicaibaizhong"
LOG_FILE="${PROJECT_DIR}/outputs/auto_update.log"
PYTHON="${PYTHON:-python3}"

cd "$PROJECT_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S')  开始自动更新..." >> "$LOG_FILE"

# 运行 auto_update.py（跳过抓取，直接进化+渲染）
SSQ_HTML_TEMPLATE="${PROJECT_DIR}/lottery-prediction.html" \
    $PYTHON "${PROJECT_DIR}/project/auto_update.py" --force --skip-fetch \
    >> "$LOG_FILE" 2>&1

# 热更新 nginx 容器中的 HTML
docker cp "${PROJECT_DIR}/lottery-prediction.html" \
    baicaibaizhong:/usr/share/nginx/html/lottery-prediction.html

echo "$(date '+%Y-%m-%d %H:%M:%S')  自动更新完成" >> "$LOG_FILE"
