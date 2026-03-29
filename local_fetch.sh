#!/usr/bin/env bash
# ================================================================
# local_fetch.sh — 本地 Mac 端抓取 + 推送脚本
# 每周二/四/日 22:00 由本地 cron 调用
#
# 流程：
#   1. 从 CWL 官网抓取最新开奖（国内网络/代理）
#   2. 写入本地 official_draws.json
#   3. scp 推送数据文件到阿里云服务器
#   4. SSH 触发服务器端 cron_update.sh（进化+渲染+热更新）
# ================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${SCRIPT_DIR}"
DATA_FILE="${PROJECT_DIR}/outputs/official_draws.json"
LOG_FILE="${PROJECT_DIR}/outputs/local_fetch.log"
PYTHON="${PYTHON:-python3}"

# 服务器参数
SERVER="43.106.103.59"
SERVER_USER="root"
SERVER_PASS="Chen123123"
SERVER_DIR="/opt/baicaibaizhong"

log() { echo "$(date '+%Y-%m-%d %H:%M:%S')  $*" | tee -a "$LOG_FILE"; }

log "══ 本地抓取启动 ══"

# ── 1. 抓取最新开奖 ──
LATEST_CODE=$($PYTHON -c "import json; d=json.load(open('$DATA_FILE')); print(max(x['code'] for x in d))")
NEXT_CODE=$((LATEST_CODE + 1))
log "本地最新: $LATEST_CODE，请求: $NEXT_CODE"

COOKIE_FILE=$(mktemp)

# 访问首页拿 cookie（走代理或直连）
PROXY_OPTS=""
if curl -s --max-time 5 'https://www.cwl.gov.cn' -o /dev/null 2>/dev/null; then
    log "直连 CWL 成功"
elif curl -s --proxy http://127.0.0.1:7892 --max-time 5 'https://www.cwl.gov.cn' -o /dev/null 2>/dev/null; then
    PROXY_OPTS="--proxy http://127.0.0.1:7892"
    log "使用代理 127.0.0.1:7892"
else
    log "ERROR: 无法连接 CWL 官网"
    rm -f "$COOKIE_FILE"
    exit 1
fi

curl -s $PROXY_OPTS -c "$COOKIE_FILE" -L 'https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/' \
    -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
    --max-time 15 -o /dev/null 2>/dev/null

API_RESP=$(curl -s $PROXY_OPTS -b "$COOKIE_FILE" -L \
    "https://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice?name=ssq&issueStart=${NEXT_CODE}&pageNo=1&pageSize=30&systemType=PC" \
    -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
    -H 'Referer: https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -H 'Accept: application/json' \
    --max-time 20 2>/dev/null || echo '{}')
rm -f "$COOKIE_FILE"

# 解析并写入
ADDED=$($PYTHON - "$API_RESP" "$DATA_FILE" << 'PYEOF'
import json, sys

api_text, data_file = sys.argv[1], sys.argv[2]
try:
    resp = json.loads(api_text)
except Exception:
    print("0")
    sys.exit(0)

if resp.get("message") != "查询成功":
    print("0")
    sys.exit(0)

results = resp.get("result") or []
if not results:
    print("0")
    sys.exit(0)

draws = json.loads(open(data_file).read())
existing = {d["code"] for d in draws}
added = 0

for r in results:
    code = str(r["code"])
    if code not in existing:
        draws.append({
            "code": code,
            "red": str(r.get("red", "")),
            "blue": str(r.get("blue", "")),
            "date": str(r.get("date", "")),
            "sales": str(r.get("sales", "")),
            "poolmoney": str(r.get("poolmoney", "")),
            "prizegrades": r.get("prizegrades", []),
        })
        added += 1
        print(f"新增: {code} 红={r.get('red')} 蓝={r.get('blue')}", file=sys.stderr)

if added:
    draws.sort(key=lambda x: int(x["code"]))
    with open(data_file, "w") as f:
        json.dump(draws, f, ensure_ascii=False, indent=2)

print(str(added))
PYEOF
)

log "新增 ${ADDED} 期"

if [[ "$ADDED" == "0" ]]; then
    log "无新数据，跳过推送"
    exit 0
fi

# ── 2. 推送数据到服务器 ──
log "推送 official_draws.json 到服务器..."
expect -c "
set timeout 30
spawn scp -o StrictHostKeyChecking=no ${DATA_FILE} ${SERVER_USER}@${SERVER}:${SERVER_DIR}/outputs/official_draws.json
expect \"*password*\"
send \"${SERVER_PASS}\r\"
expect eof
" >> "$LOG_FILE" 2>&1

# ── 3. 触发服务器端进化+渲染 ──
log "触发服务器端 cron_update.sh..."
expect -c "
set timeout 120
spawn ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER} \"bash ${SERVER_DIR}/cron_update.sh 2>&1\"
expect \"*password*\"
send \"${SERVER_PASS}\r\"
expect eof
" >> "$LOG_FILE" 2>&1

log "══ 本地抓取完成 ══"
