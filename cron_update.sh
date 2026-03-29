#!/usr/bin/env bash
# ================================================================
# cron_update.sh — 服务器端定时更新脚本
# 每周二/四/日 22:00 由 cron 调用
# 抓取最新开奖 → 进化 → 更新 HTML → 热更新到 nginx 容器
# ================================================================
set -euo pipefail

PROJECT_DIR="/opt/baicaibaizhong"
LOG_FILE="${PROJECT_DIR}/outputs/auto_update.log"
DATA_FILE="${PROJECT_DIR}/outputs/official_draws.json"
PYTHON="${PYTHON:-python3}"

cd "$PROJECT_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S')  开始自动更新..." >> "$LOG_FILE"

# ── 1. 用 curl 抓取最新开奖（绕过 Python urllib 的 403 问题）──
COOKIE_FILE=$(mktemp)
LATEST_CODE=$($PYTHON -c "import json; d=json.load(open('$DATA_FILE')); print(max(x['code'] for x in d))")
NEXT_CODE=$((LATEST_CODE + 1))

echo "$(date '+%Y-%m-%d %H:%M:%S')  抓取期号 >= $NEXT_CODE ..." >> "$LOG_FILE"

# 先访问首页拿 cookie
curl -s -c "$COOKIE_FILE" -L 'https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/' \
    -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36' \
    --max-time 15 -o /dev/null 2>/dev/null || true

# 再请求 API
API_RESP=$(curl -s -b "$COOKIE_FILE" -L \
    "https://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice?name=ssq&issueStart=${NEXT_CODE}&pageNo=1&pageSize=30&systemType=PC" \
    -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36' \
    -H 'Referer: https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -H 'Accept: application/json' \
    --max-time 20 2>/dev/null || echo '{}')
rm -f "$COOKIE_FILE"

# 解析并写入 official_draws.json
$PYTHON - "$API_RESP" "$DATA_FILE" << 'PYEOF'
import json, sys

api_text = sys.argv[1]
data_file = sys.argv[2]

try:
    resp = json.loads(api_text)
except Exception:
    print("  curl 返回非 JSON，跳过抓取", file=sys.stderr)
    sys.exit(0)

if resp.get("message") != "查询成功":
    print(f"  API 异常: {resp.get('message','unknown')}", file=sys.stderr)
    sys.exit(0)

results = resp.get("result") or []
if not results:
    print("  无新开奖数据", file=sys.stderr)
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
        print(f"  新增: {code} 红={r.get('red')} 蓝={r.get('blue')}", file=sys.stderr)

if added:
    draws.sort(key=lambda x: int(x["code"]))
    with open(data_file, "w") as f:
        json.dump(draws, f, ensure_ascii=False, indent=2)
    print(f"  共新增 {added} 期，总计 {len(draws)} 期", file=sys.stderr)
PYEOF

echo "$(date '+%Y-%m-%d %H:%M:%S')  抓取完成" >> "$LOG_FILE" 2>&1

# ── 2. 运行 auto_update.py（跳过抓取，直接进化+渲染）──
SSQ_HTML_TEMPLATE="${PROJECT_DIR}/lottery-prediction.html" \
    $PYTHON "${PROJECT_DIR}/project/auto_update.py" --force --skip-fetch \
    >> "$LOG_FILE" 2>&1

# ── 3. 热更新 nginx 容器中的 HTML（无需重启）──
docker cp "${PROJECT_DIR}/lottery-prediction.html" \
    baicaibaizhong:/usr/share/nginx/html/lottery-prediction.html

echo "$(date '+%Y-%m-%d %H:%M:%S')  自动更新完成" >> "$LOG_FILE"
