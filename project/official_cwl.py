from __future__ import annotations

import http.cookiejar
import json
import math
import re
import ssl
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


CWL_HOME_URL = "https://www.cwl.gov.cn/ygkj/wqkjgg/ssq/"
CWL_DRAW_NOTICE_URL = "https://www.cwl.gov.cn/cwl_admin/front/cwlkj/search/kjxx/findDrawNotice"


@dataclass(frozen=True)
class OfficialMetrics:
    sales: int | None = None
    poolmoney: int | None = None
    prize_counts: tuple[int | None, ...] = ()
    prize_amounts: tuple[int | None, ...] = ()
    first_region_count: int = 0
    first_region_max_share: float = 0.0
    first_region_entropy: float = 0.0
    fixed_prize_total: int = 0

    def prize_count(self, prize_type: int) -> int | None:
        index = prize_type - 1
        if 0 <= index < len(self.prize_counts):
            return self.prize_counts[index]
        return None

    def prize_amount(self, prize_type: int) -> int | None:
        index = prize_type - 1
        if 0 <= index < len(self.prize_amounts):
            return self.prize_amounts[index]
        return None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def parse_int(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    return None


def parse_first_region_stats(content: str, fallback_total: int | None) -> tuple[int, float, float]:
    region_hits: list[int] = []
    for region, count_text in re.findall(r"([\u4e00-\u9fa5A-Za-z]+)(\d+)注", content):
        if region == "共":
            continue
        count = parse_int(count_text)
        if count:
            region_hits.append(count)
    if not region_hits:
        return 0, 0.0, 0.0
    total_hits = sum(region_hits)
    if fallback_total:
        total_hits = max(total_hits, fallback_total)
    max_share = max(region_hits) / total_hits if total_hits else 0.0
    if total_hits <= 0 or len(region_hits) <= 1:
        return len(region_hits), max_share, 0.0
    entropy = -sum(
        (count / total_hits) * math.log(count / total_hits)
        for count in region_hits
        if count > 0
    ) / math.log(len(region_hits))
    return len(region_hits), max_share, entropy


def build_official_metrics(record: dict[str, Any]) -> OfficialMetrics:
    grades = record.get("prizegrades") or []
    grade_map = {
        grade_type: grade
        for grade in grades
        if (grade_type := parse_int((grade or {}).get("type"))) is not None
    }
    prize_counts = tuple(parse_int((grade_map.get(prize_type) or {}).get("typenum")) for prize_type in range(1, 8))
    prize_amounts = tuple(parse_int((grade_map.get(prize_type) or {}).get("typemoney")) for prize_type in range(1, 8))
    first_count = prize_counts[0] if prize_counts else None
    region_count, max_share, entropy = parse_first_region_stats(str(record.get("content") or ""), first_count)
    fixed_prize_total = sum(
        (prize_counts[prize_type - 1] or 0) * (prize_amounts[prize_type - 1] or 0)
        for prize_type in range(3, 7)
    )
    return OfficialMetrics(
        sales=parse_int(record.get("sales")),
        poolmoney=parse_int(record.get("poolmoney")),
        prize_counts=prize_counts,
        prize_amounts=prize_amounts,
        first_region_count=region_count,
        first_region_max_share=max_share,
        first_region_entropy=entropy,
        fixed_prize_total=fixed_prize_total,
    )


def _build_opener() -> urllib.request.OpenerDirector:
    cookie_jar = http.cookiejar.CookieJar()
    context = ssl._create_unverified_context()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookie_jar),
        urllib.request.HTTPSHandler(context=context),
    )
    opener.addheaders = [
        ("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
        ("Accept", "application/json,text/javascript,*/*;q=0.01"),
        ("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8"),
        ("Accept-Encoding", "gzip, deflate, br"),
        ("Referer", CWL_HOME_URL),
        ("X-Requested-With", "XMLHttpRequest"),
    ]
    return opener


def _fetch_json(
    opener: urllib.request.OpenerDirector,
    url: str,
    max_retries: int = 3,
    backoff_base: float = 1.5,
) -> dict[str, Any]:
    last_exc: Exception | None = None
    for attempt in range(max_retries):
        try:
            with opener.open(url, timeout=30) as response:
                raw = response.read()
                # Handle gzip-encoded responses when not auto-decoded
                try:
                    return json.loads(raw.decode("utf-8"))
                except UnicodeDecodeError:
                    return json.loads(raw.decode("utf-8", errors="replace"))
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries - 1:
                time.sleep(backoff_base ** attempt)
    raise last_exc  # type: ignore[misc]


def fetch_ssq_draw_notices(
    issue_start: int,
    issue_end: int | None = None,
    page_size: int = 200,
) -> list[dict[str, Any]]:
    opener = _build_opener()
    with opener.open(CWL_HOME_URL, timeout=30) as response:
        response.read(256)

    page_no = 1
    results: list[dict[str, Any]] = []
    total: int | None = None
    while total is None or len(results) < total:
        params: dict[str, Any] = {
            "name": "ssq",
            "issueStart": issue_start,
            "pageNo": page_no,
            "pageSize": page_size,
            "systemType": "PC",
        }
        if issue_end is not None:
            params["issueEnd"] = issue_end
        payload = _fetch_json(opener, f"{CWL_DRAW_NOTICE_URL}?{urllib.parse.urlencode(params)}")
        if str(payload.get("message")) != "查询成功":
            raise RuntimeError(f"中国福彩网接口返回异常: {payload.get('message')}")
        page_results = payload.get("result") or []
        results.extend(page_results)
        total = parse_int(payload.get("total")) or len(results)
        if not page_results:
            break
        page_no += 1
    return sorted(results, key=lambda item: int(item["code"]))


def load_or_refresh_ssq_cache(
    cache_path: Path,
    issue_start: int,
    issue_end: int | None = None,
) -> tuple[list[dict[str, Any]], str]:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        records = fetch_ssq_draw_notices(issue_start=issue_start, issue_end=issue_end)
        cache_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")
        return records, "live"
    except Exception:
        if cache_path.exists():
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            return cached, "cache_fallback"
        raise
