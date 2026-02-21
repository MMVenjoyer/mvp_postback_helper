"""
Report Router — воронка продаж: когортный и некогортный анализ

Эндпоинты:
- GET /api/report/funnel?type=cohort&start_date=2026-02-10&end_date=2026-02-22
- GET /api/report/funnel?type=non_cohort&start_date=2026-02-10&end_date=2026-02-22

Когортный: группировка по дню joined_bot_time, конверсии внутри когорты
Некогортный: события по дню их фактического наступления

Таймзона: Europe/Berlin (UTC+1)
"""

from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from enum import Enum

from db import DataBase

router = APIRouter()
db = DataBase()

TZ = "Europe/Berlin"


class ReportType(str, Enum):
    cohort = "cohort"
    non_cohort = "non_cohort"


def _run_query(query: str, params: tuple) -> List[Dict[str, Any]]:
    """Выполняет SQL запрос и возвращает список словарей"""
    import psycopg2.extras

    with db.get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def _serialize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Сериализует значения (date/datetime → ISO string, Decimal → float)"""
    from decimal import Decimal

    out = {}
    for k, v in row.items():
        if isinstance(v, (date, datetime)):
            out[k] = v.isoformat()
        elif isinstance(v, Decimal):
            out[k] = float(v)
        elif v is None:
            out[k] = None
        else:
            out[k] = v
    return out


# ==========================================
# КОГОРТНЫЙ ОТЧЁТ
# ==========================================

COHORT_SQL = """
WITH base AS (
    SELECT
        id,
        date_trunc('day', joined_bot_time AT TIME ZONE %(tz)s) AS cohort_day,
        joined_bot_time AT TIME ZONE %(tz)s AS joined_bot_time,
        joined_main_time AT TIME ZONE %(tz)s AS joined_main_time,
        ftm_time AT TIME ZONE %(tz)s AS ftm_time,
        reg_time AT TIME ZONE %(tz)s AS reg_time,
        dep_time AT TIME ZONE %(tz)s AS dep_time
    FROM users
    WHERE joined_bot_time IS NOT NULL
      AND joined_bot_time AT TIME ZONE %(tz)s >= %(start)s::timestamp
      AND joined_bot_time AT TIME ZONE %(tz)s < %(end)s::timestamp + INTERVAL '1 day'
),
flags AS (
    SELECT
        cohort_day,
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE joined_main_time IS NOT NULL) AS main,
        COUNT(*) FILTER (WHERE ftm_time IS NOT NULL) AS ftm,
        COUNT(*) FILTER (WHERE reg_time IS NOT NULL) AS reg,
        COUNT(*) FILTER (WHERE dep_time IS NOT NULL) AS dep
    FROM base
    GROUP BY cohort_day
)
SELECT
    cohort_day::date AS day,
    total,
    main,
    ROUND(main::decimal / NULLIF(total, 0) * 100, 2) AS conv_bot_to_main,
    ftm,
    ROUND(ftm::decimal / NULLIF(main, 0) * 100, 2) AS conv_main_to_ftm,
    reg,
    ROUND(reg::decimal / NULLIF(ftm, 0) * 100, 2) AS conv_ftm_to_reg,
    dep,
    ROUND(dep::decimal / NULLIF(reg, 0) * 100, 2) AS conv_reg_to_dep,
    ROUND(dep::decimal / NULLIF(total, 0) * 100, 2) AS full_funnel
FROM flags
ORDER BY cohort_day
"""


# ==========================================
# НЕКОГОРТНЫЙ ОТЧЁТ
# ==========================================

NON_COHORT_SQL = """
WITH events AS (
    SELECT (joined_bot_time AT TIME ZONE %(tz)s)::date AS day, 'new_users' AS evt
    FROM users WHERE joined_bot_time IS NOT NULL
    UNION ALL
    SELECT (joined_main_time AT TIME ZONE %(tz)s)::date, 'joined_main'
    FROM users WHERE joined_main_time IS NOT NULL
    UNION ALL
    SELECT (ftm_time AT TIME ZONE %(tz)s)::date, 'ftm'
    FROM users WHERE ftm_time IS NOT NULL
    UNION ALL
    SELECT (reg_time AT TIME ZONE %(tz)s)::date, 'reg'
    FROM users WHERE reg_time IS NOT NULL
    UNION ALL
    SELECT (dep_time AT TIME ZONE %(tz)s)::date, 'dep'
    FROM users WHERE dep_time IS NOT NULL
)
SELECT
    day,
    COUNT(*) FILTER (WHERE evt = 'new_users') AS new_users,
    COUNT(*) FILTER (WHERE evt = 'joined_main') AS joined_main,
    COUNT(*) FILTER (WHERE evt = 'ftm') AS ftm,
    COUNT(*) FILTER (WHERE evt = 'reg') AS reg,
    COUNT(*) FILTER (WHERE evt = 'dep') AS dep
FROM events
WHERE day BETWEEN %(start)s AND %(end)s
GROUP BY day
ORDER BY day
"""


# ==========================================
# СУММАРНАЯ СТРОКА (totals)
# ==========================================

def _compute_totals_cohort(rows: List[Dict]) -> Dict[str, Any]:
    """Агрегирует totals по всем дням когортного отчёта"""
    t = {"day": "total", "total": 0, "main": 0, "ftm": 0, "reg": 0, "dep": 0}
    for r in rows:
        t["total"] += r.get("total", 0) or 0
        t["main"] += r.get("main", 0) or 0
        t["ftm"] += r.get("ftm", 0) or 0
        t["reg"] += r.get("reg", 0) or 0
        t["dep"] += r.get("dep", 0) or 0

    def pct(a, b):
        return round(a / b * 100, 2) if b else None

    t["conv_bot_to_main"] = pct(t["main"], t["total"])
    t["conv_main_to_ftm"] = pct(t["ftm"], t["main"])
    t["conv_ftm_to_reg"] = pct(t["reg"], t["ftm"])
    t["conv_reg_to_dep"] = pct(t["dep"], t["reg"])
    t["full_funnel"] = pct(t["dep"], t["total"])
    return t


def _compute_totals_non_cohort(rows: List[Dict]) -> Dict[str, Any]:
    """Агрегирует totals по всем дням некогортного отчёта"""
    t = {"day": "total", "new_users": 0, "joined_main": 0, "ftm": 0, "reg": 0, "dep": 0}
    for r in rows:
        t["new_users"] += r.get("new_users", 0) or 0
        t["joined_main"] += r.get("joined_main", 0) or 0
        t["ftm"] += r.get("ftm", 0) or 0
        t["reg"] += r.get("reg", 0) or 0
        t["dep"] += r.get("dep", 0) or 0
    return t


# ==========================================
# ЭНДПОИНТ
# ==========================================

@router.get("/funnel")
async def get_funnel_report(
    type: ReportType = Query(..., description="Тип отчёта: cohort или non_cohort"),
    start_date: date = Query(..., description="Начало диапазона (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Конец диапазона (YYYY-MM-DD)")
):
    """
    Воронка продаж — когортный или некогортный анализ по дням.

    Примеры:
      GET /api/report/funnel?type=cohort&start_date=2026-02-10&end_date=2026-02-22
      GET /api/report/funnel?type=non_cohort&start_date=2026-02-10&end_date=2026-02-22

    Ответ:
    {
      "status": "ok",
      "report_type": "cohort",
      "start_date": "2026-02-10",
      "end_date": "2026-02-22",
      "days": 13,
      "rows": [ ... ],
      "totals": { ... }
    }
    """
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    params = {
        "tz": TZ,
        "start": str(start_date),
        "end": str(end_date),
    }

    try:
        if type == ReportType.cohort:
            raw_rows = _run_query(COHORT_SQL, params)
            rows = [_serialize_row(r) for r in raw_rows]
            totals = _compute_totals_cohort(rows)
        else:
            raw_rows = _run_query(NON_COHORT_SQL, params)
            rows = [_serialize_row(r) for r in raw_rows]
            totals = _compute_totals_non_cohort(rows)

        return {
            "status": "ok",
            "report_type": type.value,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": TZ,
            "days": len(rows),
            "rows": rows,
            "totals": totals,
        }

    except Exception as e:
        print(f"[REPORT] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trader_ids")
async def get_all_trader_ids():
    """
    Возвращает список всех trader_id из базы (не NULL, не пустые).

    GET /api/report/trader_ids

    Ответ:
    {
      "status": "ok",
      "count": 123,
      "trader_ids": ["TRD_001", "TRD_002", ...]
    }
    """
    try:
        import psycopg2.extras

        with db.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT trader_id
                    FROM users
                    WHERE trader_id IS NOT NULL
                      AND trader_id != ''
                    ORDER BY trader_id
                """)
                rows = cur.fetchall()

        trader_ids = [r[0] for r in rows]

        return {
            "status": "ok",
            "count": len(trader_ids),
            "trader_ids": trader_ids,
        }

    except Exception as e:
        print(f"[REPORT] ✗ Exception in trader_ids: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/funnel/summary")
async def get_funnel_summary(
    start_date: date = Query(..., description="Начало диапазона (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Конец диапазона (YYYY-MM-DD)")
):
    """
    Быстрое сравнение: когортный vs некогортный за один запрос.
    Удобно для дашборда — возвращает оба totals рядом.

    GET /api/report/funnel/summary?start_date=2026-02-10&end_date=2026-02-22
    """
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    params = {
        "tz": TZ,
        "start": str(start_date),
        "end": str(end_date),
    }

    try:
        cohort_rows = [_serialize_row(r) for r in _run_query(COHORT_SQL, params)]
        non_cohort_rows = [_serialize_row(r) for r in _run_query(NON_COHORT_SQL, params)]

        return {
            "status": "ok",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "timezone": TZ,
            "cohort": {
                "days": len(cohort_rows),
                "totals": _compute_totals_cohort(cohort_rows),
            },
            "non_cohort": {
                "days": len(non_cohort_rows),
                "totals": _compute_totals_non_cohort(non_cohort_rows),
            },
        }

    except Exception as e:
        print(f"[REPORT] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))