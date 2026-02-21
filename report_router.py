"""
Report Router — воронка продаж: когортный и некогортный анализ

Все эндпоинты защищены заголовком X-API-Key.

Эндпоинты:
- GET /api/report/funnel?type=cohort&start_date=2026-02-10&end_date=2026-02-22
- GET /api/report/funnel?type=non_cohort&start_date=2026-02-10&end_date=2026-02-22
- GET /api/report/funnel/summary?start_date=2026-02-10&end_date=2026-02-22
- GET /api/report/trader_ids

Когортный: группировка по дню joined_bot_time, конверсии внутри когорты
Некогортный: события по дню их фактического наступления

v2.4: Добавлены поля dep/redep/dep_sum/redep_sum/total_deposits/revenue
      dep и redep считаются отдельно из transactions
      revenue берётся как последнее значение на юзера (DISTINCT ON)

Таймзона: Europe/Berlin (UTC+1)
"""

from fastapi import APIRouter, Query, HTTPException, Header
from typing import Optional, List, Dict, Any
from datetime import date, datetime
from enum import Enum

from db import DataBase
from config import REPORT_API_KEY

router = APIRouter()
db = DataBase()

TZ = "Europe/Berlin"


class ReportType(str, Enum):
    cohort = "cohort"
    non_cohort = "non_cohort"


# ==========================================
# АВТОРИЗАЦИЯ
# ==========================================

def verify_api_key(x_api_key: str):
    """
    Проверяет API ключ из заголовка X-API-Key.
    Если REPORT_API_KEY не задан в .env — все запросы блокируются.
    """
    if not REPORT_API_KEY:
        raise HTTPException(status_code=500, detail="REPORT_API_KEY not configured on server")

    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")

    if x_api_key != REPORT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


# ==========================================
# УТИЛИТЫ
# ==========================================

def _run_query(query: str, params: dict) -> List[Dict[str, Any]]:
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
# SQL ЗАПРОСЫ
# ==========================================

# ---------- КОГОРТНЫЙ ----------
# Группируем по дню joined_bot_time.
# dep/redep берём из transactions (кол-во и суммы).
# revenue — последнее значение на юзера из transactions.
COHORT_SQL = """
WITH base AS (
    SELECT
        id,
        date_trunc('day', joined_bot_time AT TIME ZONE %(tz)s) AS cohort_day,
        joined_main_time,
        ftm_time,
        reg_time,
        dep_time,
        redep_time
    FROM users
    WHERE joined_bot_time IS NOT NULL
      AND (joined_bot_time AT TIME ZONE %(tz)s)::date >= %(start)s::date
      AND (joined_bot_time AT TIME ZONE %(tz)s)::date <= %(end)s::date
),
user_tx AS (
    SELECT
        t.user_id,
        COALESCE(SUM(t.sum) FILTER (WHERE t.action = 'dep'),  0) AS dep_sum,
        COALESCE(SUM(t.sum) FILTER (WHERE t.action = 'redep'), 0) AS redep_sum,
        COALESCE(SUM(t.sum) FILTER (WHERE t.action IN ('dep','redep')), 0) AS total_deposits
    FROM transactions t
    WHERE t.user_id IN (SELECT id FROM base)
      AND t.action IN ('dep', 'redep')
    GROUP BY t.user_id
),
user_revenue AS (
    SELECT DISTINCT ON (t.user_id)
        t.user_id,
        t.sum AS revenue
    FROM transactions t
    WHERE t.user_id IN (SELECT id FROM base)
      AND t.action = 'revenue'
    ORDER BY t.user_id, t.created_at DESC
),
combined AS (
    SELECT
        b.cohort_day,
        b.id,
        b.joined_main_time,
        b.ftm_time,
        b.reg_time,
        b.dep_time,
        b.redep_time,
        COALESCE(tx.dep_sum, 0)        AS dep_sum,
        COALESCE(tx.redep_sum, 0)      AS redep_sum,
        COALESCE(tx.total_deposits, 0) AS total_deposits,
        COALESCE(rv.revenue, 0)        AS revenue
    FROM base b
    LEFT JOIN user_tx      tx ON tx.user_id = b.id
    LEFT JOIN user_revenue rv ON rv.user_id = b.id
)
SELECT
    cohort_day::date                                      AS day,
    COUNT(*)                                              AS total,
    COUNT(*) FILTER (WHERE joined_main_time IS NOT NULL)  AS main,
    ROUND(COUNT(*) FILTER (WHERE joined_main_time IS NOT NULL)::decimal
        / NULLIF(COUNT(*), 0) * 100, 2)                  AS conv_bot_to_main,

    COUNT(*) FILTER (WHERE ftm_time IS NOT NULL)          AS ftm,
    ROUND(COUNT(*) FILTER (WHERE ftm_time IS NOT NULL)::decimal
        / NULLIF(COUNT(*) FILTER (WHERE joined_main_time IS NOT NULL), 0) * 100, 2) AS conv_main_to_ftm,

    COUNT(*) FILTER (WHERE reg_time IS NOT NULL)          AS reg,
    ROUND(COUNT(*) FILTER (WHERE reg_time IS NOT NULL)::decimal
        / NULLIF(COUNT(*) FILTER (WHERE ftm_time IS NOT NULL), 0) * 100, 2) AS conv_ftm_to_reg,

    COUNT(*) FILTER (WHERE dep_time IS NOT NULL)          AS dep,
    ROUND(COUNT(*) FILTER (WHERE dep_time IS NOT NULL)::decimal
        / NULLIF(COUNT(*) FILTER (WHERE reg_time IS NOT NULL), 0) * 100, 2) AS conv_reg_to_dep,

    COUNT(*) FILTER (WHERE redep_time IS NOT NULL)        AS redep,

    SUM(dep_sum)::numeric                                 AS dep_sum,
    SUM(redep_sum)::numeric                               AS redep_sum,
    SUM(total_deposits)::numeric                          AS total_deposits,
    SUM(revenue)::numeric                                 AS revenue,

    ROUND(COUNT(*) FILTER (WHERE dep_time IS NOT NULL)::decimal
        / NULLIF(COUNT(*), 0) * 100, 2)                  AS full_funnel
FROM combined
GROUP BY cohort_day
ORDER BY cohort_day
"""

# ---------- НЕКОГОРТНЫЙ ----------
# События считаются по дню их фактического наступления.
# reg/dep/redep берутся из transactions (по created_at).
# revenue — последнее на юзера (DISTINCT ON), привязано к дню транзакции.
NON_COHORT_SQL = """
WITH events AS (
    -- joined_bot
    SELECT (u.joined_bot_time AT TIME ZONE %(tz)s)::date AS day,
           'new_users' AS evt, 0::numeric AS amount
    FROM users u
    WHERE u.joined_bot_time IS NOT NULL

    UNION ALL
    -- joined_main
    SELECT (u.joined_main_time AT TIME ZONE %(tz)s)::date,
           'joined_main', 0
    FROM users u
    WHERE u.joined_main_time IS NOT NULL

    UNION ALL
    -- ftm
    SELECT (u.ftm_time AT TIME ZONE %(tz)s)::date,
           'ftm', 0
    FROM users u
    WHERE u.ftm_time IS NOT NULL

    UNION ALL
    -- reg (из transactions)
    SELECT (t.created_at AT TIME ZONE %(tz)s)::date,
           'reg', 0
    FROM transactions t
    WHERE t.action = 'reg'

    UNION ALL
    -- dep (из transactions)
    SELECT (t.created_at AT TIME ZONE %(tz)s)::date,
           'dep', COALESCE(t.sum, 0)
    FROM transactions t
    WHERE t.action = 'dep'

    UNION ALL
    -- redep (из transactions)
    SELECT (t.created_at AT TIME ZONE %(tz)s)::date,
           'redep', COALESCE(t.sum, 0)
    FROM transactions t
    WHERE t.action = 'redep'

    UNION ALL
    -- revenue (последнее значение на юзера)
    SELECT (t.created_at AT TIME ZONE %(tz)s)::date,
           'revenue', COALESCE(t.sum, 0)
    FROM (
        SELECT DISTINCT ON (user_id) user_id, created_at, sum
        FROM transactions
        WHERE action = 'revenue'
        ORDER BY user_id, created_at DESC
    ) t
)
SELECT
    day,
    COUNT(*) FILTER (WHERE evt = 'new_users')   AS new_users,
    COUNT(*) FILTER (WHERE evt = 'joined_main')  AS joined_main,
    COUNT(*) FILTER (WHERE evt = 'ftm')          AS ftm,
    COUNT(*) FILTER (WHERE evt = 'reg')          AS reg,
    COUNT(*) FILTER (WHERE evt = 'dep')          AS dep,
    COUNT(*) FILTER (WHERE evt = 'redep')        AS redep,
    COALESCE(SUM(amount) FILTER (WHERE evt = 'dep'),   0) AS dep_sum,
    COALESCE(SUM(amount) FILTER (WHERE evt = 'redep'), 0) AS redep_sum,
    COALESCE(SUM(amount) FILTER (WHERE evt = 'dep'),   0)
      + COALESCE(SUM(amount) FILTER (WHERE evt = 'redep'), 0) AS total_deposits,
    COALESCE(SUM(amount) FILTER (WHERE evt = 'revenue'), 0) AS revenue
FROM events
WHERE day BETWEEN %(start)s AND %(end)s
GROUP BY day
ORDER BY day
"""


# ==========================================
# TOTALS
# ==========================================

def _compute_totals_cohort(rows: List[Dict]) -> Dict[str, Any]:
    t = {
        "day": "total",
        "total": 0, "main": 0, "ftm": 0, "reg": 0, "dep": 0, "redep": 0,
        "dep_sum": 0.0, "redep_sum": 0.0, "total_deposits": 0.0, "revenue": 0.0,
    }
    for r in rows:
        t["total"]          += r.get("total", 0) or 0
        t["main"]           += r.get("main", 0) or 0
        t["ftm"]            += r.get("ftm", 0) or 0
        t["reg"]            += r.get("reg", 0) or 0
        t["dep"]            += r.get("dep", 0) or 0
        t["redep"]          += r.get("redep", 0) or 0
        t["dep_sum"]        += float(r.get("dep_sum", 0) or 0)
        t["redep_sum"]      += float(r.get("redep_sum", 0) or 0)
        t["total_deposits"] += float(r.get("total_deposits", 0) or 0)
        t["revenue"]        += float(r.get("revenue", 0) or 0)

    def pct(a, b):
        return round(a / b * 100, 2) if b else None

    t["conv_bot_to_main"] = pct(t["main"], t["total"])
    t["conv_main_to_ftm"] = pct(t["ftm"], t["main"])
    t["conv_ftm_to_reg"]  = pct(t["reg"], t["ftm"])
    t["conv_reg_to_dep"]  = pct(t["dep"], t["reg"])
    t["full_funnel"]      = pct(t["dep"], t["total"])

    # Округляем суммы
    t["dep_sum"]        = round(t["dep_sum"], 2)
    t["redep_sum"]      = round(t["redep_sum"], 2)
    t["total_deposits"] = round(t["total_deposits"], 2)
    t["revenue"]        = round(t["revenue"], 2)

    return t


def _compute_totals_non_cohort(rows: List[Dict]) -> Dict[str, Any]:
    t = {
        "day": "total",
        "new_users": 0, "joined_main": 0, "ftm": 0, "reg": 0,
        "dep": 0, "redep": 0,
        "dep_sum": 0.0, "redep_sum": 0.0, "total_deposits": 0.0, "revenue": 0.0,
    }
    for r in rows:
        t["new_users"]      += r.get("new_users", 0) or 0
        t["joined_main"]    += r.get("joined_main", 0) or 0
        t["ftm"]            += r.get("ftm", 0) or 0
        t["reg"]            += r.get("reg", 0) or 0
        t["dep"]            += r.get("dep", 0) or 0
        t["redep"]          += r.get("redep", 0) or 0
        t["dep_sum"]        += float(r.get("dep_sum", 0) or 0)
        t["redep_sum"]      += float(r.get("redep_sum", 0) or 0)
        t["total_deposits"] += float(r.get("total_deposits", 0) or 0)
        t["revenue"]        += float(r.get("revenue", 0) or 0)

    t["dep_sum"]        = round(t["dep_sum"], 2)
    t["redep_sum"]      = round(t["redep_sum"], 2)
    t["total_deposits"] = round(t["total_deposits"], 2)
    t["revenue"]        = round(t["revenue"], 2)

    return t


# ==========================================
# ЭНДПОИНТЫ
# ==========================================

@router.get("/funnel")
async def get_funnel_report(
    type: ReportType = Query(..., description="Тип отчёта: cohort или non_cohort"),
    start_date: date = Query(..., description="Начало диапазона (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Конец диапазона (YYYY-MM-DD)"),
    x_api_key: str = Header(None, alias="X-API-Key")
):
    """
    Воронка продаж — когортный или некогортный анализ по дням.

    Поля ответа (cohort):
      day, total, main, conv_bot_to_main, ftm, conv_main_to_ftm,
      reg, conv_ftm_to_reg, dep, conv_reg_to_dep, redep,
      dep_sum, redep_sum, total_deposits, revenue, full_funnel

    Поля ответа (non_cohort):
      day, new_users, joined_main, ftm, reg, dep, redep,
      dep_sum, redep_sum, total_deposits, revenue

    Требуется заголовок: X-API-Key
    """
    verify_api_key(x_api_key)

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    params = {"tz": TZ, "start": str(start_date), "end": str(end_date)}

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

    except HTTPException:
        raise
    except Exception as e:
        print(f"[REPORT] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/trader_ids")
async def get_all_trader_ids(
    x_api_key: str = Header(None, alias="X-API-Key")
):
    """
    Список всех trader_id из базы.
    Требуется заголовок: X-API-Key
    """
    verify_api_key(x_api_key)

    try:
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

    except HTTPException:
        raise
    except Exception as e:
        print(f"[REPORT] ✗ Exception in trader_ids: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/funnel/summary")
async def get_funnel_summary(
    start_date: date = Query(..., description="Начало диапазона (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Конец диапазона (YYYY-MM-DD)"),
    x_api_key: str = Header(None, alias="X-API-Key")
):
    """
    Когортный vs некогортный за один запрос.
    Требуется заголовок: X-API-Key
    """
    verify_api_key(x_api_key)

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    params = {"tz": TZ, "start": str(start_date), "end": str(end_date)}

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

    except HTTPException:
        raise
    except Exception as e:
        print(f"[REPORT] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))