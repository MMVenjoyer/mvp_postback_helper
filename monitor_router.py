"""
Monitoring Router v1.0

Эндпоинты для мониторинга сервиса:
- GET /api/monitor/health - полная проверка здоровья
- GET /api/monitor/logs - последние логи (фильтры по level/category)
- GET /api/monitor/logs/stats - статистика по логам
- GET /api/monitor/queue - статус очереди постбэков
- GET /api/monitor/keitaro - статус Keitaro + история
- POST /api/monitor/cleanup - очистка старых логов

Все эндпоинты защищены X-API-Key.
"""

from fastapi import APIRouter, Query, HTTPException, Header
from typing import Optional

from db import DataBase
from config import REPORT_API_KEY

router = APIRouter()
db = DataBase()


def verify_api_key(x_api_key: str):
    if not REPORT_API_KEY:
        raise HTTPException(status_code=500, detail="REPORT_API_KEY not configured")
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key")
    if x_api_key != REPORT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")


@router.get("/health")
async def full_health_check(x_api_key: str = Header(None, alias="X-API-Key")):
    """Полная проверка здоровья сервиса"""
    verify_api_key(x_api_key)

    from service_monitor import keitaro_monitor, keitaro_rate_limiter
    from postback_queue import postback_queue
    from config import ENABLE_TELEGRAM_LOGS

    try:
        # DB check
        db_ok = False
        try:
            stats = db.get_detailed_users_stats()
            db_ok = True
        except Exception:
            stats = {}

        # Queue stats
        try:
            queue_stats = postback_queue.get_stats()
        except Exception:
            queue_stats = {"error": "unavailable"}

        # Service log stats (last 24h)
        try:
            log_stats = db.get_service_log_stats(hours=24)
        except Exception:
            log_stats = {"error": "unavailable"}

        return {
            "status": "healthy" if db_ok else "degraded",
            "components": {
                "database": "ok" if db_ok else "error",
                "keitaro": keitaro_monitor.status,
                "telegram_logs": "enabled" if ENABLE_TELEGRAM_LOGS else "disabled",
                "rate_limiter_tokens": round(keitaro_rate_limiter.available_tokens, 1),
            },
            "queue": queue_stats,
            "log_stats_24h": log_stats,
            "user_stats": stats,
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/logs")
async def get_logs(
    limit: int = Query(50, ge=1, le=500),
    level: Optional[str] = Query(None, description="ERROR, WARNING, INFO, etc."),
    category: Optional[str] = Query(None, description="KEITARO, CHATTERFY, POSTBACK, etc."),
    hours: int = Query(24, ge=1, le=168),
    x_api_key: str = Header(None, alias="X-API-Key"),
):
    """Последние логи сервиса"""
    verify_api_key(x_api_key)

    try:
        logs = db.get_service_logs(limit=limit, level=level, category=category, hours=hours)
        return {
            "status": "ok",
            "count": len(logs),
            "filters": {"level": level, "category": category, "hours": hours},
            "logs": logs,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/logs/stats")
async def get_log_stats(
    hours: int = Query(24, ge=1, le=168),
    x_api_key: str = Header(None, alias="X-API-Key"),
):
    """Статистика по логам"""
    verify_api_key(x_api_key)

    try:
        stats = db.get_service_log_stats(hours=hours)
        return {"status": "ok", "hours": hours, "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/queue")
async def get_queue_status(
    x_api_key: str = Header(None, alias="X-API-Key"),
):
    """Статус очереди постбэков"""
    verify_api_key(x_api_key)

    from postback_queue import postback_queue

    try:
        stats = postback_queue.get_stats()

        # Последние элементы очереди
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, target, action, user_id, status, attempts, 
                           last_error, next_retry_at, created_at
                    FROM postback_queue
                    ORDER BY created_at DESC
                    LIMIT 20
                """)
                rows = cursor.fetchall()

                items = [{
                    "id": r[0], "target": r[1], "action": r[2],
                    "user_id": r[3], "status": r[4], "attempts": r[5],
                    "last_error": r[6],
                    "next_retry_at": r[7].isoformat() if r[7] else None,
                    "created_at": r[8].isoformat() if r[8] else None,
                } for r in rows]

        return {
            "status": "ok",
            "stats": stats,
            "recent_items": items,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/keitaro")
async def get_keitaro_status(
    hours: int = Query(24, ge=1, le=168),
    x_api_key: str = Header(None, alias="X-API-Key"),
):
    """Статус Keitaro + история health checks"""
    verify_api_key(x_api_key)

    from service_monitor import keitaro_monitor

    try:
        history = db.get_health_check_history(target="keitaro", hours=hours)

        # Считаем uptime
        ok_count = sum(1 for h in history if h["status"] == "ok")
        total = len(history)
        uptime_pct = round(ok_count / total * 100, 1) if total > 0 else None

        # Средний response time
        response_times = [h["response_ms"] for h in history if h.get("response_ms")]
        avg_ms = round(sum(response_times) / len(response_times)) if response_times else None

        return {
            "status": "ok",
            "current": keitaro_monitor.status,
            "uptime_percent": uptime_pct,
            "avg_response_ms": avg_ms,
            "total_checks": total,
            "history": history[:50],  # Последние 50
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.post("/cleanup")
async def cleanup_old_data(
    days: int = Query(30, ge=7, le=90),
    x_api_key: str = Header(None, alias="X-API-Key"),
):
    """Очистка старых логов и завершённых элементов очереди"""
    verify_api_key(x_api_key)

    try:
        result = db.cleanup_old_logs(days=days)
        return {"status": "ok", "cleaned": result}
    except Exception as e:
        return {"status": "error", "error": str(e)}
