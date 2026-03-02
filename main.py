from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio

from postback_router import router as postback_router
from resolver_router import router as resolver_router
from miniapp_router import router as miniapp_router
from report_router import router as report_router
from monitor_router import router as monitor_router
from keytaro import startup_event, shutdown_event, campaign_router
from db import DataBase
from logger_bot import close_bot, send_success_log
from api_request import close_http_session
from service_logger import slog
from postback_queue import postback_queue
from service_monitor import keitaro_monitor
from config import ENABLE_TELEGRAM_LOGS

# Глобальный экземпляр БД для graceful shutdown
db_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения
    """
    global db_instance

    # ==========================================
    # STARTUP
    # ==========================================
    print("🚀 Запуск приложения...")

    # 1. Создаем экземпляр БД для проверки соединения
    try:
        db_instance = DataBase()
        print("✓ Connection pool инициализирован")
    except Exception as e:
        print(f"✗ Ошибка инициализации БД: {e}")
        raise

    # 2. Запускаем фоновые воркеры
    slog.start_worker()
    postback_queue.start_worker()
    keitaro_monitor.start_worker()

    # 3. Запускаем фоновый сервис синхронизации кампаний (если нужно)
    # asyncio.create_task(startup_event())

    # 4. Отправляем уведомление о старте в Telegram
    if ENABLE_TELEGRAM_LOGS:
        try:
            await send_success_log(
                log_type="SERVICE_STARTED",
                message="✅ Сервис Keitaro Postback успешно запущен",
                additional_info={
                    "version": "2.6.0",
                    "features": "Postbacks + Telegram Logger + MiniApp + Parallel Sends + Reports + Monitoring + Queue + Promo"
                }
            )
        except Exception as e:
            print(
                f"⚠️ Не удалось отправить уведомление о старте в Telegram: {e}")

    yield

    # ==========================================
    # SHUTDOWN
    # ==========================================
    print("🛑 Остановка приложения...")

    # Отправляем уведомление о завершении в Telegram
    if ENABLE_TELEGRAM_LOGS:
        try:
            await send_success_log(
                log_type="SERVICE_STOPPED",
                message="🛑 Сервис Keitaro Postback остановлен",
                additional_info={
                    "reason": "Graceful shutdown"
                }
            )
        except Exception as e:
            print(
                f"⚠️ Не удалось отправить уведомление о завершении в Telegram: {e}")

    # Останавливаем кампанийный сервис
    await shutdown_event()

    # Останавливаем фоновые воркеры (в обратном порядке)
    await keitaro_monitor.stop_worker()
    await postback_queue.stop_worker()
    await slog.stop_worker()

    # Закрываем shared HTTP сессию
    await close_http_session()

    # Закрываем все соединения с БД
    if db_instance:
        db_instance.close_all_connections()
        print("✓ Connection pool закрыт")

    # Закрываем сессию Telegram бота
    await close_bot()


# Создаем FastAPI приложение с lifespan
app = FastAPI(
    title="Deeplink Service + Keitaro Integration + Monitoring v2.6",
    description="Сервис для резолва диплинков, интеграции с Keitaro, логирования, мониторинга, очереди retry и отчётов воронки",
    version="2.6.0",
    lifespan=lifespan
)

# CORS для Mini App (если будет на другом домене)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутеры
app.include_router(postback_router, prefix="/postback", tags=["postbacks"])
app.include_router(resolver_router, prefix="/resolve", tags=["resolver"])
app.include_router(campaign_router, prefix="/api", tags=["campaigns"])
app.include_router(miniapp_router, prefix="/api", tags=["miniapp"])
app.include_router(report_router, prefix="/api/report", tags=["reports"])
app.include_router(monitor_router, prefix="/api/monitor", tags=["monitoring"])


@app.get("/", tags=["main"])
async def root():
    return {
        "message": "Deeplink Service + Keitaro Integration v2.6",
        "features": [
            "Резолв UUID из диплинков",
            "Постбэки от Keitaro",
            "Автоматическая синхронизация кампаний",
            "Фоновая обработка данных",
            "Connection pooling для надежности",
            "Telegram Logger для ошибок",
            "Трекинг открытий Mini App калькулятора",
            "Параллельная отправка постбэков (v2.2)",
            "Shared HTTP session (v2.2)",
            "Отчёты воронки: когортный + некогортный (v2.3)",
            "🆕 Service Logger + Postback Queue + Health Monitor (v2.6)",
            "🆕 Promo в dep/redep + transactions (v2.6)",
            "🆕 Monitoring API /api/monitor/* (v2.6)",
        ],
        "endpoints": {
            "miniapp_track": "POST /api/get_miniapp",
            "miniapp_stats": "GET /api/calc_stats",
            "funnel_report": "GET /api/report/funnel?type=cohort&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD",
            "funnel_summary": "GET /api/report/funnel/summary?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD",
            "monitor_health": "GET /api/monitor/health",
            "monitor_logs": "GET /api/monitor/logs",
            "monitor_queue": "GET /api/monitor/queue",
            "monitor_keitaro": "GET /api/monitor/keitaro",
        }
    }


@app.get("/health", tags=["main"])
async def health_check():
    """
    Проверка здоровья сервиса
    """
    try:
        db = DataBase()
        stats = db.get_detailed_users_stats()
        calc_stats = db.get_calc_open_stats()

        return {
            "status": "healthy",
            "database": "connected",
            "connection_type": "pooled",
            "telegram_logs": "enabled" if ENABLE_TELEGRAM_LOGS else "disabled",
            "keitaro_healthy": keitaro_monitor.is_healthy,
            "stats": stats,
            "calc_stats": calc_stats
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    from config import API_HOST, API_PORT
    uvicorn.run("main:app", host=API_HOST, port=API_PORT, reload=True)