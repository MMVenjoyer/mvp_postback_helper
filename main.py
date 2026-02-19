from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import asyncio

from postback_router import router as postback_router
from resolver_router import router as resolver_router
from miniapp_router import router as miniapp_router  # NEW
from keytaro import startup_event, shutdown_event, campaign_router
from db import DataBase
from logger_bot import close_bot, send_success_log
from config import ENABLE_TELEGRAM_LOGS

# Глобальный экземпляр БД для graceful shutdown
db_instance = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения
    """
    global db_instance

    # Startup
    print("🚀 Запуск приложения...")

    # Создаем экземпляр БД для проверки соединения
    try:
        db_instance = DataBase()
        print("✓ Connection pool инициализирован")
    except Exception as e:
        print(f"✗ Ошибка инициализации БД: {e}")
        raise

    # Запускаем фоновый сервис синхронизации кампаний
    # asyncio.create_task(startup_event())

    # Отправляем уведомление о старте в Telegram (если включено)
    if ENABLE_TELEGRAM_LOGS:
        try:
            await send_success_log(
                log_type="SERVICE_STARTED",
                message="✅ Сервис Keitaro Postback успешно запущен",
                additional_info={
                    "version": "2.1.0",
                    "features": "Postbacks + Telegram Logger + MiniApp Tracker"
                }
            )
        except Exception as e:
            print(
                f"⚠️ Не удалось отправить уведомление о старте в Telegram: {e}")

    yield

    # Shutdown
    print("🛑 Остановка приложения...")

    # Отправляем уведомление о завершении в Telegram (если включено)
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

    await shutdown_event()

    # Закрываем все соединения с БД
    if db_instance:
        db_instance.close_all_connections()
        print("✓ Connection pool закрыт")

    # Закрываем сессию Telegram бота
    await close_bot()


# Создаем FastAPI приложение с lifespan
app = FastAPI(
    title="Deeplink Service + Keitaro Integration + Telegram Logger + MiniApp",
    description="Сервис для резолва диплинков, интеграции с Keitaro, автоматической отправки логов ошибок в Telegram и трекинга Mini App",
    version="2.1.0",
    lifespan=lifespan
)

# CORS для Mini App (если будет на другом домене)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В проде лучше указать конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Подключаем роутеры
app.include_router(postback_router, prefix="/postback", tags=["postbacks"])
app.include_router(resolver_router, prefix="/resolve", tags=["resolver"])
app.include_router(campaign_router, prefix="/api", tags=["campaigns"])
app.include_router(miniapp_router, prefix="/api", tags=["miniapp"])  # NEW


@app.get("/", tags=["main"])
async def root():
    return {
        "message": "Deeplink Service + Keitaro Integration + Telegram Logger + MiniApp v2.1",
        "features": [
            "Резолв UUID из диплинков",
            "Постбэки от Keitaro",
            "Автоматическая синхронизация кампаний",
            "Фоновая обработка данных",
            "Connection pooling для надежности",
            "Telegram Logger для ошибок",
            "🆕 Трекинг открытий Mini App калькулятора"
        ],
        "endpoints": {
            "miniapp_track": "POST /api/get_miniapp",
            "miniapp_stats": "GET /api/calc_stats"
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
