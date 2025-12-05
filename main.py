from fastapi import FastAPI
from contextlib import asynccontextmanager
import asyncio

from postback_router import router as postback_router
from resolver_router import router as resolver_router
from keytaro import startup_event, shutdown_event, campaign_router  # Импортируем роутер
from db import DataBase

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
    asyncio.create_task(startup_event())

    yield

    # Shutdown
    print("🛑 Остановка приложения...")
    await shutdown_event()

    # Закрываем все соединения с БД
    if db_instance:
        db_instance.close_all_connections()
        print("✓ Connection pool закрыт")

# Создаем FastAPI приложение с lifespan
app = FastAPI(
    title="Deeplink Service + Keitaro Integration",
    description="Сервис для резолва диплинков и интеграции с Keitaro",
    version="2.0.0",
    lifespan=lifespan
)

# Подключаем роутеры
app.include_router(postback_router, prefix="/postback", tags=["postbacks"])
app.include_router(resolver_router, prefix="/resolve", tags=["resolver"])
app.include_router(campaign_router, prefix="/api",
                   tags=["campaigns"])  # Добавляем роутер кампаний


@app.get("/", tags=["main"])
async def root():
    return {
        "message": "Deeplink Service + Keitaro Integration v2.0",
        "features": [
            "Резолв UUID из диплинков",
            "Постбэки от Keitaro",
            "Автоматическая синхронизация кампаний",
            "Фоновая обработка данных",
            "Connection pooling для надежности"
        ],
        "improvements": [
            "✓ Connection pooling вместо одного соединения",
            "✓ Автоматическое переподключение при сбоях",
            "✓ Защита от 'connection already closed' ошибок"
        ]
    }


@app.get("/health", tags=["main"])
async def health_check():
    """
    Проверка здоровья сервиса
    """
    try:
        db = DataBase()
        stats = db.get_detailed_users_stats()

        return {
            "status": "healthy",
            "database": "connected",
            "connection_type": "pooled",
            "stats": stats
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "database": "error",
            "error": str(e)
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
