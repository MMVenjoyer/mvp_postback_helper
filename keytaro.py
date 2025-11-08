from fastapi import APIRouter, BackgroundTasks
import asyncio
import httpx
from typing import List, Dict, Any, Optional
import time
import logging
from datetime import datetime, timedelta
from db import DataBase
from config import (
    KEITARO_DOMAIN,
    KEITARO_ADMIN_API_KEY,
    MAX_USERS_PER_SECOND,
    DELAY_BETWEEN_REQUESTS,
    AUTO_CHECK_INTERVAL
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = DataBase()

MAX_USERS_PER_SECOND = 2
DELAY_BETWEEN_REQUESTS = 0.5
BATCH_SIZE = 10
AUTO_CHECK_INTERVAL = 3600  # 60 минут = 3600 секунд


class KeitaroCampaignService:
    def __init__(self):
        self.session = None
        self.is_running = False

    async def __aenter__(self):
        self.session = httpx.AsyncClient(timeout=30.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.aclose()

    def get_users_for_processing(self) -> List[Dict[str, Any]]:
        """
        ОБНОВЛЕНО: Получает пользователей с sub_id для обработки
        """
        try:
            users = db.get_users_with_null_campaign_landing_data()
            logger.info(
                f"Найдено {len(users)} пользователей для обработки (с sub_id)")
            return users
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []

    async def get_conversion_data(self, sub_id: str) -> Dict[str, Any]:
        """
        Получает данные конверсии из Keitaro API по sub_id
        """
        headers = {
            "Api-Key": KEITARO_ADMIN_API_KEY,
            "Content-Type": "application/json"
        }

        payload = {
            "limit": 1,
            "columns": [
                "campaign_id",
                "campaign",
                "landing_id",
                "landing",
                # Используем country_flag вместо country для получения кода (US вместо United States)
                "country_flag"
            ],
            "filters": [
                {
                    "name": "sub_id",
                    "operator": "EQUALS",
                    "expression": sub_id
                }
            ]
        }

        try:
            response = await self.session.post(
                f"{KEITARO_DOMAIN}/admin_api/v1/conversions/log",
                headers=headers,
                json=payload
            )

            if response.status_code == 200:
                data = response.json()

                # Keitaro возвращает rows как массив объектов
                if data.get("rows") and len(data["rows"]) > 0:
                    row = data["rows"][0]  # Берем первую запись

                    # row - это уже объект с ключами
                    return {
                        "campaign_id": row.get("campaign_id"),
                        "campaign": row.get("campaign"),
                        "landing_id": row.get("landing_id"),
                        "landing": row.get("landing"),
                        # country_flag содержит код страны (US, AE, etc.)
                        "country": row.get("country_flag"),
                        "found": True
                    }
                else:
                    return {"found": False, "reason": "No data in response"}
            else:
                logger.warning(
                    f"API error for sub_id {sub_id}: {response.status_code}")
                return {"found": False, "reason": f"API error: {response.status_code}"}

        except Exception as e:
            logger.error(f"Request error for sub_id {sub_id}: {e}")
            return {"found": False, "reason": str(e)}

    async def get_country_by_user_id(self, user_id: int) -> Dict[str, Any]:
        """
        ОБНОВЛЕНО: Получает страну пользователя через sub_id из БД
        """
        try:
            # Сначала проверяем страну в БД
            country_from_db = db.get_user_country(user_id)
            if country_from_db and country_from_db != 'None':
                logger.info(
                    f"Страна для пользователя {user_id} найдена в БД: {country_from_db}")
                return {
                    "user_id": user_id,
                    "country": country_from_db,
                    "source": "database",
                    "found": True
                }

            # Если в БД нет, получаем sub_id и запрашиваем из Keitaro
            sub_id = db.get_user_sub_id(user_id)
            if not sub_id:
                return {
                    "user_id": user_id,
                    "country": None,
                    "found": False,
                    "reason": "sub_id not found in database"
                }

            logger.info(
                f"Страна для пользователя {user_id} не найдена в БД, запрашиваем из Keitaro по sub_id: {sub_id}")
            conversion_data = await self.get_conversion_data(sub_id)

            if conversion_data.get('found'):
                country = conversion_data.get('country')
                if country:
                    # Сохраняем в БД для будущих запросов
                    db.update_user_campaign_landing_data(
                        user_id,
                        country=country
                    )
                    logger.info(
                        f"Страна для пользователя {user_id} получена из Keitaro и сохранена: {country}")
                    return {
                        "user_id": user_id,
                        "country": country,
                        "source": "keitaro",
                        "found": True
                    }
                else:
                    return {
                        "user_id": user_id,
                        "country": None,
                        "source": "keitaro",
                        "found": False,
                        "reason": "Country field is empty in Keitaro"
                    }
            else:
                return {
                    "user_id": user_id,
                    "country": None,
                    "source": "keitaro",
                    "found": False,
                    "reason": conversion_data.get('reason', 'Unknown error')
                }

        except Exception as e:
            logger.error(
                f"Ошибка получения страны для пользователя {user_id}: {e}")
            return {
                "user_id": user_id,
                "country": None,
                "found": False,
                "error": str(e)
            }

    async def process_users_slowly(self, users: List[Dict[str, Any]]):
        """
        ОБНОВЛЕНО: Обрабатывает пользователей используя sub_id из БД
        """
        total_users = len(users)
        processed = 0
        successful = 0
        failed = 0
        skipped = 0

        logger.info(f"Начинаем обработку {total_users} пользователей")
        logger.info(f"Скорость: {MAX_USERS_PER_SECOND} запросов в секунду")

        for user in users:
            if not self.is_running:
                logger.info("Обработка остановлена")
                break

            user_id = user['user_id']
            # Получаем sub_id из результата запроса
            sub_id = user.get('sub_id')

            if not sub_id:
                logger.warning(
                    f"⚠️ Пропускаем пользователя {user_id}: отсутствует sub_id")
                failed += 1
                processed += 1
                continue

            logger.info(
                f"Обрабатываем пользователя {user_id} (sub_id: {sub_id})")

            try:
                # Получаем данные из Keitaro используя sub_id
                conversion_data = await self.get_conversion_data(sub_id)

                if conversion_data.get('found'):
                    # Найдены данные - обновляем БД реальными данными
                    result = db.update_user_campaign_landing_data(
                        user_id,
                        company=conversion_data.get('campaign'),
                        company_id=conversion_data.get('campaign_id'),
                        landing=conversion_data.get('landing'),
                        landing_id=conversion_data.get('landing_id'),
                        country=conversion_data.get('country')
                    )

                    if result.get('success'):
                        successful += 1
                        logger.info(
                            f"✓ Обновлен {user_id}: кампания={conversion_data.get('campaign')}, лендинг={conversion_data.get('landing')}, страна={conversion_data.get('country')}")
                    else:
                        failed += 1
                        logger.error(
                            f"✗ Ошибка обновления {user_id}: {result.get('error')}")
                else:
                    # Данные не найдены - ПОМЕЧАЕМ маркерами
                    result = db.update_user_campaign_landing_data(
                        user_id,
                        company="None",
                        company_id=-1,
                        landing="None",
                        landing_id=-1,
                        country="None"
                    )

                    if result.get('success'):
                        skipped += 1
                        logger.info(
                            f"⊘ Помечен как обработанный без данных {user_id}: {conversion_data.get('reason')}")
                    else:
                        failed += 1
                        logger.error(
                            f"✗ Ошибка пометки {user_id}: {result.get('error')}")

            except Exception as e:
                failed += 1
                logger.error(f"✗ Исключение при обработке {user_id}: {e}")

            processed += 1

            if processed % 10 == 0 or processed == total_users:
                progress = (processed / total_users) * 100
                logger.info(
                    f"Прогресс: {processed}/{total_users} ({progress:.1f}%)")

            if processed < total_users:
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

        logger.info(f"\nОбработка завершена:")
        logger.info(f"  Всего: {total_users}")
        logger.info(f"  Обработано: {processed}")
        logger.info(f"  Найдены данные: {successful}")
        logger.info(f"  Помечено как пустые: {skipped}")
        logger.info(f"  Ошибки: {failed}")

        return {
            "total": total_users,
            "processed": processed,
            "successful": successful,
            "skipped": skipped,
            "failed": failed
        }

    async def startup_campaign_sync(self):
        """
        Стартовая синхронизация при запуске сервиса
        """
        logger.info("=== СТАРТ: Синхронизация данных кампаний и лендингов ===")

        users = self.get_users_for_processing()
        if not users:
            logger.info("Все пользователи уже обработаны")
            return

        self.is_running = True
        result = await self.process_users_slowly(users)
        self.is_running = False

        logger.info("=== ЗАВЕРШЕНО: Синхронизация данных ===")
        return result

    async def auto_check_sync(self):
        """
        Автоматическая проверка каждые 60 минут
        Обрабатывает ТОЛЬКО новых пользователей с NULL полями
        """
        logger.info("=== АВТОПРОВЕРКА (60 мин): Поиск новых пользователей ===")

        users = self.get_users_for_processing()
        if not users:
            logger.info("Нет новых пользователей для обработки")
            return

        logger.info(
            f"Найдено {len(users)} новых пользователей для обработки")

        self.is_running = True
        result = await self.process_users_slowly(users)
        self.is_running = False

        logger.info("=== ЗАВЕРШЕНО: Автопроверка ===")
        return result

    async def get_country_by_sub_id(self, sub_id: str) -> Dict[str, Any]:
        """
        Получает страну по sub_id (формат: luqb8e.3a.4t77)
        """
        try:
            logger.info(f"Запрос страны для sub_id: {sub_id}")
            conversion_data = await self.get_conversion_data(sub_id)

            if conversion_data.get('found'):
                country = conversion_data.get('country')
                if country:
                    logger.info(f"Страна для sub_id {sub_id}: {country}")
                    return {
                        "sub_id": sub_id,
                        "country": country,
                        "campaign": conversion_data.get('campaign'),
                        "landing": conversion_data.get('landing'),
                        "source": "keitaro",
                        "found": True
                    }
                else:
                    return {
                        "sub_id": sub_id,
                        "country": None,
                        "found": False,
                        "reason": "Country field is empty"
                    }
            else:
                return {
                    "sub_id": sub_id,
                    "country": None,
                    "found": False,
                    "reason": conversion_data.get('reason', 'Unknown error')
                }

        except Exception as e:
            logger.error(f"Ошибка получения страны для sub_id {sub_id}: {e}")
            return {
                "sub_id": sub_id,
                "country": None,
                "found": False,
                "error": str(e)
            }

    async def get_full_data_by_sub_id(self, sub_id: str) -> Dict[str, Any]:
        """
        НОВЫЙ: Получает полные данные (кампания, лендинг, страна) по sub_id
        """
        try:
            logger.info(f"Запрос полных данных для sub_id: {sub_id}")
            conversion_data = await self.get_conversion_data(sub_id)

            if conversion_data.get('found'):
                logger.info(f"Данные для sub_id {sub_id} найдены")
                return {
                    "sub_id": sub_id,
                    "campaign": conversion_data.get('campaign'),
                    "campaign_id": conversion_data.get('campaign_id'),
                    "landing": conversion_data.get('landing'),
                    "landing_id": conversion_data.get('landing_id'),
                    "country": conversion_data.get('country'),
                    "found": True
                }
            else:
                return {
                    "sub_id": sub_id,
                    "found": False,
                    "reason": conversion_data.get('reason', 'Unknown error')
                }

        except Exception as e:
            logger.error(f"Ошибка получения данных для sub_id {sub_id}: {e}")
            return {
                "sub_id": sub_id,
                "found": False,
                "error": str(e)
            }


# Глобальный сервис
campaign_service = None
auto_check_task = None


async def start_campaign_service():
    """
    Запускает сервис с автоматической проверкой каждые 60 минут
    """
    global campaign_service, auto_check_task
    campaign_service = KeitaroCampaignService()

    async with campaign_service:
        # Первичная синхронизация при старте
        logger.info("🚀 Запуск первичной синхронизации данных...")
        await campaign_service.startup_campaign_sync()

        # Запускаем бесконечный цикл автопроверки
        logger.info(
            f"🔄 Запуск автоматической проверки каждые {AUTO_CHECK_INTERVAL} секунд (60 минут)")

        while True:
            try:
                logger.info(
                    f"⏰ Ожидание {AUTO_CHECK_INTERVAL} секунд до следующей проверки...")
                await asyncio.sleep(AUTO_CHECK_INTERVAL)

                logger.info("🔍 Запуск автоматической проверки...")
                await campaign_service.auto_check_sync()

            except asyncio.CancelledError:
                logger.info("❌ Автопроверка отменена")
                break
            except Exception as e:
                logger.error(f"❌ Ошибка в автоматической синхронизации: {e}")
                await asyncio.sleep(60)


async def stop_campaign_service():
    global campaign_service, auto_check_task

    if auto_check_task:
        auto_check_task.cancel()
        try:
            await auto_check_task
        except asyncio.CancelledError:
            pass

    if campaign_service:
        campaign_service.is_running = False
        logger.info("🛑 Сервис синхронизации остановлен")


async def startup_event():
    """
    Запуск сервиса при старте приложения
    """
    global auto_check_task
    auto_check_task = asyncio.create_task(start_campaign_service())


async def shutdown_event():
    """
    Остановка сервиса при завершении приложения
    """
    await stop_campaign_service()


# FastAPI роутер
campaign_router = APIRouter()


@campaign_router.post("/campaigns/sync-start")
async def manual_startup_sync(background_tasks: BackgroundTasks):
    """Ручной запуск первичной синхронизации"""
    async def run_sync():
        async with KeitaroCampaignService() as service:
            await service.startup_campaign_sync()

    background_tasks.add_task(run_sync)
    return {"status": "started", "message": "Синхронизация запущена в фоне"}


@campaign_router.post("/campaigns/sync-auto")
async def manual_auto_sync(background_tasks: BackgroundTasks):
    """Ручной запуск автоматической проверки"""
    async def run_sync():
        async with KeitaroCampaignService() as service:
            await service.auto_check_sync()

    background_tasks.add_task(run_sync)
    return {"status": "started", "message": "Автоматическая проверка запущена"}


@campaign_router.get("/campaigns/stats")
async def get_campaign_stats():
    """Получить статистику по кампаниям"""
    try:
        stats = db.get_campaign_landing_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@campaign_router.post("/campaigns/stop")
async def stop_sync():
    """Остановить синхронизацию"""
    await stop_campaign_service()
    return {"status": "stopped", "message": "Синхронизация остановлена"}


@campaign_router.get("/campaigns/test-single/{user_id}")
async def test_single_user(user_id: int):
    """
    ОБНОВЛЕНО: Тестовый эндпоинт для проверки одного пользователя по его ID
    Теперь использует sub_id из БД
    """
    async with KeitaroCampaignService() as service:
        # Получаем sub_id из БД
        sub_id = db.get_user_sub_id(user_id)
        if not sub_id:
            return {
                "status": "error",
                "user_id": user_id,
                "error": "sub_id not found in database"
            }

        result = await service.get_conversion_data(sub_id)
        return {
            "status": "ok",
            "user_id": user_id,
            "sub_id": sub_id,
            "data": result
        }


@campaign_router.get("/campaigns/users-status")
async def get_users_status():
    """Получить детальную статистику по пользователям"""
    try:
        stats = db.get_detailed_users_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@campaign_router.get("/country/by-subid/{sub_id:path}")
async def get_country_by_subid(sub_id: str):
    """
    🌍 Получить страну по sub_id (формат: luqb8e.3a.4t77)
    """
    async with KeitaroCampaignService() as service:
        result = await service.get_country_by_sub_id(sub_id)
        return result


@campaign_router.get("/country/by-userid/{user_id}")
async def get_country_by_userid(user_id: int):
    """
    🌍 НОВЫЙ: Получить страну по user_id (используя sub_id из БД)
    """
    async with KeitaroCampaignService() as service:
        result = await service.get_country_by_user_id(user_id)
        return result


@campaign_router.get("/data/{sub_id:path}")
async def get_full_data(sub_id: str):
    """
    📊 НОВЫЙ: Получить полные данные (кампания, лендинг, страна) по sub_id
    """
    async with KeitaroCampaignService() as service:
        result = await service.get_full_data_by_sub_id(sub_id)
        return result


@campaign_router.get("/test/subid/{sub_id:path}")
async def test_subid_request(sub_id: str):
    """
    🧪 ТЕСТОВЫЙ ЭНДПОИНТ: Проверка получения данных из Keitaro по sub_id
    Возвращает подробный ответ для отладки

    Примеры использования:
    - curl http://localhost:8000/api/test/subid/3tse38v.5c.507c
    - http://localhost:8000/api/test/subid/3tse38v.5c.507c
    """
    async with KeitaroCampaignService() as service:
        logger.info(f"🧪 ТЕСТ: Запрос данных для sub_id: {sub_id}")

        conversion_data = await service.get_conversion_data(sub_id)

        result = {
            "test_mode": True,
            "sub_id": sub_id,
            "timestamp": datetime.now().isoformat(),
            "keitaro_domain": KEITARO_DOMAIN,
            "request_status": "success" if conversion_data.get("found") else "not_found",
            "data": conversion_data
        }

        if conversion_data.get("found"):
            logger.info(f"✅ Данные найдены для {sub_id}")
            result["summary"] = {
                "campaign": conversion_data.get("campaign"),
                "campaign_id": conversion_data.get("campaign_id"),
                "landing": conversion_data.get("landing"),
                "landing_id": conversion_data.get("landing_id"),
                # Теперь это код страны (US, AE, etc.)
                "country": conversion_data.get("country"),
                "country_note": "country_flag используется для получения кода страны"
            }
        else:
            logger.warning(f"❌ Данные НЕ найдены для {sub_id}")
            result["error"] = conversion_data.get("reason", "Unknown error")

        return result
