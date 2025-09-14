from fastapi import APIRouter, BackgroundTasks
import asyncio
import httpx
from typing import List, Dict, Any, Optional
import time
import logging
from datetime import datetime, timedelta
from db import DataBase

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = DataBase()

# Конфигурация - очень консервативные значения
KEITARO_DOMAIN = "https://your-keitaro-domain.com"
KEITARO_ADMIN_API_KEY = "your-admin-api-key"
MAX_USERS_PER_SECOND = 2  # Максимум 2 пользователя в секунду
DELAY_BETWEEN_REQUESTS = 0.5  # 0.5 секунды между запросами (2 в секунду)
BATCH_SIZE = 10  # Маленькие батчи для API
HOURLY_CHECK_INTERVAL = 3600  # Раз в час (3600 секунд)

# Специальные маркеры для "пустых" записей
EMPTY_COMPANY_MARKER = "None"
EMPTY_COMPANY_ID_MARKER = -1


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

    def get_users_without_campaign_data(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей без данных кампании или с пустыми маркерами
        """
        try:
            users = db.get_users_without_campaign_data()
            logger.info(
                f"Найдено {len(users)} пользователей без данных кампании")
            return users
        except Exception as e:
            logger.error(f"Ошибка получения пользователей: {e}")
            return []

    async def get_campaign_data_for_user(self, sub_3: str) -> Dict[str, Any]:
        """
        Получает данные кампании для одного пользователя
        """
        headers = {
            "Api-Key": KEITARO_ADMIN_API_KEY,
            "Content-Type": "application/json"
        }

        payload = {
            "range": {
                "interval": "last_30_days",
            },
            "filters": [
                {
                    "name": "sub_id",
                    "operator": "EQUALS",
                    "expression": sub_3
                }
            ],
            "grouping": ["campaign_id", "campaign_name"],
            "metrics": ["clicks"],
            "limit": 1
        }

        try:
            response = await self.session.post(
                f"{KEITARO_DOMAIN}/admin_api/v1/reports/build",
                headers=headers,
                json=payload
            )

            if response.status_code == 200:
                data = response.json()

                if data.get("rows") and len(data["rows"]) > 0:
                    row = data["rows"][0]
                    return {
                        "campaign_id": row.get("campaign_id"),
                        "campaign_name": row.get("campaign_name"),
                        "found": True
                    }
                else:
                    return {
                        "found": False,
                        "reason": "No data in response"
                    }
            else:
                logger.warning(
                    f"API error for sub_3 {sub_3}: {response.status_code}")
                return {
                    "found": False,
                    "reason": f"API error: {response.status_code}"
                }

        except Exception as e:
            logger.error(f"Request error for sub_3 {sub_3}: {e}")
            return {
                "found": False,
                "reason": str(e)
            }

    async def process_users_slowly(self, users: List[Dict[str, Any]]):
        """
        Медленно обрабатывает пользователей с соблюдением лимитов
        """
        total_users = len(users)
        processed = 0
        successful = 0
        failed = 0
        empty_results = 0

        logger.info(
            f"Начинаем медленную обработку {total_users} пользователей")
        logger.info(
            f"Скорость: {MAX_USERS_PER_SECOND} пользователей в секунду")

        estimated_time = total_users / MAX_USERS_PER_SECOND
        logger.info(
            f"Примерное время обработки: {estimated_time/60:.1f} минут")

        for user in users:
            if not self.is_running:
                logger.info("Обработка остановлена")
                break

            user_id = user['user_id']
            sub_3 = user['sub_3']

            logger.info(
                f"Обрабатываем пользователя {user_id} (sub_3: {sub_3})")

            try:
                # Получаем данные кампании
                campaign_data = await self.get_campaign_data_for_user(sub_3)

                if campaign_data.get('found'):
                    # Найдены данные кампании
                    result = db.update_user_campaign_data(
                        user_id,
                        company=campaign_data.get('campaign_name'),
                        company_id=campaign_data.get('campaign_id')
                    )

                    if result.get('success'):
                        successful += 1
                        logger.info(
                            f"✓ Обновлен {user_id}: {campaign_data.get('campaign_name')}")
                    else:
                        failed += 1
                        logger.error(
                            f"✗ Ошибка обновления {user_id}: {result.get('error')}")
                else:
                    # Данные не найдены - помечаем пустыми маркерами
                    result = db.update_user_campaign_data(
                        user_id,
                        company=EMPTY_COMPANY_MARKER,
                        company_id=EMPTY_COMPANY_ID_MARKER
                    )

                    if result.get('success'):
                        empty_results += 1
                        logger.info(
                            f"○ Помечен как пустой {user_id}: {campaign_data.get('reason')}")
                    else:
                        failed += 1
                        logger.error(
                            f"✗ Ошибка маркировки {user_id}: {result.get('error')}")

            except Exception as e:
                failed += 1
                logger.error(f"✗ Исключение при обработке {user_id}: {e}")

            processed += 1

            # Показываем прогресс каждые 10 пользователей
            if processed % 10 == 0 or processed == total_users:
                progress = (processed / total_users) * 100
                logger.info(
                    f"Прогресс: {processed}/{total_users} ({progress:.1f}%)")

            # Пауза между запросами
            if processed < total_users:
                await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

        logger.info(f"\nОбработка завершена:")
        logger.info(f"  Всего: {total_users}")
        logger.info(f"  Обработано: {processed}")
        logger.info(f"  Найдены данные: {successful}")
        logger.info(f"  Пустые результаты: {empty_results}")
        logger.info(f"  Ошибки: {failed}")

        return {
            "total": total_users,
            "processed": processed,
            "successful": successful,
            "empty_results": empty_results,
            "failed": failed
        }

    async def startup_campaign_sync(self):
        """
        Синхронизация при старте приложения
        """
        logger.info("=== СТАРТ: Синхронизация данных кампаний ===")

        users = self.get_users_without_campaign_data()
        if not users:
            logger.info("Все пользователи уже имеют данные кампаний")
            return

        self.is_running = True
        result = await self.process_users_slowly(users)
        self.is_running = False

        logger.info("=== ЗАВЕРШЕНО: Синхронизация данных кампаний ===")
        return result

    async def hourly_campaign_sync(self):
        """
        Почасовая синхронизация для пользователей без данных
        """
        logger.info("=== ПОЧАСОВАЯ ПРОВЕРКА: Синхронизация данных кампаний ===")

        # Получаем только пользователей с пустыми маркерами (которые нужно перепроверить)
        users = db.get_users_with_empty_markers()
        if not users:
            logger.info("Нет пользователей для повторной проверки")
            return

        logger.info(
            f"Найдено {len(users)} пользователей для повторной проверки")

        self.is_running = True
        result = await self.process_users_slowly(users)
        self.is_running = False

        logger.info("=== ЗАВЕРШЕНО: Почасовая проверка ===")
        return result


# Глобальный сервис
campaign_service = None


async def start_campaign_service():
    """
    Запуск сервиса синхронизации кампаний
    """
    global campaign_service
    campaign_service = KeitaroCampaignService()

    async with campaign_service:
        # Запускаем стартовую синхронизацию
        await campaign_service.startup_campaign_sync()

        # Запускаем почасовую синхронизацию
        while True:
            try:
                await asyncio.sleep(HOURLY_CHECK_INTERVAL)
                await campaign_service.hourly_campaign_sync()
            except Exception as e:
                logger.error(f"Ошибка в почасовой синхронизации: {e}")
                await asyncio.sleep(60)  # Пауза минута при ошибке


async def stop_campaign_service():
    """
    Остановка сервиса
    """
    global campaign_service
    if campaign_service:
        campaign_service.is_running = False
        logger.info("Сервис синхронизации остановлен")


async def startup_event():
    """
    Вызывается при старте FastAPI приложения
    """
    asyncio.create_task(start_campaign_service())


async def shutdown_event():
    """
    Вызывается при остановке FastAPI приложения  
    """
    await stop_campaign_service()
