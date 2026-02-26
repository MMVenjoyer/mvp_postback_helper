"""
Pocket Option API Client

Модуль для получения данных трейдера из Pocket Option API.
Используется внутри существующих эндпоинтов (get_status/reg, get_status/dep).

API Pocket Option:
  URL: https://affiliate.pocketoption.com/api/user-info/{user_id}/{partner_id}/{hash}
  hash = md5("{user_id}:{partner_id}:{api_token}")
"""

import hashlib
import asyncio
import aiohttp
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any

from config import POCKET_API_TOKEN, POCKET_PARTNER_ID, POCKET_API_BASE_URL


def compute_hash(user_id: str, partner_id: str, api_token: str) -> str:
    """
    hash = md5("{user_id}:{partner_id}:{api_token}")
    """
    raw = f"{user_id}:{partner_id}:{api_token}"
    return hashlib.md5(raw.encode()).hexdigest()


def clean_trader_id(trader_id: str) -> str:
    """
    Чистит trader_id от префиксов (TRD_ и т.п.)
    Pocket Option ожидает чистый числовой ID.
    """
    cleaned = trader_id.strip()
    if cleaned.upper().startswith("TRD_"):
        cleaned = cleaned[4:]
    return cleaned


async def fetch_pocket_user_info(trader_id: str) -> Dict[str, Any]:
    """
    Запрашивает данные трейдера из Pocket Option API.

    Returns:
        {"success": True, "data": {...}} или {"success": False, "error": "..."}
    """
    if not POCKET_API_TOKEN or not POCKET_PARTNER_ID:
        return {"success": False, "error": "Pocket Option API not configured"}

    cid = clean_trader_id(trader_id)
    h = compute_hash(cid, POCKET_PARTNER_ID, POCKET_API_TOKEN)
    url = f"{POCKET_API_BASE_URL}/api/user-info/{cid}/{POCKET_PARTNER_ID}/{h}"

    try:
        timeout = aiohttp.ClientTimeout(total=10, connect=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as resp:
                status = resp.status
                text = await resp.text()

                if status != 200:
                    print(f"[POCKET] ✗ HTTP {status} для trader_id={trader_id}")
                    return {"success": False, "error": f"HTTP {status}", "http_status": status}

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    return {"success": False, "error": f"Invalid JSON: {text[:200]}"}

                if isinstance(data, dict) and data.get("error"):
                    return {"success": False, "error": data["error"]}

                print(f"[POCKET] ✓ Данные получены: trader_id={trader_id}")
                return {"success": True, "data": data}

    except asyncio.TimeoutError:
        print(f"[POCKET] ✗ Таймаут: trader_id={trader_id}")
        return {"success": False, "error": "Timeout (10s)"}

    except Exception as e:
        print(f"[POCKET] ✗ Ошибка: {e}")
        return {"success": False, "error": str(e)}


def save_pocket_data_to_db(db, user_id: int, pocket_data: dict) -> bool:
    """
    Обновляет поля Pocket Option в таблице users.
    Вызывается синхронно (db уже есть).

    Returns:
        True если обновлено, False если ошибка
    """
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                now = datetime.now(timezone.utc)

                # Парсим registered_at
                registered_at = None
                if pocket_data.get("registered_at"):
                    try:
                        registered_at = datetime.fromisoformat(
                            pocket_data["registered_at"].replace("Z", "+00:00")
                        )
                    except (ValueError, AttributeError):
                        pass

                cursor.execute("""
                    UPDATE users SET
                        balance = %s,
                        demo_balance = %s,
                        pocket_status = %s,
                        pocket_total_deposits = %s,
                        pocket_ftd_amount = %s,
                        pocket_country = %s,
                        pocket_registered_at = %s,
                        pocket_synced_at = %s
                    WHERE id = %s
                """, (
                    pocket_data.get("real_balance"),
                    pocket_data.get("demo_balance"),
                    pocket_data.get("status"),
                    pocket_data.get("total_deposits"),
                    pocket_data.get("ftd_amount"),
                    pocket_data.get("country"),
                    registered_at,
                    now,
                    user_id
                ))

                if cursor.rowcount > 0:
                    print(f"[POCKET DB] ✓ user {user_id}: balance={pocket_data.get('real_balance')}, status={pocket_data.get('status')}")
                    return True
                else:
                    print(f"[POCKET DB] ✗ user {user_id} не найден")
                    return False

    except Exception as e:
        print(f"[POCKET DB] ✗ Ошибка: {e}")
        return False


async def sync_and_get_balance(db, user_id: int) -> Dict[str, Any]:
    """
    Главная функция: синкает данные с покета и возвращает результат.

    Логика:
    1. Берём trader_id из БД
    2. Дёргаем Pocket Option API
    3. Сохраняем в БД
    4. Возвращаем balance + pocket_data

    Returns:
        {
            "synced": True/False,
            "balance": 260.0 или None,
            "pocket_data": {...} или None,
            "error": "..." или None
        }
    """
    # 1. Получаем trader_id
    trader_id = db.get_user_trader_id(user_id)

    if not trader_id:
        # Нет trader_id — возвращаем баланс из БД если есть
        balance = _get_balance_from_db(db, user_id)
        return {
            "synced": False,
            "balance": balance,
            "pocket_data": None,
            "error": "no_trader_id"
        }

    # 2. Дёргаем API
    result = await fetch_pocket_user_info(trader_id)

    if not result.get("success"):
        # API не ответил — возвращаем старый баланс из БД
        balance = _get_balance_from_db(db, user_id)
        return {
            "synced": False,
            "balance": balance,
            "pocket_data": None,
            "error": result.get("error")
        }

    pocket_data = result["data"]

    # 3. Сохраняем
    save_pocket_data_to_db(db, user_id, pocket_data)

    # 4. Возвращаем
    return {
        "synced": True,
        "balance": pocket_data.get("real_balance"),
        "pocket_data": pocket_data,
        "error": None
    }


def _get_balance_from_db(db, user_id: int) -> Optional[float]:
    """Достаёт текущий баланс из БД (без API вызова)"""
    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT balance FROM users WHERE id = %s", (user_id,))
                row = cursor.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
                return None
    except Exception:
        return None
