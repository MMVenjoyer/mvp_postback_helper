"""
Postback Router - обработка постбэков от внешних систем

Поддерживаемые эндпоинты:
- /postback/ftm - First Time Message (первое сообщение)
- /postback/reg - Registration (регистрация)
- /postback/dep - Deposit (первый депозит)
- /postback/redep - Redeposit (повторный депозит)
- /postback/withdraw - Withdraw (вывод средств)
- /postback/revenue - Revenue (выручка с лида)
- /postback/user_info - Информация о юзере (trader_id, clickid, статусы)

Параметры запросов:
- id: Telegram User ID (обязательный)
- subscriber_id: UUID идентификатор (опциональный, для обратной совместимости)
- trader_id: ID трейдера из платформы (для reg)
- clickid: Click ID из трекера Chatterfry (опциональный)
- sum: Сумма депозита/вывода/выручки (для dep/redep/withdraw/revenue)
- commission: Комиссия (для dep/redep)

Поиск пользователя происходит по всем доступным идентификаторам:
1. id (Telegram User ID) - приоритет
2. subscriber_id (UUID)
3. clickid (clickid_chatterfry)
4. trader_id

ВАЖНО: trader_id обновляется при КАЖДОМ постбэке, если передан.
Это нужно потому что юзеры могут регистрировать новые аккаунты на платформе.

v2.2: Параллельная отправка постбэков через asyncio.gather
- FTM: Chatterfy FTM + Keitaro параллельно
- DEP: Chatterfy DEP + Keitaro параллельно
- REDEP: Chatterfy REDEP + Keitaro параллельно

v2.4.1: Фикс пустого id от Pocket Option
- Pocket Option шлёт id= (пустая строка) → FastAPI 422
- id теперь принимается как str и парсится вручную
- Поиск по clickid работает когда id пустой

v2.4.2: Новый эндпоинт /postback/user_info
- Возвращает trader_id, clickid_chatterfry, reg, dep по Telegram ID
- Защищён X-API-Key
"""

from fastapi import APIRouter, Query, Header, HTTPException
from typing import Optional
import asyncio
import re

from db import DataBase
from api_request import (
    send_keitaro_postback, 
    send_chatterfy_postback, 
    send_chatterfy_withdraw_postback,
    send_chatterfy_ftm_postback
)
from logger_bot import send_error_log
from config import ENABLE_TELEGRAM_LOGS, REPORT_API_KEY
from pocket_api import sync_and_get_balance

db = DataBase()
router = APIRouter()

# UUID regex pattern для валидации subscriber_id
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')

# Паттерн для определения нераскрытых плейсхолдеров типа {trader_id}, {clickid} и т.д.
PLACEHOLDER_PATTERN = re.compile(r'^\{[^}]+\}$')


def sanitize_identifier(value: str, param_name: str = "param") -> Optional[str]:
    """
    Проверяет идентификатор на валидность.
    Возвращает None если значение:
    - пустое
    - является нераскрытым плейсхолдером типа {trader_id}
    - совпадает с именем параметра (trader_id='trader_id')
    """
    if not value or value.strip() == "":
        return None
    
    value = value.strip()
    
    # Проверяем на плейсхолдер типа {trader_id}
    if PLACEHOLDER_PATTERN.match(value):
        print(f"[POSTBACK] ⚠️ Игнорируем плейсхолдер {param_name}={value}")
        return None
    
    # Проверяем на буквальное имя параметра (trader_id='trader_id')
    if value.lower() == param_name.lower():
        print(f"[POSTBACK] ⚠️ Игнорируем невалидный {param_name}={value}")
        return None
    
    return value


def parse_id_parameter(id_value) -> Optional[int]:
    """
    Безопасно парсит параметр id.
    Pocket Option иногда шлёт id= (пустая строка) → FastAPI 422 если тип int.
    Принимаем как str, парсим вручную.

    Returns:
        int (positive) или None
    """
    if id_value is None or id_value == "":
        return None

    if isinstance(id_value, int):
        return id_value if id_value > 0 else None

    try:
        parsed = int(id_value)
        return parsed if parsed > 0 else None
    except (ValueError, TypeError):
        return None


def parse_sum_parameter(sum_value) -> float:
    """
    Безопасно парсит параметр sum.
    Если значение не может быть преобразовано в число или <= 0, возвращает 59.
    """
    DEFAULT_SUM = 59.0

    if sum_value is None or sum_value == "":
        return DEFAULT_SUM

    if isinstance(sum_value, (int, float)):
        return DEFAULT_SUM if sum_value <= 0 else float(sum_value)

    try:
        parsed = float(sum_value)
        return DEFAULT_SUM if parsed <= 0 else parsed
    except (ValueError, TypeError):
        return DEFAULT_SUM


def parse_revenue_parameter(revenue_value) -> Optional[float]:
    """
    Безопасно парсит параметр revenue.
    В отличие от sum, может быть 0 или отрицательным (для корректировок).
    Возвращает None если значение невалидно.
    """
    if revenue_value is None or revenue_value == "":
        return None

    if isinstance(revenue_value, (int, float)):
        return float(revenue_value)

    try:
        return float(revenue_value)
    except (ValueError, TypeError):
        return None


def parse_commission_parameter(commission_value) -> Optional[float]:
    """
    Безопасно парсит параметр commission.
    Возвращает None если значение невалидно.
    """
    if commission_value is None or commission_value == "":
        return None

    if isinstance(commission_value, (int, float)):
        return float(commission_value) if commission_value >= 0 else None

    try:
        parsed = float(commission_value)
        return parsed if parsed >= 0 else None
    except (ValueError, TypeError):
        return None


def is_valid_uuid(value: str) -> bool:
    """Проверяет, является ли строка валидным UUID"""
    if not value:
        return False
    return bool(UUID_PATTERN.match(value))


async def ensure_user_and_update_clickid(
    user_id: int,
    subscriber_id: str = None,
    trader_id: str = None,
    clickid: str = None
) -> dict:
    """
    Гарантирует существование пользователя и обновляет clickid/trader_id если нужно.

    ВАЖНО: trader_id обновляется ВСЕГДА когда передан, даже для существующих юзеров.
    Это нужно т.к. юзеры могут регать новые аккаунты на платформе.
    """
    result = db.ensure_user_exists(
        user_id=user_id,
        subscriber_id=subscriber_id,
        trader_id=trader_id,
        clickid_chatterfry=clickid
    )

    if not result.get("success"):
        return result

    actual_user_id = result.get("user_id", user_id)

    # Если юзер уже существовал - обновляем clickid и trader_id
    if result.get("existed"):
        # Обновляем clickid (только если пустой)
        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

        # ВАЖНО: Обновляем trader_id ВСЕГДА когда передан
        # Юзер мог зарегать новый аккаунт на платформе
        if trader_id:
            old_trader_id = db.get_user_trader_id(actual_user_id)
            if old_trader_id != trader_id:
                update_result = db.update_user_trader_id(
                    actual_user_id, trader_id)
                if update_result.get("success"):
                    print(
                        f"[POSTBACK] ✓ trader_id обновлен для user {actual_user_id}: {old_trader_id} -> {trader_id}")
                    result["trader_id_updated"] = True
                    result["old_trader_id"] = old_trader_id
                    result["new_trader_id"] = trader_id

    return result


async def find_user_for_deposit(
    user_id: int = None,
    subscriber_id: str = None,
    clickid: str = None,
    trader_id: str = None
) -> Optional[int]:
    """
    Ищет пользователя для операций deposit/redeposit/revenue.
    """
    found = db.find_user_by_any_identifier(
        user_id=user_id,
        subscriber_id=subscriber_id,
        clickid_chatterfry=clickid,
        trader_id=trader_id
    )

    if found:
        return found.get("user_id")

    return None


async def update_trader_id_if_needed(user_id: int, trader_id: str) -> dict:
    """
    Обновляет trader_id если он передан и отличается от текущего.
    Возвращает информацию об обновлении.
    """
    if not trader_id:
        return {"updated": False, "reason": "no_trader_id_provided"}

    old_trader_id = db.get_user_trader_id(user_id)

    if old_trader_id == trader_id:
        return {"updated": False, "reason": "same_trader_id"}

    update_result = db.update_user_trader_id(user_id, trader_id)

    if update_result.get("success"):
        print(
            f"[POSTBACK] ✓ trader_id обновлен для user {user_id}: {old_trader_id} -> {trader_id}")
        return {
            "updated": True,
            "old_trader_id": old_trader_id,
            "new_trader_id": trader_id
        }
    else:
        return {"updated": False, "reason": "db_error", "error": update_result.get("error")}


async def send_postbacks_parallel(**named_coros):
    """
    Выполняет несколько постбэков параллельно через asyncio.gather.
    
    Args:
        **named_coros: именованные корутины, None значения пропускаются
        
    Returns:
        dict с результатами по именам. Если корутина была None — её нет в результате.
        Если корутина выбросила исключение — возвращается fallback dict.
    
    Пример:
        results = await send_postbacks_parallel(
            chatterfy=send_chatterfy_postback(...) if clickid else None,
            keitaro=send_keitaro_postback(...) if subid else None,
        )
        chatterfy_result = results.get('chatterfy')  # dict или None
        keitaro_result = results.get('keitaro')       # dict или None
    """
    # Фильтруем None корутины
    active = {k: v for k, v in named_coros.items() if v is not None}
    
    if not active:
        return {}
    
    keys = list(active.keys())
    coros = list(active.values())
    
    # return_exceptions=True чтобы одна ошибка не отменяла остальные
    raw_results = await asyncio.gather(*coros, return_exceptions=True)
    
    output = {}
    for key, result in zip(keys, raw_results):
        if isinstance(result, Exception):
            print(f"[PARALLEL] ⚠️ Ошибка в {key}: {result}")
            output[key] = {
                "ok": False,
                "text": str(result),
                "error_type": type(result).__name__,
                "full_url": "unknown"
            }
        else:
            output[key] = result
    
    return output


@router.get("/ftm")
async def ftm_postback(
    id: int = Query(..., description="Telegram User ID"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (optional)"),
    trader_id: str = Query(None, description="Trader ID (optional)")
):
    """
    FTM (First Time Message) постбэк
    
    При FTM также отправляется постбэк в Chatterfy с source и company:
    - source определяется по company: direct/facebook/google
    - company берется из БД
    
    v2.2: Chatterfy FTM + Keitaro отправляются параллельно
    """
    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    print(
        f"[POSTBACK FTM] id: {id}, clickid: {clickid}, subscriber_id: {subscriber_id}, trader_id: {trader_id}")

    try:
        user_result = await ensure_user_and_update_clickid(
            user_id=id,
            subscriber_id=subscriber_id,
            trader_id=trader_id,
            clickid=clickid
        )

        if not user_result.get("success"):
            error_msg = user_result.get('error', 'Unknown error')
            print(
                f"[POSTBACK FTM] ✗ Ошибка создания/поиска пользователя: {error_msg}")
            return {"status": "error", "error": error_msg}

        user_created = user_result.get("created", False)
        trader_id_updated = user_result.get("trader_id_updated", False)

        if user_created:
            print(f"[POSTBACK FTM] ✓ Создан новый пользователь {id}")

        if db.check_duplicate_transaction(id, "ftm", time_window_seconds=30):
            print(
                f"[POSTBACK FTM] ⚠️ Дубликат транзакции для user {id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": id,
                "message": "Transaction already processed within last 30 seconds"
            }

        result = db.process_postback(
            user_id=id,
            action="ftm",
            sum_amount=None,
            raw_data={
                "id": id,
                "action": "ftm",
                "clickid": clickid,
                "subscriber_id": subscriber_id,
                "trader_id": trader_id,
                "user_created": user_created,
                "trader_id_updated": trader_id_updated
            }
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK FTM] ✗ Ошибка записи в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи FTM в БД: {error_msg}",
                    user_id=id,
                    additional_info={
                        "action": "ftm", "endpoint": "/postback/ftm", "clickid": clickid},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK FTM] ✓ Записано в БД для user {id}")

        subid = db.get_user_sub_id(id)
        user_clickid = db.get_user_clickid(id)
        user_company = db.get_user_company(id)

        # ========================================
        # Параллельная отправка постбэков (v2.2)
        # ========================================
        if subid:
            print(
                f"[POSTBACK FTM] Отправляем постбэк в Keitaro для subid: {subid}, tid=4")
        if user_clickid:
            print(
                f"[POSTBACK FTM] Отправляем постбэк в Chatterfy: clickid={user_clickid}, company={user_company}")

        postback_results = await send_postbacks_parallel(
            chatterfy=send_chatterfy_ftm_postback(
                clickid=user_clickid,
                company=user_company,
                user_id=id
            ) if user_clickid else None,
            keitaro=send_keitaro_postback(
                subid=subid, status="ftm", tid=4, user_id=id
            ) if subid else None,
        )

        chatterfy_ftm_result = postback_results.get('chatterfy')
        keitaro_result = postback_results.get('keitaro')

        # ========================================
        # Формируем ответ (формат идентичен v2.1)
        # ========================================
        return {
            "status": "ok",
            "user_id": id,
            "action": "ftm",
            "user_created": user_created,
            "trader_id_updated": trader_id_updated,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "tid": 4,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            } if subid else "skipped - no subid",
            "chatterfy_ftm_postback": {
                "sent": chatterfy_ftm_result.get("ok") if chatterfy_ftm_result else False,
                "clickid": user_clickid,
                "source": chatterfy_ftm_result.get("source") if chatterfy_ftm_result else None,
                "company": chatterfy_ftm_result.get("company") if chatterfy_ftm_result else None,
                "url": chatterfy_ftm_result.get("full_url") if chatterfy_ftm_result else None
            } if user_clickid else "skipped - no clickid"
        }

    except Exception as e:
        print(f"[POSTBACK FTM] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_FTM_EXCEPTION",
                error_message=f"Необработанная ошибка в FTM постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "ftm",
                                 "endpoint": "/postback/ftm", "clickid": clickid},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/reg")
async def reg_postback(
    id: int = Query(..., description="Telegram User ID"),
    trader_id: str = Query(None, description="Trader ID from MVP platform"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (optional)")
):
    """
    Регистрация пользователя.
    trader_id обновляется ВСЕГДА когда передан (юзер мог зарегать новый аккаунт).
    
    REG отправляет только Keitaro постбэк (один HTTP вызов), параллелизация не нужна.
    """
    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    print(
        f"[POSTBACK REG] id: {id}, trader_id: {trader_id}, clickid: {clickid}, subscriber_id: {subscriber_id}")

    try:
        user_result = await ensure_user_and_update_clickid(
            user_id=id,
            subscriber_id=subscriber_id,
            trader_id=trader_id,
            clickid=clickid
        )

        if not user_result.get("success"):
            error_msg = user_result.get('error', 'Unknown error')
            print(
                f"[POSTBACK REG] ✗ Ошибка создания/поиска пользователя: {error_msg}")
            return {"status": "error", "error": error_msg}

        user_created = user_result.get("created", False)
        trader_id_updated = user_result.get("trader_id_updated", False)
        old_trader_id = user_result.get("old_trader_id")

        if user_created:
            print(f"[POSTBACK REG] ✓ Создан новый пользователь {id}")

        if trader_id_updated:
            print(
                f"[POSTBACK REG] ✓ trader_id обновлен: {old_trader_id} -> {trader_id}")

        if db.check_duplicate_transaction(id, "reg", time_window_seconds=30):
            print(
                f"[POSTBACK REG] ⚠️ Дубликат транзакции для user {id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": id,
                "message": "Transaction already processed within last 30 seconds"
            }

        raw_data = {
            "id": id,
            "action": "reg",
            "clickid": clickid,
            "subscriber_id": subscriber_id,
            "user_created": user_created,
            "trader_id_updated": trader_id_updated
        }
        if trader_id:
            raw_data["trader_id"] = trader_id
        if old_trader_id:
            raw_data["old_trader_id"] = old_trader_id

        result = db.process_postback(
            user_id=id, action="reg", sum_amount=None, raw_data=raw_data)

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK REG] ✗ Ошибка записи в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи REG в БД: {error_msg}",
                    user_id=id,
                    additional_info={"action": "reg", "trader_id": trader_id,
                                     "clickid": clickid, "endpoint": "/postback/reg"},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK REG] ✓ Записано в БД для user {id}")

        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK REG] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": id,
                "action": "reg",
                "user_created": user_created,
                "trader_id": trader_id,
                "trader_id_updated": trader_id_updated,
                "old_trader_id": old_trader_id,
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        print(
            f"[POSTBACK REG] Отправляем постбэк в Keitaro для subid: {subid}, tid=5")
        keitaro_result = await send_keitaro_postback(subid=subid, status="reg", tid=5, user_id=id)

        return {
            "status": "ok",
            "user_id": id,
            "action": "reg",
            "user_created": user_created,
            "trader_id": trader_id,
            "trader_id_updated": trader_id_updated,
            "old_trader_id": old_trader_id,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "tid": 5,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK REG] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_REG_EXCEPTION",
                error_message=f"Необработанная ошибка в REG постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "reg", "trader_id": trader_id,
                                 "clickid": clickid, "endpoint": "/postback/reg"},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/dep")
async def dep_postback(
    id: str = Query(None, description="Telegram User ID"),
    sum: str = Query(None, description="Deposit amount (default: 59)"),
    commission: str = Query(None, description="Commission amount"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(
        None, description="Trader ID (for search and update)")
):
    """
    Депозит пользователя (первый депозит)
    Отправляет событие SALE в Keitaro и постбэк в Chatterfy с суммой депозитов (event: sumdep)

    ВАЖНО: trader_id обновляется если передан (юзер мог зарегать новый аккаунт)
    
    v2.2: Chatterfy DEP + Keitaro SALE отправляются параллельно
    v2.4.1: id принимается как str (Pocket Option шлёт id= пустым)
    """
    # v2.4.1: Парсим id из строки (Pocket Option шлёт id= пустым)
    id = parse_id_parameter(id)

    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    sum_value = parse_sum_parameter(sum)
    commission_value = parse_commission_parameter(commission)

    print(
        f"[POSTBACK DEP] id: {id}, subscriber_id: {subscriber_id}, clickid: {clickid}, trader_id: {trader_id}")
    print(
        f"[POSTBACK DEP] sum_raw: {sum!r}, sum_parsed: {sum_value}, commission_raw: {commission!r}, commission_parsed: {commission_value}")

    if not id and not subscriber_id and not clickid and not trader_id:
        return {"status": "error", "error": "At least one identifier required: 'id', 'subscriber_id', 'clickid', or 'trader_id'"}

    try:
        actual_user_id = await find_user_for_deposit(
            user_id=id,
            subscriber_id=subscriber_id,
            clickid=clickid,
            trader_id=trader_id
        )

        user_created = False
        trader_id_update_info = {"updated": False}

        if not actual_user_id and id:
            user_result = await ensure_user_and_update_clickid(
                user_id=id,
                subscriber_id=subscriber_id,
                trader_id=trader_id,
                clickid=clickid
            )
            if user_result.get("success"):
                actual_user_id = id
                user_created = user_result.get("created", False)
                print(f"[POSTBACK DEP] ✓ Создан новый пользователь {id}")

        if not actual_user_id:
            error_msg = f"User not found: id={id}, subscriber_id={subscriber_id}, clickid={clickid}, trader_id={trader_id}"
            print(f"[POSTBACK DEP] ✗ {error_msg}")
            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK DEP] Найден пользователь: {actual_user_id}")

        # Обновляем clickid если передан
        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

        # ВАЖНО: Обновляем trader_id если передан (юзер мог зарегать новый аккаунт)
        if trader_id and not user_created:
            trader_id_update_info = await update_trader_id_if_needed(actual_user_id, trader_id)

        if db.check_duplicate_transaction(actual_user_id, "dep", sum_amount=sum_value, time_window_seconds=60):
            print(
                f"[POSTBACK DEP] ⚠️ Дубликат транзакции для user {actual_user_id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": actual_user_id,
                "message": "Transaction already processed within last 60 seconds"
            }

        previous_deposits = db.get_user_deposits_count(actual_user_id)
        tid_value = 6 + previous_deposits
        print(
            f"[POSTBACK DEP] Предыдущих депозитов: {previous_deposits}, tid будет: {tid_value}")

        result = db.process_postback(
            user_id=actual_user_id,
            action="dep",
            sum_amount=sum_value,
            commission=commission_value,
            raw_data={
                "id": id,
                "subscriber_id": subscriber_id,
                "clickid": clickid,
                "trader_id": trader_id,
                "action": "dep",
                "sum": sum_value,
                "commission": commission_value,
                "tid": tid_value,
                "user_created": user_created,
                "trader_id_updated": trader_id_update_info.get("updated", False),
                "old_trader_id": trader_id_update_info.get("old_trader_id")
            }
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK DEP] ✗ Ошибка записи в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи DEP в БД: {error_msg}",
                    user_id=actual_user_id,
                    additional_info={"action": "dep", "sum": sum_value, "commission": commission_value,
                                     "tid": tid_value, "clickid": clickid, "endpoint": "/postback/dep"},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(
            f"[POSTBACK DEP] ✓ Записано в БД для user {actual_user_id}, sum={sum_value}, commission={commission_value}")

        subid = db.get_user_sub_id(actual_user_id)
        user_clickid = db.get_user_clickid(actual_user_id)
        total_deposits_sum = db.get_user_total_deposits_sum(actual_user_id)
        print(
            f"[POSTBACK DEP] Общая сумма депозитов после записи: {total_deposits_sum}")

        # ========================================
        # Параллельная отправка постбэков (v2.2)
        # ========================================
        if user_clickid:
            print(
                f"[POSTBACK DEP] Отправляем постбэк в Chatterfy: clickid={user_clickid}, sumdep={total_deposits_sum}, previous_dep={sum_value}, event=sumdep")
        else:
            print(
                f"[POSTBACK DEP] ⚠️ clickid_chatterfry не найден для user {actual_user_id}, постбэк в Chatterfy не отправлен")

        if subid:
            print(
                f"[POSTBACK DEP] Отправляем постбэк SALE в Keitaro для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        else:
            print(
                f"[POSTBACK DEP] ⚠️ sub_id не найден для user {actual_user_id}, постбэк в Keitaro не отправлен")

        postback_results = await send_postbacks_parallel(
            chatterfy=send_chatterfy_postback(
                clickid=user_clickid,
                sumdep=total_deposits_sum,
                previous_dep=sum_value,
                is_redep=False,  # DEP использует event "sumdep"
                user_id=actual_user_id
            ) if user_clickid else None,
            keitaro=send_keitaro_postback(
                subid=subid,
                status="sale",
                payout=sum_value,
                tid=tid_value,
                user_id=actual_user_id
            ) if subid else None,
        )

        chatterfy_result = postback_results.get('chatterfy')
        keitaro_result = postback_results.get('keitaro')

        # ========================================
        # Формируем ответ (формат идентичен v2.1)
        # ========================================
        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "dep",
            "sum": sum_value,
            "commission": commission_value,
            "tid": tid_value,
            "user_created": user_created,
            "trader_id_updated": trader_id_update_info.get("updated", False),
            "old_trader_id": trader_id_update_info.get("old_trader_id"),
            "new_trader_id": trader_id if trader_id_update_info.get("updated") else None,
            "transaction_id": result.get("transaction_id"),
            "total_deposits_sum": total_deposits_sum,
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "status_sent": "sale",
                "payout": sum_value,
                "tid": tid_value,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            } if subid else "skipped - no subid",
            "chatterfy_postback": {
                "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                "clickid": user_clickid,
                "event": "sumdep",
                "sumdep": total_deposits_sum,
                "previous_dep": sum_value,
                "url": chatterfy_result.get("full_url") if chatterfy_result else None
            } if user_clickid else "skipped - no clickid"
        }

    except Exception as e:
        print(f"[POSTBACK DEP] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_DEP_EXCEPTION",
                error_message=f"Необработанная ошибка в DEP постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "dep", "sum": sum, "commission": commission, "subscriber_id": subscriber_id,
                                 "clickid": clickid, "trader_id": trader_id, "endpoint": "/postback/dep"},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/redep")
async def redep_postback(
    id: str = Query(None, description="Telegram User ID"),
    sum: str = Query(None, description="Redeposit amount (default: 59)"),
    commission: str = Query(None, description="Commission amount"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(
        None, description="Trader ID (for search and update)")
):
    """
    Редепозит пользователя (повторный депозит)
    Отправляет событие DEP в Keitaro и постбэк в Chatterfy с суммой депозитов (event: pb_redep)

    ВАЖНО: trader_id обновляется если передан (юзер мог зарегать новый аккаунт)
    
    v2.2: Chatterfy REDEP + Keitaro DEP отправляются параллельно
    v2.4.1: id принимается как str (Pocket Option шлёт id= пустым)
    """
    # v2.4.1: Парсим id из строки (Pocket Option шлёт id= пустым)
    id = parse_id_parameter(id)

    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    sum_value = parse_sum_parameter(sum)
    commission_value = parse_commission_parameter(commission)

    print(
        f"[POSTBACK REDEP] id: {id}, subscriber_id: {subscriber_id}, clickid: {clickid}, trader_id: {trader_id}")
    print(
        f"[POSTBACK REDEP] sum_raw: {sum!r}, sum_parsed: {sum_value}, commission_raw: {commission!r}, commission_parsed: {commission_value}")

    if not id and not subscriber_id and not clickid and not trader_id:
        return {"status": "error", "error": "At least one identifier required: 'id', 'subscriber_id', 'clickid', or 'trader_id'"}

    try:
        actual_user_id = await find_user_for_deposit(
            user_id=id,
            subscriber_id=subscriber_id,
            clickid=clickid,
            trader_id=trader_id
        )

        user_created = False
        trader_id_update_info = {"updated": False}

        if not actual_user_id and id:
            user_result = await ensure_user_and_update_clickid(
                user_id=id,
                subscriber_id=subscriber_id,
                trader_id=trader_id,
                clickid=clickid
            )
            if user_result.get("success"):
                actual_user_id = id
                user_created = user_result.get("created", False)
                print(f"[POSTBACK REDEP] ✓ Создан новый пользователь {id}")

        if not actual_user_id:
            error_msg = f"User not found: id={id}, subscriber_id={subscriber_id}, clickid={clickid}, trader_id={trader_id}"
            print(f"[POSTBACK REDEP] ✗ {error_msg}")
            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK REDEP] Найден пользователь: {actual_user_id}")

        # Обновляем clickid если передан
        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

        # ВАЖНО: Обновляем trader_id если передан (юзер мог зарегать новый аккаунт)
        if trader_id and not user_created:
            trader_id_update_info = await update_trader_id_if_needed(actual_user_id, trader_id)

        if db.check_duplicate_transaction(actual_user_id, "redep", sum_amount=sum_value, time_window_seconds=60):
            print(
                f"[POSTBACK REDEP] ⚠️ Дубликат транзакции для user {actual_user_id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": actual_user_id,
                "message": "Transaction already processed within last 60 seconds"
            }

        previous_deposits = db.get_user_deposits_count(actual_user_id)
        tid_value = 6 + previous_deposits
        print(
            f"[POSTBACK REDEP] Предыдущих депозитов: {previous_deposits}, tid будет: {tid_value}")

        result = db.process_postback(
            user_id=actual_user_id,
            action="redep",
            sum_amount=sum_value,
            commission=commission_value,
            raw_data={
                "id": id,
                "subscriber_id": subscriber_id,
                "clickid": clickid,
                "trader_id": trader_id,
                "action": "redep",
                "sum": sum_value,
                "commission": commission_value,
                "tid": tid_value,
                "user_created": user_created,
                "trader_id_updated": trader_id_update_info.get("updated", False),
                "old_trader_id": trader_id_update_info.get("old_trader_id")
            }
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK REDEP] ✗ Ошибка записи в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи REDEP в БД: {error_msg}",
                    user_id=actual_user_id,
                    additional_info={"action": "redep", "sum": sum_value, "commission": commission_value,
                                     "tid": tid_value, "clickid": clickid, "endpoint": "/postback/redep"},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(
            f"[POSTBACK REDEP] ✓ Записано в БД для user {actual_user_id}, sum={sum_value}, commission={commission_value}")

        subid = db.get_user_sub_id(actual_user_id)
        user_clickid = db.get_user_clickid(actual_user_id)
        total_deposits_sum = db.get_user_total_deposits_sum(actual_user_id)
        print(
            f"[POSTBACK REDEP] Общая сумма депозитов после записи: {total_deposits_sum}")

        # ========================================
        # Параллельная отправка постбэков (v2.2)
        # ========================================
        if user_clickid:
            print(
                f"[POSTBACK REDEP] Отправляем постбэк в Chatterfy: clickid={user_clickid}, sumdep={total_deposits_sum}, previous_dep={sum_value}, event=pb_redep")
        else:
            print(
                f"[POSTBACK REDEP] ⚠️ clickid_chatterfry не найден для user {actual_user_id}, постбэк в Chatterfy не отправлен")

        if subid:
            print(
                f"[POSTBACK REDEP] Отправляем постбэк DEP в Keitaro для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        else:
            print(
                f"[POSTBACK REDEP] ⚠️ sub_id не найден для user {actual_user_id}, постбэк в Keitaro не отправлен")

        postback_results = await send_postbacks_parallel(
            chatterfy=send_chatterfy_postback(
                clickid=user_clickid,
                sumdep=total_deposits_sum,
                previous_dep=sum_value,
                is_redep=True,  # REDEP использует event "pb_redep"
                user_id=actual_user_id
            ) if user_clickid else None,
            keitaro=send_keitaro_postback(
                subid=subid,
                status="dep",
                payout=sum_value,
                tid=tid_value,
                user_id=actual_user_id
            ) if subid else None,
        )

        chatterfy_result = postback_results.get('chatterfy')
        keitaro_result = postback_results.get('keitaro')

        # ========================================
        # Формируем ответ (формат идентичен v2.1)
        # ========================================
        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "redep",
            "sum": sum_value,
            "commission": commission_value,
            "tid": tid_value,
            "user_created": user_created,
            "trader_id_updated": trader_id_update_info.get("updated", False),
            "old_trader_id": trader_id_update_info.get("old_trader_id"),
            "new_trader_id": trader_id if trader_id_update_info.get("updated") else None,
            "transaction_id": result.get("transaction_id"),
            "total_deposits_sum": total_deposits_sum,
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "status_sent": "dep",
                "payout": sum_value,
                "tid": tid_value,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            } if subid else "skipped - no subid",
            "chatterfy_postback": {
                "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                "clickid": user_clickid,
                "event": "pb_redep",
                "sumdep": total_deposits_sum,
                "previous_dep": sum_value,
                "url": chatterfy_result.get("full_url") if chatterfy_result else None
            } if user_clickid else "skipped - no clickid"
        }

    except Exception as e:
        print(f"[POSTBACK REDEP] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_REDEP_EXCEPTION",
                error_message=f"Необработанная ошибка в REDEP постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "redep", "sum": sum, "commission": commission, "subscriber_id": subscriber_id,
                                 "clickid": clickid, "trader_id": trader_id, "endpoint": "/postback/redep"},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/withdraw")
async def withdraw_postback(
    id: str = Query(None, description="Telegram User ID"),
    sum: str = Query(None, description="Withdraw amount"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(
        None, description="Trader ID (for search and update)")
):
    """
    Вывод средств пользователя
    Отправляет событие withdraw в Chatterfy с суммой вывода

    ВАЖНО: trader_id обновляется если передан (юзер мог зарегать новый аккаунт)
    
    v2.4.1: id принимается как str (Pocket Option шлёт id= пустым)
    """
    # v2.4.1: Парсим id из строки (Pocket Option шлёт id= пустым)
    id = parse_id_parameter(id)

    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    sum_value = parse_sum_parameter(sum)
    print(
        f"[POSTBACK WITHDRAW] id: {id}, sum: {sum} -> {sum_value}, clickid: {clickid}, subscriber_id: {subscriber_id}, trader_id: {trader_id}")

    try:
        # Стандартная логика поиска/создания пользователя
        actual_user_id = None
        user_created = False
        trader_id_update_info = {}

        # Ищем существующего пользователя
        actual_user_id = await find_user_for_deposit(
            user_id=id,
            subscriber_id=subscriber_id,
            clickid=clickid,
            trader_id=trader_id
        )

        # Если не нашли и есть id - создаем нового
        if not actual_user_id and id:
            print(
                f"[POSTBACK WITHDRAW] Пользователь не найден, создаем нового с id={id}")
            user_result = await ensure_user_and_update_clickid(
                user_id=id,
                subscriber_id=subscriber_id,
                trader_id=trader_id,
                clickid=clickid
            )
            if user_result.get("success"):
                actual_user_id = id
                user_created = user_result.get("created", False)
                print(f"[POSTBACK WITHDRAW] ✓ Создан новый пользователь {id}")

        if not actual_user_id:
            error_msg = f"User not found: id={id}, subscriber_id={subscriber_id}, clickid={clickid}, trader_id={trader_id}"
            print(f"[POSTBACK WITHDRAW] ✗ {error_msg}")
            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK WITHDRAW] Найден пользователь: {actual_user_id}")

        # Обновляем clickid если передан
        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

        # ВАЖНО: Обновляем trader_id если передан (юзер мог зарегать новый аккаунт)
        if trader_id and not user_created:
            trader_id_update_info = await update_trader_id_if_needed(actual_user_id, trader_id)

        # Проверка дубликата
        if db.check_duplicate_transaction(actual_user_id, "withdraw", sum_amount=sum_value, time_window_seconds=60):
            print(
                f"[POSTBACK WITHDRAW] ⚠️ Дубликат транзакции для user {actual_user_id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": actual_user_id,
                "message": "Transaction already processed within last 60 seconds"
            }

        # Записываем транзакцию в БД
        result = db.process_postback(
            user_id=actual_user_id,
            action="withdraw",
            sum_amount=sum_value,
            commission=None,
            raw_data={
                "id": id,
                "subscriber_id": subscriber_id,
                "clickid": clickid,
                "trader_id": trader_id,
                "action": "withdraw",
                "sum": sum_value,
                "user_created": user_created,
                "trader_id_updated": trader_id_update_info.get("updated", False),
                "old_trader_id": trader_id_update_info.get("old_trader_id")
            }
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK WITHDRAW] ✗ Ошибка записи в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи WITHDRAW в БД: {error_msg}",
                    user_id=actual_user_id,
                    additional_info={"action": "withdraw", "sum": sum_value,
                                     "clickid": clickid, "endpoint": "/postback/withdraw"},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(
            f"[POSTBACK WITHDRAW] ✓ Записано в БД для user {actual_user_id}, sum={sum_value}")

        user_clickid = db.get_user_clickid(actual_user_id)

        # Отправляем постбэк в Chatterfy (если есть clickid)
        chatterfy_result = None
        if user_clickid:
            print(
                f"[POSTBACK WITHDRAW] Отправляем постбэк в Chatterfy: clickid={user_clickid}, withdraw={sum_value}")
            chatterfy_result = await send_chatterfy_withdraw_postback(
                clickid=user_clickid,
                withdraw_amount=sum_value,
                user_id=actual_user_id
            )
        else:
            print(
                f"[POSTBACK WITHDRAW] ⚠️ clickid_chatterfry не найден для user {actual_user_id}, постбэк в Chatterfy не отправлен")

        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "withdraw",
            "sum": sum_value,
            "user_created": user_created,
            "trader_id_updated": trader_id_update_info.get("updated", False),
            "old_trader_id": trader_id_update_info.get("old_trader_id"),
            "new_trader_id": trader_id if trader_id_update_info.get("updated") else None,
            "transaction_id": result.get("transaction_id"),
            "chatterfy_postback": {
                "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                "clickid": user_clickid,
                "event": "withdraw",
                "withdraw_amount": sum_value,
                "url": chatterfy_result.get("full_url") if chatterfy_result else None
            } if user_clickid else "skipped - no clickid"
        }

    except Exception as e:
        print(f"[POSTBACK WITHDRAW] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_WITHDRAW_EXCEPTION",
                error_message=f"Необработанная ошибка в WITHDRAW постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "withdraw", "sum": sum, "subscriber_id": subscriber_id,
                                 "clickid": clickid, "trader_id": trader_id, "endpoint": "/postback/withdraw"},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/get_status/reg")
async def get_status_reg(
    id: int = Query(..., description="Telegram User ID")
):
    """
    Проверяет статус регистрации пользователя (есть ли reg_time).
    Заодно синкает данные с Pocket Option и возвращает баланс.
    """
    try:
        reg_status = False
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT reg_time FROM users WHERE id = %s", (id,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    reg_status = True

        # Синк с Pocket Option (не блокирует ответ при ошибке)
        pocket = await sync_and_get_balance(db, id)

        return {
            "status": reg_status,
            "balance": pocket.get("balance"),
            "pocket_synced": pocket.get("synced", False),
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/get_status/dep")
async def get_status_dep(
    id: int = Query(..., description="Telegram User ID")
):
    """
    Проверяет статус депозита пользователя (есть ли dep_time).
    Заодно синкает данные с Pocket Option и возвращает баланс.
    """
    try:
        dep_status = False
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT dep_time FROM users WHERE id = %s", (id,))
                result = cursor.fetchone()
                if result and result[0] is not None:
                    dep_status = True

        # Синк с Pocket Option (не блокирует ответ при ошибке)
        pocket = await sync_and_get_balance(db, id)

        return {
            "status": dep_status,
            "balance": pocket.get("balance"),
            "pocket_synced": pocket.get("synced", False),
        }

    except Exception as e:
        return {"status": "error", "error": str(e)}
    

@router.get("/manager{manager_id}")
async def manager_postback(
    manager_id: int,
    id: int = Query(..., description="Telegram User ID"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(None, description="UUID subscriber ID (optional)"),
    trader_id: str = Query(None, description="Trader ID (optional)")
):
    """
    Назначение менеджера лиду.
    
    URL формат:
    /postback/manager1?id={chatId}&clickid={tracker.clickid}
    /postback/manager2?id={chatId}&clickid={tracker.clickid}
    
    manager_id берётся из URL path (1, 2, и т.д.)
    """
    # Санитизация идентификаторов
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    manager_name = f"manager{manager_id}"

    print(
        f"[POSTBACK MANAGER] id: {id}, manager: {manager_name}, clickid: {clickid}, subscriber_id: {subscriber_id}, trader_id: {trader_id}")

    try:
        # Гарантируем что юзер существует + обновляем clickid/trader_id
        user_result = await ensure_user_and_update_clickid(
            user_id=id,
            subscriber_id=subscriber_id,
            trader_id=trader_id,
            clickid=clickid
        )

        if not user_result.get("success"):
            error_msg = user_result.get('error', 'Unknown error')
            print(f"[POSTBACK MANAGER] ✗ Ошибка создания/поиска пользователя: {error_msg}")
            return {"status": "error", "error": error_msg}

        user_created = user_result.get("created", False)
        trader_id_updated = user_result.get("trader_id_updated", False)

        if user_created:
            print(f"[POSTBACK MANAGER] ✓ Создан новый пользователь {id}")

        # Получаем предыдущего менеджера
        old_manager = db.get_user_manager(id)

        # Обновляем менеджера
        manager_result = db.update_user_manager(id, manager_name)

        if not manager_result.get("success"):
            error_msg = manager_result.get('error', 'Unknown error')
            print(f"[POSTBACK MANAGER] ✗ Ошибка обновления менеджера: {error_msg}")
            return {"status": "error", "error": error_msg}

        # Записываем транзакцию для истории
        db.create_transaction(
            user_id=id,
            action="manager_assign",
            sum_amount=None,
            raw_data={
                "id": id,
                "action": "manager_assign",
                "manager": manager_name,
                "old_manager": old_manager,
                "clickid": clickid,
                "subscriber_id": subscriber_id,
                "trader_id": trader_id,
                "user_created": user_created,
                "trader_id_updated": trader_id_updated
            }
        )

        manager_changed = old_manager != manager_name

        print(
            f"[POSTBACK MANAGER] ✓ Менеджер назначен: user={id}, manager={manager_name}"
            + (f" (был: {old_manager})" if old_manager and manager_changed else ""))

        return {
            "status": "ok",
            "user_id": id,
            "action": "manager_assign",
            "manager": manager_name,
            "old_manager": old_manager,
            "manager_changed": manager_changed,
            "user_created": user_created,
            "trader_id_updated": trader_id_updated
        }

    except Exception as e:
        print(f"[POSTBACK MANAGER] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_MANAGER_EXCEPTION",
                error_message=f"Ошибка в MANAGER постбэке: {str(e)}",
                user_id=id,
                additional_info={
                    "action": "manager_assign",
                    "manager": manager_name,
                    "endpoint": f"/postback/manager{manager_id}"
                },
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/manager_stats")
async def get_manager_stats():
    """Статистика по менеджерам"""
    try:
        stats = db.get_manager_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@router.get("/revenue")
async def revenue_postback(
    id: str = Query(None, description="Telegram User ID (optional)"),
    sum: str = Query(None, description="Revenue amount (actual total revenue)"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(
        None, description="Trader ID (priority over id if both point to different users)")
):
    """
    Выручка с лида (Revenue)
    
    Логика:
    - В transactions записываем каждое событие как есть (action='revenue', sum=переданная сумма)
    - В users.revenue ПЕРЕЗАПИСЫВАЕМ значение на актуальное (не суммируем)
    
    v2.4.1: id принимается как str (Pocket Option шлёт id= пустым)
    """
    # v2.4.1: Парсим id из строки (Pocket Option шлёт id= пустым)
    id = parse_id_parameter(id)

    # Санитизация идентификаторов - фильтруем плейсхолдеры
    trader_id = sanitize_identifier(trader_id, "trader_id")
    clickid = sanitize_identifier(clickid, "clickid")
    subscriber_id = sanitize_identifier(subscriber_id, "subscriber_id")

    revenue_value = parse_revenue_parameter(sum)
    
    print(
        f"[POSTBACK REVENUE] id: {id}, sum: {sum} -> {revenue_value}, clickid: {clickid}, subscriber_id: {subscriber_id}, trader_id: {trader_id}")

    # Проверяем что сумма передана
    if revenue_value is None:
        return {"status": "error", "error": "Parameter 'sum' is required for revenue postback"}

    # Проверяем что есть хотя бы один идентификатор
    if not id and not subscriber_id and not clickid and not trader_id:
        return {"status": "error", "error": "At least one identifier required: 'id', 'subscriber_id', 'clickid', or 'trader_id'"}

    try:
        actual_user_id = None
        found_by = None
        user_created = False
        trader_id_update_info = {"updated": False}

        # ПРИОРИТЕТ 1: Ищем по trader_id (главный приоритет)
        if trader_id:
            found = db.find_user_by_any_identifier(trader_id=trader_id)
            if found:
                actual_user_id = found.get("user_id")
                found_by = "trader_id"
                print(f"[POSTBACK REVENUE] Найден пользователь {actual_user_id} по trader_id={trader_id}")

        # ПРИОРИТЕТ 2: Если не нашли по trader_id - ищем по остальным
        if not actual_user_id:
            found = db.find_user_by_any_identifier(
                user_id=id,
                subscriber_id=subscriber_id,
                clickid_chatterfry=clickid
            )
            if found:
                actual_user_id = found.get("user_id")
                found_by = found.get("found_by")
                print(f"[POSTBACK REVENUE] Найден пользователь {actual_user_id} по {found_by}")

        # Если не нашли и есть id - создаем нового
        if not actual_user_id and id:
            print(
                f"[POSTBACK REVENUE] Пользователь не найден, создаем нового с id={id}")
            user_result = await ensure_user_and_update_clickid(
                user_id=id,
                subscriber_id=subscriber_id,
                trader_id=trader_id,
                clickid=clickid
            )
            if user_result.get("success"):
                actual_user_id = id
                user_created = user_result.get("created", False)
                found_by = "created_new"
                print(f"[POSTBACK REVENUE] ✓ Создан новый пользователь {id}")

        if not actual_user_id:
            error_msg = f"User not found: id={id}, subscriber_id={subscriber_id}, clickid={clickid}, trader_id={trader_id}"
            print(f"[POSTBACK REVENUE] ✗ {error_msg}")
            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK REVENUE] Используем пользователя: {actual_user_id} (found_by: {found_by})")

        # Обновляем clickid если передан
        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

        # Обновляем trader_id если передан и юзер не только что создан
        if trader_id and not user_created:
            trader_id_update_info = await update_trader_id_if_needed(actual_user_id, trader_id)

        # Получаем предыдущее значение revenue для логирования
        previous_revenue = db.get_user_revenue(actual_user_id)

        # Проверка дубликата (то же значение в течение 60 сек)
        if db.check_duplicate_transaction(actual_user_id, "revenue", sum_amount=revenue_value, time_window_seconds=60):
            print(
                f"[POSTBACK REVENUE] ⚠️ Дубликат транзакции для user {actual_user_id}, пропускаем")
            return {
                "status": "duplicate",
                "user_id": actual_user_id,
                "message": "Transaction already processed within last 60 seconds"
            }

        # 1. Записываем транзакцию (фиксируем каждое событие)
        transaction_result = db.create_transaction(
            user_id=actual_user_id,
            action="revenue",
            sum_amount=revenue_value,
            commission=None,
            raw_data={
                "id": id,
                "subscriber_id": subscriber_id,
                "clickid": clickid,
                "trader_id": trader_id,
                "action": "revenue",
                "sum": revenue_value,
                "previous_revenue": previous_revenue,
                "found_by": found_by,
                "user_created": user_created,
                "trader_id_updated": trader_id_update_info.get("updated", False),
                "old_trader_id": trader_id_update_info.get("old_trader_id")
            }
        )

        if not transaction_result.get("success"):
            error_msg = transaction_result.get('error', 'Unknown error')
            print(f"[POSTBACK REVENUE] ✗ Ошибка записи транзакции в БД: {error_msg}")

            if ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи REVENUE в БД: {error_msg}",
                    user_id=actual_user_id,
                    additional_info={"action": "revenue", "sum": revenue_value,
                                     "clickid": clickid, "endpoint": "/postback/revenue"},
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        # 2. Обновляем users.revenue (перезаписываем на актуальное значение)
        revenue_update_result = db.update_user_revenue(actual_user_id, revenue_value)

        if not revenue_update_result.get("success"):
            error_msg = revenue_update_result.get('error', 'Unknown error')
            print(f"[POSTBACK REVENUE] ⚠️ Ошибка обновления revenue в users: {error_msg}")
            # Не возвращаем ошибку - транзакция уже записана

        print(
            f"[POSTBACK REVENUE] ✓ Записано: user={actual_user_id}, revenue={revenue_value} (было: {previous_revenue})")

        # 3. Отправляем постбэк в Keitaro ТОЛЬКО если значение изменилось
        keitaro_result = None
        revenue_changed = previous_revenue != revenue_value
        
        if not revenue_changed:
            print(
                f"[POSTBACK REVENUE] ⚠️ Revenue не изменился ({revenue_value}), постбэк в Keitaro не отправлен")
        else:
            # Получаем subid для отправки в Keitaro
            subid = db.get_user_sub_id(actual_user_id)
            
            if not subid:
                print(
                    f"[POSTBACK REVENUE] ⚠️ sub_id не найден для user {actual_user_id}, постбэк в Keitaro не отправлен")
            else:
                print(
                    f"[POSTBACK REVENUE] Отправляем постбэк в Keitaro для subid: {subid}, status=revenue, payout={revenue_value}")
                keitaro_result = await send_keitaro_postback(
                    subid=subid,
                    status="revenue",
                    payout=revenue_value,
                    tid=None,  # tid не требуется для revenue
                    user_id=actual_user_id
                )

        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "revenue",
            "revenue": revenue_value,
            "previous_revenue": previous_revenue,
            "revenue_changed": revenue_changed,
            "found_by": found_by,
            "user_created": user_created,
            "trader_id_updated": trader_id_update_info.get("updated", False),
            "old_trader_id": trader_id_update_info.get("old_trader_id"),
            "new_trader_id": trader_id if trader_id_update_info.get("updated") else None,
            "transaction_id": transaction_result.get("transaction_id"),
            "revenue_updated": revenue_update_result.get("success", False),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok") if keitaro_result else False,
                "subid": db.get_user_sub_id(actual_user_id) if revenue_changed else None,
                "status_sent": "revenue",
                "payout": revenue_value,
                "url": keitaro_result.get("full_url") if keitaro_result else None,
                "response": keitaro_result.get("text")[:100] if keitaro_result and keitaro_result.get("text") else None
            } if revenue_changed else "skipped - same value"
        }

    except Exception as e:
        print(f"[POSTBACK REVENUE] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_REVENUE_EXCEPTION",
                error_message=f"Необработанная ошибка в REVENUE постбэке: {str(e)}",
                user_id=id,
                additional_info={"action": "revenue", "sum": sum, "subscriber_id": subscriber_id,
                                 "clickid": clickid, "trader_id": trader_id, "endpoint": "/postback/revenue"},
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


# ==========================================
# USER INFO (защищённый эндпоинт)
# ==========================================

@router.get("/user_info")
async def get_user_info(
    id: int = Query(..., description="Telegram User ID"),
    x_api_key: str = Header(None, alias="X-API-Key")
):
    """
    Возвращает trader_id, clickid_chatterfry и статусы reg/dep по Telegram ID.
    
    Требуется заголовок: X-API-Key (тот же REPORT_API_KEY)
    
    Пример:
        curl -H "X-API-Key: YOUR_KEY" "https://tylerwhite.icu/postback/user_info?id=123456789"
    
    Ответ:
    {
        "status": "ok",
        "user_id": 123456789,
        "trader_id": "12345",
        "clickid_chatterfry": "abc123",
        "reg": true,
        "dep": false
    }
    """
    # Проверка API ключа
    if not REPORT_API_KEY:
        raise HTTPException(status_code=500, detail="REPORT_API_KEY not configured on server")
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Missing X-API-Key header")
    if x_api_key != REPORT_API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")

    try:
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT trader_id, clickid_chatterfry, reg, dep
                    FROM users
                    WHERE id = %s
                """, (id,))
                result = cursor.fetchone()

        if not result:
            return {
                "status": "not_found",
                "user_id": id,
                "error": "User not found"
            }

        return {
            "status": "ok",
            "user_id": id,
            "trader_id": result[0],
            "clickid_chatterfry": result[1],
            "reg": bool(result[2]) if result[2] is not None else False,
            "dep": bool(result[3]) if result[3] is not None else False
        }

    except Exception as e:
        print(f"[POSTBACK USER_INFO] ✗ Exception: {e}")
        return {"status": "error", "error": str(e)}