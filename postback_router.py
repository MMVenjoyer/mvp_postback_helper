"""
Postback Router - обработка постбэков от внешних систем

Поддерживаемые эндпоинты:
- /postback/ftm - First Time Message (первое сообщение)
- /postback/reg - Registration (регистрация)
- /postback/dep - Deposit (первый депозит)
- /postback/redep - Redeposit (повторный депозит)

Параметры запросов:
- id: Telegram User ID (обязательный)
- subscriber_id: UUID идентификатор (опциональный, для обратной совместимости)
- trader_id: ID трейдера из платформы (для reg)
- clickid: Click ID из трекера Chatterfry (опциональный)
- sum: Сумма депозита (для dep/redep)
- commission: Комиссия (для dep/redep)

Поиск пользователя происходит по всем доступным идентификаторам:
1. id (Telegram User ID) - приоритет
2. subscriber_id (UUID)
3. clickid (clickid_chatterfry)
4. trader_id
"""

from fastapi import APIRouter, Query
from typing import Optional
import re

from db import DataBase
from api_request import send_keitaro_postback, send_chatterfy_postback
from logger_bot import send_error_log
from config import ENABLE_TELEGRAM_LOGS

db = DataBase()
router = APIRouter()

# UUID regex pattern для валидации subscriber_id
UUID_PATTERN = re.compile(
    r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')


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
    Гарантирует существование пользователя и обновляет clickid если нужно.
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

    if result.get("existed") and clickid:
        db.update_user_clickid(actual_user_id, clickid)

    return result


async def find_user_for_deposit(
    user_id: int = None,
    subscriber_id: str = None,
    clickid: str = None,
    trader_id: str = None
) -> Optional[int]:
    """
    Ищет пользователя для операций deposit/redeposit.
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
    """
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
                "user_created": user_created
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

        if not subid:
            print(
                f"[POSTBACK FTM] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": id,
                "action": "ftm",
                "user_created": user_created,
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        print(
            f"[POSTBACK FTM] Отправляем постбэк в Keitaro для subid: {subid}, tid=4")
        keitaro_result = await send_keitaro_postback(subid=subid, status="ftm", tid=4, user_id=id)

        return {
            "status": "ok",
            "user_id": id,
            "action": "ftm",
            "user_created": user_created,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "tid": 4,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
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
    Регистрация пользователя
    """
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
        if user_created:
            print(f"[POSTBACK REG] ✓ Создан новый пользователь {id}")

        trader_saved = False
        if trader_id and not user_created:
            trader_result = db.update_user_trader_id(id, trader_id)
            trader_saved = trader_result.get("success", False)
            if trader_saved:
                print(
                    f"[POSTBACK REG] ✓ trader_id обновлен для user {id}: {trader_id}")

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
            "user_created": user_created
        }
        if trader_id:
            raw_data["trader_id"] = trader_id

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
                "trader_id": trader_id if (trader_saved or user_created) else None,
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
            "trader_id": trader_id if (trader_saved or user_created) else None,
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
    id: int = Query(None, description="Telegram User ID"),
    sum: str = Query(None, description="Deposit amount (default: 59)"),
    commission: str = Query(None, description="Commission amount"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(None, description="Trader ID (for search)")
):
    """
    Депозит пользователя (первый депозит)
    Отправляет событие SALE в Keitaro и постбэк в Chatterfy с суммой депозитов (event: sumdep)
    """
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

        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

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
                "user_created": user_created
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

        # Отправляем постбэк в Chatterfy (если есть clickid) - is_redep=False для DEP
        chatterfy_result = None
        if user_clickid:
            print(
                f"[POSTBACK DEP] Отправляем постбэк в Chatterfy: clickid={user_clickid}, sumdep={total_deposits_sum}, previous_dep={sum_value}, event=sumdep")
            chatterfy_result = await send_chatterfy_postback(
                clickid=user_clickid,
                sumdep=total_deposits_sum,
                previous_dep=sum_value,
                is_redep=False,  # DEP использует event "sumdep"
                user_id=actual_user_id
            )
        else:
            print(
                f"[POSTBACK DEP] ⚠️ clickid_chatterfry не найден для user {actual_user_id}, постбэк в Chatterfy не отправлен")

        if not subid:
            print(
                f"[POSTBACK DEP] ⚠️ sub_id не найден для user {actual_user_id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": actual_user_id,
                "action": "dep",
                "sum": sum_value,
                "commission": commission_value,
                "tid": tid_value,
                "user_created": user_created,
                "transaction_id": result.get("transaction_id"),
                "total_deposits_sum": total_deposits_sum,
                "keitaro_postback": "skipped - no subid",
                "chatterfy_postback": {
                    "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                    "clickid": user_clickid,
                    "event": "sumdep",
                    "sumdep": total_deposits_sum,
                    "previous_dep": sum_value,
                    "url": chatterfy_result.get("full_url") if chatterfy_result else None
                } if user_clickid else "skipped - no clickid"
            }

        print(
            f"[POSTBACK DEP] Отправляем постбэк SALE в Keitaro для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="sale",
            payout=sum_value,
            tid=tid_value,
            user_id=actual_user_id
        )

        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "dep",
            "sum": sum_value,
            "commission": commission_value,
            "tid": tid_value,
            "user_created": user_created,
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
            },
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
    id: int = Query(None, description="Telegram User ID"),
    sum: str = Query(None, description="Redeposit amount (default: 59)"),
    commission: str = Query(None, description="Commission amount"),
    clickid: str = Query(None, description="Click ID from Chatterfry tracker"),
    subscriber_id: str = Query(
        None, description="UUID subscriber ID (for backward compatibility)"),
    trader_id: str = Query(None, description="Trader ID (for search)")
):
    """
    Редепозит пользователя (повторный депозит)
    Отправляет событие DEP в Keitaro и постбэк в Chatterfy с суммой депозитов (event: sumdep_postback_rd)
    """
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

        if clickid:
            db.update_user_clickid(actual_user_id, clickid)

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
                "user_created": user_created
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

        # Отправляем постбэк в Chatterfy (если есть clickid) - is_redep=True для REDEP
        chatterfy_result = None
        if user_clickid:
            print(
                f"[POSTBACK REDEP] Отправляем постбэк в Chatterfy: clickid={user_clickid}, sumdep={total_deposits_sum}, previous_dep={sum_value}, event=sumdep_postback_rd")
            chatterfy_result = await send_chatterfy_postback(
                clickid=user_clickid,
                sumdep=total_deposits_sum,
                previous_dep=sum_value,
                is_redep=True,  # REDEP использует event "sumdep_postback_rd"
                user_id=actual_user_id
            )
        else:
            print(
                f"[POSTBACK REDEP] ⚠️ clickid_chatterfry не найден для user {actual_user_id}, постбэк в Chatterfy не отправлен")

        if not subid:
            print(
                f"[POSTBACK REDEP] ⚠️ sub_id не найден для user {actual_user_id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": actual_user_id,
                "action": "redep",
                "sum": sum_value,
                "commission": commission_value,
                "tid": tid_value,
                "user_created": user_created,
                "transaction_id": result.get("transaction_id"),
                "total_deposits_sum": total_deposits_sum,
                "keitaro_postback": "skipped - no subid",
                "chatterfy_postback": {
                    "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                    "clickid": user_clickid,
                    "event": "sumdep_postback_rd",
                    "sumdep": total_deposits_sum,
                    "previous_dep": sum_value,
                    "url": chatterfy_result.get("full_url") if chatterfy_result else None
                } if user_clickid else "skipped - no clickid"
            }

        print(
            f"[POSTBACK REDEP] Отправляем постбэк DEP в Keitaro для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="dep",
            payout=sum_value,
            tid=tid_value,
            user_id=actual_user_id
        )

        return {
            "status": "ok",
            "user_id": actual_user_id,
            "action": "redep",
            "sum": sum_value,
            "commission": commission_value,
            "tid": tid_value,
            "user_created": user_created,
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
            },
            "chatterfy_postback": {
                "sent": chatterfy_result.get("ok") if chatterfy_result else False,
                "clickid": user_clickid,
                "event": "sumdep_postback_rd",
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


# ====== ВСПОМОГАТЕЛЬНЫЕ ЭНДПОИНТЫ ======

@router.get("/test/{user_id}")
async def test_postback(user_id: int):
    """
    Тестовый эндпоинт для проверки данных пользователя
    """
    try:
        events = db.get_user_events_summary(user_id)
        transactions = db.get_user_transactions(user_id, limit=10)
        deposits_count = db.get_user_deposits_count(user_id)
        total_deposits_sum = db.get_user_total_deposits_sum(user_id)
        next_tid = 6 + deposits_count

        return {
            "status": "ok",
            "user_id": user_id,
            "events": events,
            "recent_transactions": transactions,
            "deposits_count": deposits_count,
            "total_deposits_sum": total_deposits_sum,
            "next_deposit_tid": next_tid
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/stats")
async def get_postback_stats():
    """
    Получить статистику по транзакциям
    """
    try:
        stats = db.get_transactions_stats()
        return {"status": "ok", "stats": stats}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/user/{user_id}/history")
async def get_user_history(user_id: int, limit: int = 50):
    """
    Получить историю транзакций пользователя
    """
    try:
        transactions = db.get_user_transactions(user_id, limit)
        events = db.get_user_events_summary(user_id)
        deposits_count = db.get_user_deposits_count(user_id)
        total_deposits_sum = db.get_user_total_deposits_sum(user_id)
        next_tid = 6 + deposits_count

        return {
            "status": "ok",
            "user_id": user_id,
            "events_summary": events,
            "transactions": transactions,
            "total_transactions": len(transactions),
            "deposits_count": deposits_count,
            "total_deposits_sum": total_deposits_sum,
            "next_deposit_tid": next_tid
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@router.get("/lookup")
async def lookup_user(
    id: int = Query(None, description="Telegram User ID"),
    subscriber_id: str = Query(None, description="UUID subscriber ID"),
    clickid: str = Query(None, description="Click ID from Chatterfry"),
    trader_id: str = Query(None, description="Trader ID")
):
    """
    Поиск пользователя по любому идентификатору.
    Поддерживает: id, subscriber_id, clickid, trader_id
    """
    if not id and not subscriber_id and not clickid and not trader_id:
        return {"status": "error", "error": "At least one identifier required: 'id', 'subscriber_id', 'clickid', or 'trader_id'"}

    try:
        found = db.find_user_by_any_identifier(
            user_id=id,
            subscriber_id=subscriber_id,
            clickid_chatterfry=clickid,
            trader_id=trader_id
        )

        if found:
            user_id = found.get("user_id")
            events = db.get_user_events_summary(user_id)
            total_deposits_sum = db.get_user_total_deposits_sum(user_id)
            return {
                "status": "ok",
                "found": True,
                "found_by": found.get("found_by"),
                "user_id": user_id,
                "events": events,
                "total_deposits_sum": total_deposits_sum
            }
        else:
            return {
                "status": "ok",
                "found": False,
                "searched_id": id,
                "searched_subscriber_id": subscriber_id,
                "searched_clickid": clickid,
                "searched_trader_id": trader_id
            }
    except Exception as e:
        return {"status": "error", "error": str(e)}
