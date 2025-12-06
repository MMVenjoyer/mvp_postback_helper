from fastapi import APIRouter, Request, Query
from db import DataBase
from api_request import send_keitaro_postback
from logger_bot import send_error_log
from config import ENABLE_TELEGRAM_LOGS

db = DataBase()
router = APIRouter()


def parse_sum_parameter(sum_value) -> float:
    """
    Безопасно парсит параметр sum.
    Если значение не может быть преобразовано в число или <= 0, возвращает 59.

    Обрабатывает:
    - None -> 59
    - Пустая строка -> 59
    - "{sumdep}" или любую непарсящуюся строку -> 59
    - Число <= 0 -> 59
    - Валидное число > 0 -> это число

    Args:
        sum_value: значение параметра sum (может быть float, str, None)

    Returns:
        float: либо переданное значение, либо 59
    """
    DEFAULT_SUM = 59.0

    # Если None или пустая строка
    if sum_value is None or sum_value == "":
        return DEFAULT_SUM

    # Если это уже число
    if isinstance(sum_value, (int, float)):
        return DEFAULT_SUM if sum_value <= 0 else float(sum_value)

    # Пытаемся преобразовать строку в число
    try:
        parsed = float(sum_value)
        return DEFAULT_SUM if parsed <= 0 else parsed
    except (ValueError, TypeError):
        # Строка не парсится (например "{sumdep}")
        return DEFAULT_SUM


@router.get("/ftm")
async def ftm_postback(id: int = Query(..., description="User ID")):
    """
    FTM (First Time Message) постбэк

    Параметры:
    - id: Telegram ID пользователя
    - tid: автоматически 4

    Пример: /postback/ftm?id=123456
    """
    print(f"[POSTBACK FTM] id: {id}")

    try:
        # 1. Записываем в БД
        result = db.process_postback(
            user_id=id,
            action="ftm",
            sum_amount=None,
            raw_data={"id": id, "action": "ftm"}
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK FTM] ✗ Ошибка записи в БД: {error_msg}")

            # Отправляем в Telegram только если это НЕ "User not found"
            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи FTM в БД: {error_msg}",
                    user_id=id,
                    additional_info={
                        "action": "ftm",
                        "endpoint": "/postback/ftm"
                    },
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK FTM] ✓ Записано в БД для user {id}")

        # 2. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK FTM] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            # НЕ отправляем в Telegram - это ожидаемая ситуация
            return {
                "status": "ok",
                "user_id": id,
                "action": "ftm",
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 3. Отправляем постбэк в Keitaro с tid=4
        print(
            f"[POSTBACK FTM] Отправляем постбэк в Keitaro для subid: {subid}, tid=4")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="ftm",
            tid=4,
            user_id=id
        )

        return {
            "status": "ok",
            "user_id": id,
            "action": "ftm",
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

        # Отправляем в Telegram
        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_FTM_EXCEPTION",
                error_message=f"Необработанная ошибка в FTM постбэке: {str(e)}",
                user_id=id,
                additional_info={
                    "action": "ftm",
                    "endpoint": "/postback/ftm"
                },
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/reg")
async def reg_postback(id: int = Query(..., description="User ID")):
    """
    Регистрация пользователя

    Параметры:
    - id: Telegram ID пользователя
    - tid: автоматически 5

    Пример: /postback/reg?id=123456
    """
    print(f"[POSTBACK REG] id: {id}")

    try:
        # 1. Записываем в БД
        result = db.process_postback(
            user_id=id,
            action="reg",
            sum_amount=None,
            raw_data={"id": id, "action": "reg"}
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK REG] ✗ Ошибка записи в БД: {error_msg}")

            # Отправляем в Telegram только если это НЕ "User not found"
            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи REG в БД: {error_msg}",
                    user_id=id,
                    additional_info={
                        "action": "reg",
                        "endpoint": "/postback/reg"
                    },
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK REG] ✓ Записано в БД для user {id}")

        # 2. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK REG] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            # НЕ отправляем в Telegram - это ожидаемая ситуация
            return {
                "status": "ok",
                "user_id": id,
                "action": "reg",
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 3. Отправляем постбэк в Keitaro с tid=5
        print(
            f"[POSTBACK REG] Отправляем постбэк в Keitaro для subid: {subid}, tid=5")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="reg",
            tid=5,
            user_id=id
        )

        return {
            "status": "ok",
            "user_id": id,
            "action": "reg",
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

        # Отправляем в Telegram
        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_REG_EXCEPTION",
                error_message=f"Необработанная ошибка в REG постбэке: {str(e)}",
                user_id=id,
                additional_info={
                    "action": "reg",
                    "endpoint": "/postback/reg"
                },
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/dep")
async def dep_postback(
    id: int = Query(..., description="User ID"),
    sum: str = Query(None, description="Deposit amount (default: 59)")
):
    """
    Депозит пользователя

    Параметры:
    - id: Telegram ID пользователя
    - sum: Сумма депозита (если не указана или невалидна, используется 59)
    - tid: автоматически 6 + количество предыдущих депозитов

    Примеры: 
    - /postback/dep?id=123456&sum=100.50
    - /postback/dep?id=123456&sum={sumdep} (будет 59)
    - /postback/dep?id=123456 (будет 59)
    """
    # Парсим и валидируем сумму
    sum_value = parse_sum_parameter(sum)
    print(
        f"[POSTBACK DEP] id: {id}, sum_raw: {sum!r}, sum_parsed: {sum_value}")

    try:
        # 1. Получаем количество предыдущих депозитов ДО записи новой транзакции
        previous_deposits = db.get_user_deposits_count(id)
        tid_value = 6 + previous_deposits
        print(
            f"[POSTBACK DEP] Предыдущих депозитов: {previous_deposits}, tid будет: {tid_value}")

        # 2. Записываем в БД
        result = db.process_postback(
            user_id=id,
            action="dep",
            sum_amount=sum_value,
            raw_data={"id": id, "action": "dep",
                      "sum": sum_value, "tid": tid_value}
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK DEP] ✗ Ошибка записи в БД: {error_msg}")

            # Отправляем в Telegram только если это НЕ "User not found"
            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи DEP в БД: {error_msg}",
                    user_id=id,
                    additional_info={
                        "action": "dep",
                        "sum": sum_value,
                        "tid": tid_value,
                        "endpoint": "/postback/dep"
                    },
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(f"[POSTBACK DEP] ✓ Записано в БД для user {id}, sum={sum_value}")

        # 3. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK DEP] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            # НЕ отправляем в Telegram - это ожидаемая ситуация
            return {
                "status": "ok",
                "user_id": id,
                "action": "dep",
                "sum": sum_value,
                "tid": tid_value,
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 4. Отправляем постбэк в Keitaro с суммой и tid
        print(
            f"[POSTBACK DEP] Отправляем постбэк в Keitaro для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="dep",
            payout=sum_value,
            tid=tid_value,
            user_id=id
        )

        return {
            "status": "ok",
            "user_id": id,
            "action": "dep",
            "sum": sum_value,
            "tid": tid_value,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "payout": sum_value,
                "tid": tid_value,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK DEP] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        # Отправляем в Telegram
        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_DEP_EXCEPTION",
                error_message=f"Необработанная ошибка в DEP постбэке: {str(e)}",
                user_id=id,
                additional_info={
                    "action": "dep",
                    "sum": sum,
                    "endpoint": "/postback/dep"
                },
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


@router.get("/redep")
async def redep_postback(
    id: int = Query(..., description="User ID"),
    sum: str = Query(None, description="Redeposit amount (default: 59)")
):
    """
    Редепозит пользователя (отправляется в Keitaro как dep с суммой и tid)

    Параметры:
    - id: Telegram ID пользователя
    - sum: Сумма редепозита (если не указана или невалидна, используется 59)
    - tid: автоматически 6 + количество предыдущих депозитов (включая dep и redep)

    Примеры:
    - /postback/redep?id=123456&sum=250.00
    - /postback/redep?id=123456&sum={sumdep} (будет 59)
    - /postback/redep?id=123456 (будет 59)
    """
    # Парсим и валидируем сумму
    sum_value = parse_sum_parameter(sum)
    print(
        f"[POSTBACK REDEP] id: {id}, sum_raw: {sum!r}, sum_parsed: {sum_value}")

    try:
        # 1. Получаем количество предыдущих депозитов ДО записи новой транзакции
        previous_deposits = db.get_user_deposits_count(id)
        tid_value = 6 + previous_deposits
        print(
            f"[POSTBACK REDEP] Предыдущих депозитов: {previous_deposits}, tid будет: {tid_value}")

        # 2. Записываем в БД как redep
        result = db.process_postback(
            user_id=id,
            action="redep",
            sum_amount=sum_value,
            raw_data={"id": id, "action": "redep",
                      "sum": sum_value, "tid": tid_value}
        )

        if not result.get("success"):
            error_msg = result.get('error', 'Unknown error')
            print(f"[POSTBACK REDEP] ✗ Ошибка записи в БД: {error_msg}")

            # Отправляем в Telegram только если это НЕ "User not found"
            if ENABLE_TELEGRAM_LOGS and "not found" not in error_msg.lower():
                await send_error_log(
                    error_type="POSTBACK_DB_ERROR",
                    error_message=f"Ошибка записи REDEP в БД: {error_msg}",
                    user_id=id,
                    additional_info={
                        "action": "redep",
                        "sum": sum_value,
                        "tid": tid_value,
                        "endpoint": "/postback/redep"
                    },
                    full_traceback=True
                )

            return {"status": "error", "error": error_msg}

        print(
            f"[POSTBACK REDEP] ✓ Записано в БД для user {id}, sum={sum_value}")

        # 3. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK REDEP] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            # НЕ отправляем в Telegram - это ожидаемая ситуация
            return {
                "status": "ok",
                "user_id": id,
                "action": "redep",
                "sum": sum_value,
                "tid": tid_value,
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 4. Отправляем постбэк в Keitaro как dep (согласно ТЗ: dep и redep идут как dep)
        print(
            f"[POSTBACK REDEP] Отправляем постбэк в Keitaro (как dep) для subid: {subid}, payout: {sum_value}, tid: {tid_value}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="dep",  # отправляем как dep
            payout=sum_value,
            tid=tid_value,
            user_id=id
        )

        return {
            "status": "ok",
            "user_id": id,
            "action": "redep",
            "sum": sum_value,
            "tid": tid_value,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "status_sent": "dep",  # указываем что отправили как dep
                "payout": sum_value,
                "tid": tid_value,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK REDEP] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()

        # Отправляем в Telegram
        if ENABLE_TELEGRAM_LOGS:
            await send_error_log(
                error_type="POSTBACK_REDEP_EXCEPTION",
                error_message=f"Необработанная ошибка в REDEP постбэке: {str(e)}",
                user_id=id,
                additional_info={
                    "action": "redep",
                    "sum": sum,
                    "endpoint": "/postback/redep"
                },
                full_traceback=True
            )

        return {"status": "error", "error": str(e)}


# ====== ВСПОМОГАТЕЛЬНЫЕ ЭНДПОИНТЫ ======

@router.get("/test/{user_id}")
async def test_postback(user_id: int):
    """
    Тестовый эндпоинт для проверки постбэков
    """
    try:
        events = db.get_user_events_summary(user_id)
        transactions = db.get_user_transactions(user_id, limit=10)
        deposits_count = db.get_user_deposits_count(user_id)
        next_tid = 6 + deposits_count

        return {
            "status": "ok",
            "user_id": user_id,
            "events": events,
            "recent_transactions": transactions,
            "deposits_count": deposits_count,
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
        next_tid = 6 + deposits_count

        return {
            "status": "ok",
            "user_id": user_id,
            "events_summary": events,
            "transactions": transactions,
            "total_transactions": len(transactions),
            "deposits_count": deposits_count,
            "next_deposit_tid": next_tid
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
