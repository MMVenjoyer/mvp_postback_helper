from fastapi import APIRouter, Request, Query
from db import DataBase
from api_request import send_keitaro_postback

db = DataBase()
router = APIRouter()


@router.get("/ftm")
async def ftm_postback(id: int = Query(..., description="User ID")):
    """
    FTM (First Time Message) постбэк

    Параметры:
    - id: Telegram ID пользователя

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
            print(
                f"[POSTBACK FTM] ✗ Ошибка записи в БД: {result.get('error')}")
            return {"status": "error", "error": result.get("error")}

        print(f"[POSTBACK FTM] ✓ Записано в БД для user {id}")

        # 2. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK FTM] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": id,
                "action": "ftm",
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 3. Отправляем постбэк в Keitaro
        print(
            f"[POSTBACK FTM] Отправляем постбэк в Keitaro для subid: {subid}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="ftm",
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
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK FTM] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@router.get("/reg")
async def reg_postback(id: int = Query(..., description="User ID")):
    """
    Регистрация пользователя

    Параметры:
    - id: Telegram ID пользователя

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
            print(
                f"[POSTBACK REG] ✗ Ошибка записи в БД: {result.get('error')}")
            return {"status": "error", "error": result.get("error")}

        print(f"[POSTBACK REG] ✓ Записано в БД для user {id}")

        # 2. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK REG] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": id,
                "action": "reg",
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 3. Отправляем постбэк в Keitaro
        print(
            f"[POSTBACK REG] Отправляем постбэк в Keitaro для subid: {subid}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="reg",
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
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK REG] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@router.get("/dep")
async def dep_postback(
    id: int = Query(..., description="User ID"),
    sum: float = Query(..., description="Deposit amount")
):
    """
    Депозит пользователя

    Параметры:
    - id: Telegram ID пользователя
    - sum: Сумма депозита

    Пример: /postback/dep?id=123456&sum=100.50
    """
    print(f"[POSTBACK DEP] id: {id}, sum: {sum}")

    try:
        # 1. Записываем в БД
        result = db.process_postback(
            user_id=id,
            action="dep",
            sum_amount=sum,
            raw_data={"id": id, "action": "dep", "sum": sum}
        )

        if not result.get("success"):
            print(
                f"[POSTBACK DEP] ✗ Ошибка записи в БД: {result.get('error')}")
            return {"status": "error", "error": result.get("error")}

        print(f"[POSTBACK DEP] ✓ Записано в БД для user {id}, sum={sum}")

        # 2. Получаем sub_3 (subid) из БД
        subid = db.get_user_sub_id(id)

        if not subid:
            print(
                f"[POSTBACK DEP] ⚠️ sub_id не найден для user {id}, постбэк в Keitaro не отправлен")
            return {
                "status": "ok",
                "user_id": id,
                "action": "dep",
                "sum": sum,
                "transaction_id": result.get("transaction_id"),
                "keitaro_postback": "skipped - no subid"
            }

        # 3. Отправляем постбэк в Keitaro с суммой
        print(
            f"[POSTBACK DEP] Отправляем постбэк в Keitaro для subid: {subid}, payout: {sum}")
        keitaro_result = await send_keitaro_postback(
            subid=subid,
            status="dep",
            payout=sum,
            user_id=id
        )

        return {
            "status": "ok",
            "user_id": id,
            "action": "dep",
            "sum": sum,
            "transaction_id": result.get("transaction_id"),
            "keitaro_postback": {
                "sent": keitaro_result.get("ok"),
                "subid": subid,
                "payout": sum,
                "url": keitaro_result.get("full_url"),
                "response": keitaro_result.get("text")[:100] if keitaro_result.get("text") else None
            }
        }

    except Exception as e:
        print(f"[POSTBACK DEP] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@router.get("/redep")
async def redep_postback(
    id: int = Query(..., description="User ID"),
    sum: float = Query(..., description="Redeposit amount")
):
    """
    Редепозит пользователя

    Параметры:
    - id: Telegram ID пользователя
    - sum: Сумма редепозита

    Пример: /postback/redep?id=123456&sum=250.00
    """
    print(f"[POSTBACK REDEP] id: {id}, sum: {sum}")

    try:
        result = db.process_postback(
            user_id=id,
            action="redep",
            sum_amount=sum,
            raw_data={"id": id, "action": "redep", "sum": sum}
        )

        if result.get("success"):
            print(
                f"[POSTBACK REDEP] ✓ Успешно обработан для user {id}, sum={sum}")
            return {
                "status": "ok",
                "user_id": id,
                "action": "redep",
                "sum": sum,
                "transaction_id": result.get("transaction_id")
            }
        else:
            print(f"[POSTBACK REDEP] ✗ Ошибка: {result.get('error')}")
            return {"status": "error", "error": result.get("error")}

    except Exception as e:
        print(f"[POSTBACK REDEP] ✗ Exception: {e}")
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

        return {
            "status": "ok",
            "user_id": user_id,
            "events": events,
            "recent_transactions": transactions
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

        return {
            "status": "ok",
            "user_id": user_id,
            "events_summary": events,
            "transactions": transactions,
            "total_transactions": len(transactions)
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
