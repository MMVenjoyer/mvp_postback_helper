from fastapi import APIRouter, Request
from db import DataBase

db = DataBase()
router = APIRouter()


@router.get("/")
async def receive_postback(request: Request):
    """
    Принимает постбэки от MVP Project

    Поддерживаемые параметры:
    - user_id: Telegram ID пользователя (обязательный)
    - action: тип события (reg, dep, redep, или кастомная цель)
    - sum: сумма депозита/редепозита (для dep/redep)
    - playerid: MVP Player ID для сверки

    Примеры:
    - Регистрация: /postback?user_id=123456&action=reg
    - Депозит: /postback?user_id=123456&action=dep&sum=100.50&playerid=mvp_123
    - Редеп: /postback?user_id=123456&action=redep&sum=250.00&playerid=mvp_123
    - Кастомная цель: /postback?user_id=123456&action=payout&sum=50.00
    """
    data = dict(request.query_params)
    print(f"[POSTBACK] Получены данные: {data}")

    # Получаем параметры
    user_id = data.get("user_id")
    action = data.get("action")
    sum_amount = data.get("sum")
    playerid = data.get("playerid")

    # Валидация обязательных параметров
    if not user_id:
        print("[POSTBACK] ✗ Ошибка: отсутствует user_id")
        return {"status": "error", "error": "Missing user_id"}

    if not action:
        print("[POSTBACK] ✗ Ошибка: отсутствует action")
        return {"status": "error", "error": "Missing action"}

    try:
        # Преобразуем user_id в int
        user_id_int = int(user_id)
        print(f"[POSTBACK] user_id: {user_id_int}, action: {action}")

        # Безопасно преобразуем sum
        sum_float = None

        # Для reg - sum вообще не нужен, игнорируем
        if action == "reg":
            print(f"[POSTBACK] Регистрация - sum не требуется")
        else:
            # Для dep/redep/кастомных целей - пытаемся распарсить sum
            if sum_amount and sum_amount.strip():
                try:
                    sum_float = float(sum_amount)
                    print(f"[POSTBACK] sum: {sum_float}")
                except ValueError:
                    print(
                        f"[POSTBACK] ⚠️ Некорректный формат sum: '{sum_amount}' - устанавливаем None")
                    sum_float = None
            else:
                print(f"[POSTBACK] sum отсутствует или пустой - устанавливаем None")

        # Проверяем playerid если пришел
        if playerid:
            print(f"[POSTBACK] playerid: {playerid}")

        # Обрабатываем постбэк через новый метод БД
        result = db.process_postback(
            user_id=user_id_int,
            action=action,
            sum_amount=sum_float,
            raw_data=data  # Сохраняем все параметры в raw_data
        )

        if result.get("success"):
            print(f"[POSTBACK] ✓ Постбэк успешно обработан")
            print(f"[POSTBACK] Transaction ID: {result.get('transaction_id')}")
            print(f"[POSTBACK] User updated: {result.get('user_updated')}")

            return {
                "status": "ok",
                "user_id": user_id_int,
                "action": action,
                "transaction_id": result.get("transaction_id"),
                "sum": sum_float
            }
        else:
            print(f"[POSTBACK] ✗ Ошибка обработки: {result.get('error')}")
            return {
                "status": "error",
                "error": result.get("error")
            }

    except ValueError as ve:
        print(f"[POSTBACK] ✗ ValueError: {ve}")
        return {"status": "error", "error": f"Invalid parameter format: {str(ve)}"}

    except Exception as e:
        print(f"[POSTBACK] ✗ Exception: {e}")
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@router.get("/test/{user_id}")
async def test_postback(user_id: int):
    """
    Тестовый эндпоинт для проверки постбэков
    """
    try:
        # Получаем сводку по событиям пользователя
        events = db.get_user_events_summary(user_id)

        # Получаем последние транзакции
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
        return {
            "status": "ok",
            "stats": stats
        }
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
