from fastapi import APIRouter, Request
from db import DataBase

db = DataBase()
router = APIRouter()


@router.get("/")
async def receive_postback(request: Request):
    data = dict(request.query_params)
    print(f"[POSTBACK] Получены данные: {data}")

    user_id = data.get("user_id")
    action = data.get("action")

    print(
        f"[POSTBACK] user_id: '{user_id}' (тип: {type(user_id)}), action: {action}")

    if not user_id:
        print("[POSTBACK] Ошибка: отсутствует user_id")
        return {"status": "error", "error": "Missing user_id"}

    ACTION_TO_STATUS = {
        "reg": 2,
        "dep": 3,
        "redep": 4
    }

    try:
        # Явно преобразуем в int и логируем
        user_id_int = int(user_id)
        print(
            f"[POSTBACK] Преобразованный user_id: {user_id_int} (тип: {type(user_id_int)})")

        status = ACTION_TO_STATUS.get(action, 1)
        print(
            f"[POSTBACK] Устанавливаем статус {status} для пользователя {user_id_int}")

        result = db.update_started_chat(user_id_int, status)
        print(f"[POSTBACK] Результат обновления БД: {result}")

        return {
            "status": "ok",
            "user_id": user_id,
            "new_status": status,
            "action": action or "started_chat"
        }

    except ValueError as ve:
        print(f"[POSTBACK] ValueError: {ve}")
        return {"status": "error", "error": str(ve)}
    except Exception as e:
        print(f"[POSTBACK] Exception: {e}")
        return {"status": "error", "error": str(e)}
