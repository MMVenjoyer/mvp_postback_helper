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

    if not user_id:
        return {"status": "error", "error": "Missing user_id"}

    ACTION_TO_STATUS = {
        "reg": 2,
        "dep": 3,
        "redep": 4
    }

    try:
        status = ACTION_TO_STATUS.get(action, 1)
        db.update_started_chat(int(user_id), status)

        return {
            "status": "ok",
            "user_id": user_id,
            "new_status": status,
            "action": action or "started_chat"
        }

    except ValueError as ve:
        return {"status": "error", "error": str(ve)}
    except Exception as e:
        return {"status": "error", "error": str(e)}
