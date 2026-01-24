"""
Роутер для Telegram Mini App
Принимает данные об открытии калькулятора
"""
from fastapi import APIRouter, Request
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
from db import DataBase

router = APIRouter()


class MiniAppOpenRequest(BaseModel):
    """Данные от Telegram Mini App при открытии"""
    user_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language_code: Optional[str] = None


@router.post("/get_miniapp")
async def track_miniapp_open(data: MiniAppOpenRequest):
    """
    Фиксирует открытие Mini App калькулятора пользователем.
    Записывает timestamp в поле is_open_calc.
    """
    try:
        db = DataBase()

        # Обновляем timestamp открытия калькулятора
        result = db.update_calc_opened(
            user_id=data.user_id,
            username=data.username,
            first_name=data.first_name,
            last_name=data.last_name,
            language_code=data.language_code
        )

        if result.get("success"):
            print(
                f"[MINIAPP] ✓ Открытие калькулятора: user_id={data.user_id}, username={data.username}")
            return {
                "status": "ok",
                "user_id": data.user_id,
                "recorded_at": result.get("timestamp"),
                "is_new_user": result.get("created", False)
            }
        else:
            print(f"[MINIAPP] ✗ Ошибка записи: user_id={data.user_id}")
            return {
                "status": "error",
                "message": result.get("error", "Unknown error")
            }

    except Exception as e:
        print(f"[MINIAPP] ✗ Exception: {e}")
        return {
            "status": "error",
            "message": str(e)
        }


@router.get("/calc_stats")
async def get_calc_stats():
    """
    Статистика по открытиям калькулятора
    """
    try:
        db = DataBase()
        stats = db.get_calc_open_stats()
        return {
            "status": "ok",
            "stats": stats
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }
