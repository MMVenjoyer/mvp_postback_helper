"""
Telegram Logger Bot
Отправляет логи ошибок в Telegram группу/чат
"""
import asyncio
from datetime import datetime
from typing import Optional
import traceback
from aiogram import Bot
from aiogram.types import ParseMode
from config import BOT_TOKEN, CHAT_ID

# Глобальный экземпляр бота
_bot_instance: Optional[Bot] = None


def get_bot() -> Optional[Bot]:
    """Получить экземпляр бота"""
    global _bot_instance
    if _bot_instance is None and BOT_TOKEN and BOT_TOKEN != "your_bot_token_here":
        try:
            _bot_instance = Bot(token=BOT_TOKEN)
        except Exception as e:
            print(f"[TELEGRAM BOT] ✗ Ошибка инициализации бота: {e}")
    return _bot_instance


async def send_error_log(
    error_type: str,
    error_message: str,
    user_id: Optional[int] = None,
    additional_info: Optional[dict] = None,
    full_traceback: bool = True
):
    """
    Отправляет лог ошибки в Telegram

    Args:
        error_type: Тип ошибки (например, "POSTBACK_ERROR", "KEITARO_ERROR")
        error_message: Сообщение об ошибке
        user_id: ID пользователя (опционально)
        additional_info: Дополнительная информация (опционально)
        full_traceback: Отправлять ли полный traceback
    """
    bot = get_bot()

    if not bot or not CHAT_ID or CHAT_ID == "your_chat_id_here":
        print(
            f"[TELEGRAM BOT] ⚠️ Бот не настроен, пропускаем отправку: {error_type}")
        return

    try:
        # Формируем сообщение
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_parts = [
            f"🔴 <b>{error_type}</b>",
            f"🕐 {timestamp}",
            f"",
            f"<b>Ошибка:</b>",
            f"<code>{error_message}</code>"
        ]

        if user_id:
            message_parts.insert(2, f"👤 User ID: <code>{user_id}</code>")

        if additional_info:
            message_parts.append("")
            message_parts.append("<b>Дополнительно:</b>")
            for key, value in additional_info.items():
                message_parts.append(f"  • {key}: <code>{value}</code>")

        if full_traceback:
            tb = traceback.format_exc()
            if tb and tb != "NoneType: None\n":
                # Ограничиваем длину traceback для Telegram (макс 4096 символов)
                if len(tb) > 2000:
                    tb = tb[-2000:]
                message_parts.append("")
                message_parts.append("<b>Traceback:</b>")
                message_parts.append(f"<pre>{tb}</pre>")

        message = "\n".join(message_parts)

        # Telegram имеет лимит 4096 символов
        if len(message) > 4096:
            message = message[:4090] + "\n...</pre>"

        await bot.send_message(
            chat_id=CHAT_ID,
            text=message,
            parse_mode=ParseMode.HTML
        )

        print(f"[TELEGRAM BOT] ✓ Лог отправлен: {error_type}")

    except Exception as e:
        print(f"[TELEGRAM BOT] ✗ Ошибка отправки лога: {e}")
        traceback.print_exc()


async def send_success_log(
    log_type: str,
    message: str,
    user_id: Optional[int] = None,
    additional_info: Optional[dict] = None
):
    """
    Отправляет успешный лог в Telegram (опционально)

    Args:
        log_type: Тип лога (например, "SYNC_COMPLETE")
        message: Сообщение
        user_id: ID пользователя (опционально)
        additional_info: Дополнительная информация (опционально)
    """
    bot = get_bot()

    if not bot or not CHAT_ID or CHAT_ID == "your_chat_id_here":
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_parts = [
            f"✅ <b>{log_type}</b>",
            f"🕐 {timestamp}",
            f"",
            message
        ]

        if user_id:
            message_parts.insert(2, f"👤 User ID: <code>{user_id}</code>")

        if additional_info:
            message_parts.append("")
            for key, value in additional_info.items():
                message_parts.append(f"  • {key}: <code>{value}</code>")

        text = "\n".join(message_parts)

        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        print(f"[TELEGRAM BOT] ✗ Ошибка отправки success лога: {e}")


async def send_warning_log(
    warning_type: str,
    message: str,
    user_id: Optional[int] = None,
    additional_info: Optional[dict] = None
):
    """
    Отправляет предупреждение в Telegram

    Args:
        warning_type: Тип предупреждения
        message: Сообщение
        user_id: ID пользователя (опционально)
        additional_info: Дополнительная информация (опционально)
    """
    bot = get_bot()

    if not bot or not CHAT_ID or CHAT_ID == "your_chat_id_here":
        return

    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        message_parts = [
            f"⚠️ <b>{warning_type}</b>",
            f"🕐 {timestamp}",
            f"",
            message
        ]

        if user_id:
            message_parts.insert(2, f"👤 User ID: <code>{user_id}</code>")

        if additional_info:
            message_parts.append("")
            for key, value in additional_info.items():
                message_parts.append(f"  • {key}: <code>{value}</code>")

        text = "\n".join(message_parts)

        await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML
        )

    except Exception as e:
        print(f"[TELEGRAM BOT] ✗ Ошибка отправки warning лога: {e}")


async def close_bot():
    """Закрывает сессию бота"""
    global _bot_instance
    if _bot_instance:
        try:
            await _bot_instance.close()
            print("[TELEGRAM BOT] ✓ Сессия закрыта")
        except Exception as e:
            print(f"[TELEGRAM BOT] ✗ Ошибка закрытия сессии: {e}")
        finally:
            _bot_instance = None


# Удобные обёртки для синхронного кода
def sync_send_error_log(*args, **kwargs):
    """Синхронная обёртка для отправки ошибок"""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.create_task(send_error_log(*args, **kwargs))
        else:
            loop.run_until_complete(send_error_log(*args, **kwargs))
    except Exception as e:
        print(f"[TELEGRAM BOT] ✗ Ошибка в sync_send_error_log: {e}")
