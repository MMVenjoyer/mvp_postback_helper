#!/usr/bin/env python3
"""
Тестовый скрипт для проверки работы Telegram Logger
Использование: python test_telegram.py
"""
import asyncio
import sys
from logger_bot import send_error_log, send_success_log, send_warning_log
from config import BOT_TOKEN, CHAT_ID, ENABLE_TELEGRAM_LOGS


async def test_all_log_types():
    """Тестирует все типы логов"""

    print("=" * 60)
    print("ТЕСТИРОВАНИЕ TELEGRAM LOGGER")
    print("=" * 60)
    print()

    # Проверяем конфигурацию
    print("📋 Проверка конфигурации:")
    print(
        f"  • BOT_TOKEN: {'✓ Настроен' if BOT_TOKEN != 'your_bot_token_here' else '✗ НЕ НАСТРОЕН'}")
    print(
        f"  • CHAT_ID: {'✓ Настроен' if CHAT_ID != 'your_chat_id_here' else '✗ НЕ НАСТРОЕН'}")
    print(
        f"  • ENABLE_TELEGRAM_LOGS: {'✓ Включено' if ENABLE_TELEGRAM_LOGS else '✗ Выключено'}")
    print()

    if not ENABLE_TELEGRAM_LOGS:
        print("⚠️ ПРЕДУПРЕЖДЕНИЕ: Telegram логи выключены в конфиге!")
        print("   Установите ENABLE_TELEGRAM_LOGS=true в .env файле")
        return

    if BOT_TOKEN == 'your_bot_token_here' or CHAT_ID == 'your_chat_id_here':
        print("❌ ОШИБКА: Не настроены BOT_TOKEN или CHAT_ID!")
        print("   Настройте их в .env файле")
        return

    print("🧪 Начинаем тестирование...")
    print()

    # Тест 1: Успешный лог
    print("1️⃣ Отправка SUCCESS лога...")
    try:
        await send_success_log(
            log_type="TEST_SUCCESS",
            message="Это тестовое успешное сообщение",
            user_id=999999,
            additional_info={
                "test_type": "success_log",
                "timestamp": "2024-12-06 15:00:00"
            }
        )
        print("   ✓ SUCCESS лог отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    await asyncio.sleep(2)

    # Тест 2: Предупреждение
    print("2️⃣ Отправка WARNING лога...")
    try:
        await send_warning_log(
            warning_type="TEST_WARNING",
            message="Это тестовое предупреждение",
            user_id=888888,
            additional_info={
                "test_type": "warning_log",
                "warning_level": "medium"
            }
        )
        print("   ✓ WARNING лог отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    await asyncio.sleep(2)

    # Тест 3: Ошибка без traceback
    print("3️⃣ Отправка ERROR лога (без traceback)...")
    try:
        await send_error_log(
            error_type="TEST_ERROR_SIMPLE",
            error_message="Это простая тестовая ошибка без traceback",
            user_id=777777,
            additional_info={
                "test_type": "error_log_simple",
                "severity": "low"
            },
            full_traceback=False
        )
        print("   ✓ ERROR лог отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    await asyncio.sleep(2)

    # Тест 4: Ошибка с traceback
    print("4️⃣ Отправка ERROR лога (с traceback)...")
    try:
        # Создаем реальную ошибку для traceback
        try:
            result = 10 / 0
        except ZeroDivisionError:
            await send_error_log(
                error_type="TEST_ERROR_WITH_TRACEBACK",
                error_message="Это тестовая ошибка с полным traceback",
                user_id=666666,
                additional_info={
                    "test_type": "error_log_full",
                    "severity": "high",
                    "operation": "division_by_zero"
                },
                full_traceback=True
            )
        print("   ✓ ERROR лог с traceback отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    await asyncio.sleep(2)

    # Тест 5: Ошибка Keitaro HTTP
    print("5️⃣ Отправка KEITARO_HTTP_ERROR лога...")
    try:
        await send_error_log(
            error_type="KEITARO_HTTP_ERROR",
            error_message="HTTP 500 при отправке постбэка",
            user_id=555555,
            additional_info={
                "url": "https://ytgtech.com/e87f58c/postback?subid=test123&status=ftm&tid=4",
                "postback_type": "Keitaro_FTM",
                "status_code": 500,
                "response": "Internal Server Error",
                "attempts": 3
            },
            full_traceback=False
        )
        print("   ✓ KEITARO_HTTP_ERROR лог отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    await asyncio.sleep(2)

    # Тест 6: Ошибка постбэка
    print("6️⃣ Отправка POSTBACK_DEP_EXCEPTION лога...")
    try:
        try:
            # Имитируем ошибку
            data = {"sum": None}
            result = float(data["sum"]) * 2
        except (TypeError, KeyError) as e:
            await send_error_log(
                error_type="POSTBACK_DEP_EXCEPTION",
                error_message=f"Необработанная ошибка в DEP постбэке: {str(e)}",
                user_id=444444,
                additional_info={
                    "action": "dep",
                    "sum": "None",
                    "endpoint": "/postback/dep"
                },
                full_traceback=True
            )
        print("   ✓ POSTBACK_DEP_EXCEPTION лог отправлен")
    except Exception as e:
        print(f"   ✗ Ошибка: {e}")

    print()
    print("=" * 60)
    print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)
    print()
    print("📱 Проверьте ваш Telegram чат/группу!")
    print("   Вы должны были получить 6 сообщений:")
    print("   1. SUCCESS лог")
    print("   2. WARNING лог")
    print("   3. ERROR без traceback")
    print("   4. ERROR с traceback")
    print("   5. KEITARO_HTTP_ERROR")
    print("   6. POSTBACK_DEP_EXCEPTION")
    print()


async def test_bot_connection():
    """Тест подключения к боту"""
    import aiohttp

    print("🔍 Проверка подключения к боту...")

    if BOT_TOKEN == 'your_bot_token_here':
        print("❌ BOT_TOKEN не настроен!")
        return False

    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get("ok"):
                        bot_info = data.get("result", {})
                        print(f"✅ Бот подключен!")
                        print(f"   • Имя: @{bot_info.get('username')}")
                        print(f"   • ID: {bot_info.get('id')}")
                        print(f"   • Имя: {bot_info.get('first_name')}")
                        return True
                    else:
                        print(f"❌ Ошибка API: {data}")
                        return False
                else:
                    print(f"❌ HTTP ошибка: {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return False


async def main():
    """Главная функция"""

    # Сначала проверяем подключение
    connection_ok = await test_bot_connection()
    print()

    if not connection_ok:
        print("⚠️ Не удалось подключиться к боту. Проверьте BOT_TOKEN.")
        return

    # Если подключение ок, запускаем тесты
    await test_all_log_types()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n❌ Прервано пользователем")
        sys.exit(0)
