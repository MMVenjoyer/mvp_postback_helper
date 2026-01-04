import aiohttp
import asyncio
from datetime import datetime
from config import *
from typing import Optional
from urllib.parse import urlencode
from logger_bot import send_error_log


async def fetch_with_retry(url, params=None, retries=3, delay=60, bot=None, postback_type=None, user_id=None):
    """
    Отправка HTTP запроса с повторными попытками и логированием ошибок
    """
    start_time = datetime.now()
    last_exception = None

    # Формируем полный URL для логирования
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"

    for attempt in range(1, retries + 1):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params, timeout=30) as resp:
                    text = await resp.text()

                    if resp.status == 200:
                        return {
                            "ok": True,
                            "status": resp.status,
                            "text": text,
                            "attempt": attempt,
                            "error_type": None,
                            "timestamp": start_time.strftime('%H:%M:%S'),
                            "duration": (datetime.now() - start_time).total_seconds(),
                            "full_url": full_url
                        }
                    else:
                        last_exception = Exception(
                            f"HTTP {resp.status}: {text[:200]}...")

                        # Логируем HTTP ошибку если есть bot и это последняя попытка
                        if attempt == retries and ENABLE_TELEGRAM_LOGS:
                            await send_error_log(
                                error_type="KEITARO_HTTP_ERROR",
                                error_message=f"HTTP {resp.status} при отправке постбэка",
                                user_id=user_id,
                                additional_info={
                                    "url": full_url,
                                    "postback_type": postback_type,
                                    "status_code": resp.status,
                                    "response": text[:200],
                                    "attempts": attempt
                                },
                                full_traceback=False
                            )

        except asyncio.TimeoutError:
            last_exception = Exception("Таймаут запроса (30 сек)")
            if attempt == retries and ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="KEITARO_TIMEOUT",
                    error_message="Превышено время ожидания ответа от Keitaro",
                    user_id=user_id,
                    additional_info={
                        "url": full_url,
                        "postback_type": postback_type,
                        "timeout": "30 сек",
                        "attempts": attempt
                    },
                    full_traceback=False
                )

        except aiohttp.ClientError as e:
            last_exception = Exception(f"Ошибка клиента: {str(e)}")
            if attempt == retries and ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="KEITARO_CLIENT_ERROR",
                    error_message=f"Ошибка HTTP клиента: {str(e)}",
                    user_id=user_id,
                    additional_info={
                        "url": full_url,
                        "postback_type": postback_type,
                        "attempts": attempt
                    },
                    full_traceback=True
                )

        except Exception as e:
            last_exception = Exception(f"Неизвестная ошибка: {str(e)}")
            if attempt == retries and ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="KEITARO_UNKNOWN_ERROR",
                    error_message=f"Неизвестная ошибка при отправке постбэка: {str(e)}",
                    user_id=user_id,
                    additional_info={
                        "url": full_url,
                        "postback_type": postback_type,
                        "attempts": attempt
                    },
                    full_traceback=True
                )

        # Ждём перед следующей попыткой
        if attempt < retries:
            wait_time = delay * attempt
            await asyncio.sleep(wait_time)

    # Финальная ошибка после всех попыток
    if ENABLE_TELEGRAM_LOGS:
        await send_error_log(
            error_type="KEITARO_POSTBACK_FAILED",
            error_message=f"Не удалось отправить постбэк после {retries} попыток",
            user_id=user_id,
            additional_info={
                "url": full_url,
                "postback_type": postback_type,
                "last_error": str(last_exception),
                "total_attempts": retries
            },
            full_traceback=False
        )

    return {
        "ok": False,
        "status": getattr(last_exception, 'status', None),
        "text": str(last_exception),
        "attempt": retries,
        "error_type": type(last_exception).__name__,
        "timestamp": start_time.strftime('%H:%M:%S'),
        "duration": (datetime.now() - start_time).total_seconds(),
        "full_url": full_url
    }


async def send_keitaro_postback(subid: str, status: str, payout: float = None, tid: int = None, retries=3, delay=60, bot=None, user_id=None):
    """
    Постбэк в Keitaro
    URL: https://ytgtech.com/e87f58c/postback?subid=XXX&status=ftm&payout=100&tid=4

    Параметры:
    - subid: sub_3 из БД
    - status: ftm, reg, dep
    - payout: сумма (опционально, для dep)
    - tid: ID цели (ftm=4, reg=5, dep=6+)
    """
    from config import KEITARO_POSTBACK_URL

    params = {
        "subid": subid,
        "status": status
    }

    # Добавляем payout только если он передан
    if payout is not None:
        params["payout"] = payout

    # Добавляем tid только если он передан
    if tid is not None:
        params["tid"] = tid

    result = await fetch_with_retry(
        KEITARO_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=bot,
        postback_type=f"Keitaro_{status.upper()}",
        user_id=user_id
    )
    result["postback_type"] = f"Keitaro {status.upper()}"

    print(f"📤 Постбэк Keitaro ({status}): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")

    return result


async def send_chatterfy_postback(
    clickid: str,
    sumdep: float,
    previous_dep: float,
    retries: int = 3,
    delay: int = 60,
    user_id: int = None
):
    """
    Постбэк в Chatterfy для отправки информации о депозитах
    URL: https://api.chatterfy.ai/api/postbacks/3bdc8be1-76d1-4312-9842-c68e7f88f9c8/tracker-postback

    Параметры:
    - clickid: clickid_chatterfry из БД
    - sumdep: общая сумма всех депозитов пользователя
    - previous_dep: сумма текущей транзакции
    """
    from config import CHATTERFY_POSTBACK_URL

    params = {
        "tracker.event": "sumdep",
        "clickid": clickid,
        "fields.sumdep": sumdep,
        "fields.previous_dep": previous_dep
    }

    result = await fetch_with_retry(
        CHATTERFY_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=None,
        postback_type="Chatterfy_SUMDEP",
        user_id=user_id
    )
    result["postback_type"] = "Chatterfy SUMDEP"

    print(f"📤 Постбэк Chatterfy (sumdep): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")

    return result
