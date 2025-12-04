import aiohttp
import asyncio
from datetime import datetime
from config import *
from typing import Optional
from urllib.parse import urlencode


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

                        # Логируем HTTP ошибку если есть bot
                        if bot and postback_type and user_id:
                            print('pass -1')

        except asyncio.TimeoutError:
            last_exception = Exception("Таймаут запроса (30 сек)")
            if bot and postback_type and user_id and attempt == retries:
                print('pass 0')

        except aiohttp.ClientError as e:
            last_exception = Exception(f"Ошибка клиента: {str(e)}")
            if bot and postback_type and user_id and attempt == retries:
                print('pass 1')

        except Exception as e:
            last_exception = Exception(f"Неизвестная ошибка: {str(e)}")
            if bot and postback_type and user_id and attempt == retries:
                print('pass 2')

        # Ждём перед следующей попыткой
        if attempt < retries:
            wait_time = delay * attempt
            await asyncio.sleep(wait_time)

    # Финальная ошибка после всех попыток
    if bot and postback_type and user_id:
        print('pass 3')

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
