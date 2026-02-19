import aiohttp
import asyncio
from datetime import datetime
from config import *
from typing import Optional
from urllib.parse import urlencode
from logger_bot import send_error_log


# ==========================================
# SHARED HTTP SESSION (один на воркер-процесс)
# ==========================================
_http_session: Optional[aiohttp.ClientSession] = None


async def get_http_session() -> aiohttp.ClientSession:
    """
    Получает или создает shared HTTP сессию для текущего воркера.
    Переиспользует TCP соединения вместо создания новых на каждый запрос.
    """
    global _http_session
    if _http_session is None or _http_session.closed:
        connector = aiohttp.TCPConnector(
            limit=20,              # макс одновременных соединений
            keepalive_timeout=30,  # держим соединения открытыми 30с
            enable_cleanup_closed=True
        )
        _http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=10)  # 10с глобальный таймаут
        )
    return _http_session


async def close_http_session():
    """
    Закрывает HTTP сессию (вызывается при shutdown приложения)
    """
    global _http_session
    if _http_session and not _http_session.closed:
        await _http_session.close()
        _http_session = None
        print("[HTTP] ✓ HTTP сессия закрыта")


async def fetch_with_retry(url, params=None, retries=2, delay=5, bot=None, postback_type=None, user_id=None):
    """
    Отправка HTTP запроса с повторными попытками и логированием ошибок
    
    v2.2: Оптимизация для предотвращения блокировки воркеров
    - timeout: 30с -> 10с (через shared session)
    - retries: 3 -> 2
    - delay: 60с -> 5с
    - shared session: переиспользование TCP соединений
    - Максимальная блокировка одного вызова: ~25с вместо ~270с
    """
    start_time = datetime.now()
    last_exception = None

    # Формируем полный URL для логирования
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"

    session = await get_http_session()

    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, params=params) as resp:
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

                    # Логируем HTTP ошибку если это последняя попытка
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
            last_exception = Exception("Таймаут запроса (10 сек)")
            if attempt == retries and ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="KEITARO_TIMEOUT",
                    error_message="Превышено время ожидания ответа",
                    user_id=user_id,
                    additional_info={
                        "url": full_url,
                        "postback_type": postback_type,
                        "timeout": "10 сек",
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

        # Ждём перед следующей попыткой (5с * attempt)
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


async def send_keitaro_postback(subid: str, status: str, payout: float = None, tid: int = None, retries=2, delay=5, bot=None, user_id=None):
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
    is_redep: bool = False,
    retries: int = 2,
    delay: int = 5,
    user_id: int = None
):
    """
    Постбэк в Chatterfy для отправки информации о депозитах
    URL: https://api.chatterfy.ai/api/postbacks/3bdc8be1-76d1-4312-9842-c68e7f88f9c8/tracker-postback

    Параметры:
    - clickid: clickid_chatterfry из БД
    - sumdep: общая сумма всех депозитов пользователя
    - previous_dep: сумма текущей транзакции
    - is_redep: True для редепозита (использует event pb_redep), False для депозита (sumdep)
    """
    from config import CHATTERFY_POSTBACK_URL

    # Разные события для dep и redep
    event_type = "pb_redep" if is_redep else "sumdep"

    params = {
        "tracker.event": event_type,
        "clickid": clickid,
        "fields.sumdep": sumdep,
        "fields.previous_dep": previous_dep,
        "tracker.cost": previous_dep
    }

    result = await fetch_with_retry(
        CHATTERFY_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=None,
        postback_type=f"Chatterfy_{event_type.upper()}",
        user_id=user_id
    )
    result["postback_type"] = f"Chatterfy {event_type.upper()}"

    print(f"📤 Постбэк Chatterfy ({event_type}): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")

    return result


async def send_chatterfy_withdraw_postback(
    clickid: str,
    withdraw_amount: float,
    retries: int = 2,
    delay: int = 5,
    user_id: int = None
):
    """
    Постбэк в Chatterfy для отправки информации о выводе средств
    URL: https://api.chatterfy.ai/api/postbacks/3bdc8be1-76d1-4312-9842-c68e7f88f9c8/tracker-postback?tracker.event=withdraw&clickid={clickid}&fields.withdraw={withdraw}

    Параметры:
    - clickid: clickid_chatterfry из БД
    - withdraw_amount: сумма вывода
    """
    from config import CHATTERFY_POSTBACK_URL

    params = {
        "tracker.event": "withdraw",
        "clickid": clickid,
        "fields.withdraw": withdraw_amount
    }

    result = await fetch_with_retry(
        CHATTERFY_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=None,
        postback_type="Chatterfy_WITHDRAW",
        user_id=user_id
    )
    result["postback_type"] = "Chatterfy WITHDRAW"

    print(f"📤 Постбэк Chatterfy (withdraw): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")

    return result


def determine_source_from_company(company: str) -> str:
    """
    Определяет source на основе названия кампании.
    
    Логика:
    1. Если company пустое/None - возвращаем "direct"
    2. Если в company есть "fb", "tmz", "shade" (case insensitive) - возвращаем "facebook"
    3. Если в company есть "google" (case insensitive) - возвращаем "google"
    4. Иначе - возвращаем "facebook" (по умолчанию для упрощения маппинга)
    
    Args:
        company: Название кампании из БД
        
    Returns:
        source: "direct", "facebook" или "google"
    """
    # Если company пустое - это direct трафик
    if not company or company.strip() == "" or company == "None":
        return "direct"
    
    company_lower = company.lower()
    
    # Проверяем на Google
    if "google" in company_lower:
        return "google"
    
    # Проверяем на Facebook маркеры (fb, tmz, shade)
    facebook_markers = ["fb", "tmz", "shade"]
    for marker in facebook_markers:
        if marker in company_lower:
            return "facebook"
    
    # По умолчанию - facebook (для упрощения маппинга)
    return "facebook"


async def send_chatterfy_ftm_postback(
    clickid: str,
    company: str,
    retries: int = 2,
    delay: int = 5,
    user_id: int = None
):
    """
    Постбэк в Chatterfy при событии FTM (First Time Message)
    URL: https://api.chatterfy.ai/api/postbacks/3bdc8be1-76d1-4312-9842-c68e7f88f9c8/tracker-postback
         ?tracker.event=new_postback_event_7&clickid={clickid}&fields.source={source}&fields.company={company}

    Параметры:
    - clickid: clickid_chatterfry из БД
    - company: название кампании из БД (используется для определения source)
    
    Source определяется автоматически:
    - "direct" - если company пустое
    - "facebook" - если в company есть fb, tmz, shade
    - "google" - если в company есть google
    - "facebook" - по умолчанию (если ничего не подошло)
    """
    from config import CHATTERFY_POSTBACK_URL

    # Определяем source на основе company
    source = determine_source_from_company(company)
    
    # Если company пустое - передаем "direct" вместо пустой строки
    company_value = company if (company and company.strip() and company != "None") else "direct"

    params = {
        "tracker.event": "new_postback_event_7",
        "clickid": clickid,
        "fields.source": source,
        "fields.company": company_value
    }

    result = await fetch_with_retry(
        CHATTERFY_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=None,
        postback_type="Chatterfy_FTM_SOURCE",
        user_id=user_id
    )
    result["postback_type"] = "Chatterfy FTM_SOURCE"
    result["source"] = source
    result["company"] = company_value

    print(f"📤 Постбэк Chatterfy FTM (new_postback_event_7): {result['full_url']}")
    print(f"   Source: {source}, Company: {company_value}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")

    return result