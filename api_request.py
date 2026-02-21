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


def _make_connector() -> aiohttp.TCPConnector:
    """Создаёт TCP коннектор с оптимальными настройками"""
    return aiohttp.TCPConnector(
        limit=30,                    # макс одновременных соединений (было 20)
        keepalive_timeout=10,        # держим соединения 10с (было 30 — Cloudflare режет раньше)
        enable_cleanup_closed=True,
        force_close=False,           # переиспользуем живые соединения
        ttl_dns_cache=300,           # кешируем DNS 5 минут
    )


async def get_http_session() -> aiohttp.ClientSession:
    """
    Получает или создает shared HTTP сессию для текущего воркера.
    Переиспользует TCP соединения вместо создания новых на каждый запрос.
    """
    global _http_session
    if _http_session is None or _http_session.closed:
        _http_session = aiohttp.ClientSession(
            connector=_make_connector(),
            timeout=aiohttp.ClientTimeout(
                total=10,       # общий таймаут 10с
                connect=5,      # таймаут на подключение 5с (ловим stale быстрее)
                sock_read=8,    # таймаут на чтение 8с
            )
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


async def _fresh_request(url: str, params: dict = None) -> dict:
    """
    Отправляет запрос через НОВУЮ сессию (не shared).
    Используется при retry после таймаута — гарантирует свежее TCP соединение.
    """
    connector = aiohttp.TCPConnector(
        limit=5,
        force_close=True,  # закрываем после использования
    )
    timeout = aiohttp.ClientTimeout(total=10, connect=5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(url, params=params) as resp:
            text = await resp.text()
            return {"status": resp.status, "text": text}


async def fetch_with_retry(url, params=None, retries=2, delay=5, bot=None, postback_type=None, user_id=None):
    """
    Отправка HTTP запроса с повторными попытками и логированием ошибок

    v2.3: Фикс stale connections
    - Attempt 1: через shared session (быстро, переиспользует соединения)
    - Attempt 2: через FRESH session (новое TCP соединение, обходит stale)
    - connect timeout: 5с (быстро детектим мёртвые сокеты)
    - keepalive: 30с → 10с (Cloudflare режет idle раньше)
    """
    start_time = datetime.now()
    last_exception = None

    # Формируем полный URL для логирования
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"

    for attempt in range(1, retries + 1):
        try:
            if attempt == 1:
                # Первая попытка — shared session (быстрая)
                session = await get_http_session()
                async with session.get(url, params=params) as resp:
                    text = await resp.text()
                    status = resp.status
            else:
                # Retry — свежее соединение (обходит stale keepalive)
                print(f"[HTTP] 🔄 Retry #{attempt} через fresh connection: {full_url}")
                result = await _fresh_request(url, params)
                status = result["status"]
                text = result["text"]

            if status == 200:
                return {
                    "ok": True,
                    "status": status,
                    "text": text,
                    "attempt": attempt,
                    "error_type": None,
                    "timestamp": start_time.strftime('%H:%M:%S'),
                    "duration": (datetime.now() - start_time).total_seconds(),
                    "full_url": full_url
                }
            else:
                last_exception = Exception(
                    f"HTTP {status}: {text[:200]}...")

                if attempt == retries and ENABLE_TELEGRAM_LOGS:
                    await send_error_log(
                        error_type="KEITARO_HTTP_ERROR",
                        error_message=f"HTTP {status} при отправке постбэка",
                        user_id=user_id,
                        additional_info={
                            "url": full_url,
                            "postback_type": postback_type,
                            "status_code": status,
                            "response": text[:200],
                            "attempts": attempt
                        },
                        full_traceback=False
                    )

        except asyncio.TimeoutError:
            last_exception = Exception(f"Таймаут запроса (attempt {attempt})")
            if attempt == retries and ENABLE_TELEGRAM_LOGS:
                await send_error_log(
                    error_type="KEITARO_TIMEOUT",
                    error_message="Превышено время ожидания ответа",
                    user_id=user_id,
                    additional_info={
                        "url": full_url,
                        "postback_type": postback_type,
                        "timeout": "10 сек",
                        "attempts": attempt,
                        "used_fresh_session": attempt > 1
                    },
                    full_traceback=False
                )

        except (aiohttp.ClientError, aiohttp.ServerDisconnectedError, 
                aiohttp.ClientOSError, ConnectionResetError) as e:
            last_exception = Exception(f"Ошибка соединения: {str(e)}")
            
            # При ошибке соединения на первой попытке — пересоздаём shared session
            if attempt == 1:
                print(f"[HTTP] ⚠️ Connection error, recreating shared session: {e}")
                await close_http_session()
            
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

        # Короткая пауза перед retry (не блокируем event loop надолго)
        if attempt < retries:
            await asyncio.sleep(min(delay * attempt, 10))

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
    """
    from config import KEITARO_POSTBACK_URL

    params = {
        "subid": subid,
        "status": status
    }

    if payout is not None:
        params["payout"] = payout

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
    """
    from config import CHATTERFY_POSTBACK_URL

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
    """
    if not company or company.strip() == "" or company == "None":
        return "direct"
    
    company_lower = company.lower()
    
    if "google" in company_lower:
        return "google"
    
    facebook_markers = ["fb", "tmz", "shade"]
    for marker in facebook_markers:
        if marker in company_lower:
            return "facebook"
    
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
    """
    from config import CHATTERFY_POSTBACK_URL

    source = determine_source_from_company(company)
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