import aiohttp
import asyncio
from datetime import datetime, timezone
from config import *
from typing import Optional
from urllib.parse import urlencode
from logger_bot import send_error_log


# ==========================================
# SHARED HTTP SESSION (один на воркер-процесс)
# ==========================================
_http_session: Optional[aiohttp.ClientSession] = None

# ==========================================
# СЕМАФОРЫ: раздельные для Keitaro и Chatterfy
# v2.5: Раньше был один _keitaro_semaphore(2) на оба сервиса —
#        при параллельной отправке (asyncio.gather) они конкурировали
#        за 2 слота, и Chatterfy таймаутил при нагрузке.
# ==========================================
_keitaro_semaphore: asyncio.Semaphore = asyncio.Semaphore(2)
_chatterfy_semaphore: asyncio.Semaphore = asyncio.Semaphore(4)


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
                total=15,       # общий таймаут 15с (было 10 — мало при очереди)
                connect=5,      # таймаут на подключение 5с (ловим stale быстрее)
                sock_read=12,   # таймаут на чтение 12с (было 8)
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


async def _fresh_request(url: str, params: dict = None, timeout_total: int = 15) -> dict:
    """
    Отправляет запрос через НОВУЮ сессию (не shared).
    Используется при retry после таймаута — гарантирует свежее TCP соединение.

    v2.5: Добавлен настраиваемый timeout_total
    """
    connector = aiohttp.TCPConnector(
        limit=5,
        force_close=True,  # закрываем после использования
    )
    timeout = aiohttp.ClientTimeout(total=timeout_total, connect=5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async with session.get(url, params=params) as resp:
            text = await resp.text()
            return {"status": resp.status, "text": text}


async def fetch_with_retry(
    url, params=None, retries=3, delay=3, bot=None,
    postback_type=None, user_id=None, semaphore=None,
    use_shared_session_first=True, timeout_total=15
):
    """
    Отправка HTTP запроса с повторными попытками и логированием ошибок

    v2.5: Фикс конкуренции Keitaro vs Chatterfy
    - Раздельные семафоры (передаётся через параметр semaphore)
    - use_shared_session_first=False → все попытки через fresh session
      (для Chatterfy, у которого другой keepalive/CDN)
    - timeout_total — настраиваемый таймаут для разных сервисов
    """
    start_time = datetime.now(timezone.utc)
    last_exception = None

    # Формируем полный URL для логирования
    full_url = url
    if params:
        full_url = f"{url}?{urlencode(params)}"

    # Если семафор не передан — без ограничения
    if semaphore is None:
        semaphore = asyncio.Semaphore(999)

    # Семафор — ждём свою очередь
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                if attempt == 1 and use_shared_session_first:
                    # Первая попытка — shared session (быстрая, для Keitaro)
                    session = await get_http_session()
                    async with session.get(url, params=params) as resp:
                        text = await resp.text()
                        status = resp.status
                else:
                    # Retry или Chatterfy — свежее соединение
                    if attempt > 1:
                        print(f"[HTTP] 🔄 Retry #{attempt} через fresh connection: {full_url}")
                    result = await _fresh_request(url, params, timeout_total=timeout_total)
                    status = result["status"]
                    text = result["text"]

                if status == 200:
                    return {
                        "ok": True,
                        "status": status,
                        "text": text,
                        "attempt": attempt,
                        "error_type": None,
                        "timestamp": start_time.strftime('%H:%M:%S UTC'),
                        "duration": (datetime.now(timezone.utc) - start_time).total_seconds(),
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
                            "timeout": f"{timeout_total} сек",
                            "attempts": attempt,
                            "used_fresh_session": attempt > 1 or not use_shared_session_first
                        },
                        full_traceback=False
                    )

            except (aiohttp.ClientError, aiohttp.ServerDisconnectedError, 
                    aiohttp.ClientOSError, ConnectionResetError) as e:
                last_exception = Exception(f"Ошибка соединения: {str(e)}")
                
                # При ошибке соединения на первой попытке — пересоздаём shared session
                if attempt == 1 and use_shared_session_first:
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

            # Пауза перед retry — экспоненциальная с jitter
            if attempt < retries:
                import random
                wait = min(delay * attempt, 10) + random.uniform(0.5, 2.0)
                await asyncio.sleep(wait)

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
        "timestamp": start_time.strftime('%H:%M:%S UTC'),
        "duration": (datetime.now(timezone.utc) - start_time).total_seconds(),
        "full_url": full_url
    }


async def send_keitaro_postback(subid: str, status: str, payout: float = None, tid: int = None, retries=3, delay=3, bot=None, user_id=None):
    """
    Постбэк в Keitaro
    URL: https://ytgtech.com/e87f58c/postback?subid=XXX&status=ftm&payout=100&tid=4
    
    v2.6: Проверяет health monitor, при недоступности — в очередь.
          При фейле всех retry — тоже в очередь.
    """
    from config import KEITARO_POSTBACK_URL
    from service_monitor import keitaro_monitor
    from postback_queue import postback_queue

    params = {
        "subid": subid,
        "status": status
    }

    if payout is not None:
        params["payout"] = payout

    if tid is not None:
        params["tid"] = tid

    full_url = f"{KEITARO_POSTBACK_URL}?{urlencode(params)}"

    # v2.6: Если Keitaro недоступен — сразу в очередь
    if not keitaro_monitor.is_healthy:
        print(f"[HTTP] ⚠️ Keitaro unhealthy, постбэк в очередь: user={user_id}, status={status}")
        postback_queue.enqueue(
            target="keitaro",
            action=status,
            user_id=user_id or 0,
            payload={"subid": subid, "status": status, "payout": payout, "tid": tid},
            last_error="Keitaro unhealthy (health monitor)"
        )
        return {
            "ok": False,
            "text": "Queued - Keitaro unhealthy",
            "attempt": 0,
            "error_type": "QUEUED",
            "timestamp": datetime.now(timezone.utc).strftime('%H:%M:%S UTC'),
            "duration": 0,
            "full_url": full_url,
            "postback_type": f"Keitaro {status.upper()}"
        }

    result = await fetch_with_retry(
        KEITARO_POSTBACK_URL,
        params=params,
        retries=retries,
        delay=delay,
        bot=bot,
        postback_type=f"Keitaro_{status.upper()}",
        user_id=user_id,
        semaphore=_keitaro_semaphore,
        use_shared_session_first=True,
        timeout_total=15,
    )
    result["postback_type"] = f"Keitaro {status.upper()}"

    print(f"📤 Постбэк Keitaro ({status}): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")
        # v2.6: Все retry провалились — в очередь
        postback_queue.enqueue(
            target="keitaro",
            action=status,
            user_id=user_id or 0,
            payload={"subid": subid, "status": status, "payout": payout, "tid": tid},
            last_error=result.get("text", "Unknown error after retries")
        )

    return result


async def send_chatterfy_postback(
    clickid: str,
    sumdep: float,
    previous_dep: float,
    is_redep: bool = False,
    retries: int = 3,
    delay: int = 3,
    user_id: int = None
):
    """
    Постбэк в Chatterfy для отправки информации о депозитах

    v2.6: При фейле всех retry — в очередь
    """
    from config import CHATTERFY_POSTBACK_URL
    from postback_queue import postback_queue

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
        user_id=user_id,
        semaphore=_chatterfy_semaphore,
        use_shared_session_first=False,
        timeout_total=20,
    )
    result["postback_type"] = f"Chatterfy {event_type.upper()}"

    print(f"📤 Постбэк Chatterfy ({event_type}): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")
        # v2.6: В очередь при фейле
        postback_queue.enqueue(
            target="chatterfy",
            action=event_type,
            user_id=user_id or 0,
            payload={
                "event": event_type,
                "clickid": clickid,
                "sumdep": sumdep,
                "previous_dep": previous_dep,
                "is_redep": is_redep,
            },
            last_error=result.get("text", "Unknown error after retries")
        )

    return result


async def send_chatterfy_withdraw_postback(
    clickid: str,
    withdraw_amount: float,
    retries: int = 3,
    delay: int = 3,
    user_id: int = None
):
    """
    Постбэк в Chatterfy для отправки информации о выводе средств

    v2.6: При фейле — в очередь
    """
    from config import CHATTERFY_POSTBACK_URL
    from postback_queue import postback_queue

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
        user_id=user_id,
        semaphore=_chatterfy_semaphore,
        use_shared_session_first=False,
        timeout_total=20,
    )
    result["postback_type"] = "Chatterfy WITHDRAW"

    print(f"📤 Постбэк Chatterfy (withdraw): {result['full_url']}")
    if result['ok']:
        print(f"Результат: ✓ OK")
    else:
        print(f"Результат: ✗ FAIL - {result.get('text')}")
        # v2.6: В очередь при фейле
        postback_queue.enqueue(
            target="chatterfy",
            action="withdraw",
            user_id=user_id or 0,
            payload={
                "event": "withdraw",
                "clickid": clickid,
                "withdraw_amount": withdraw_amount,
            },
            last_error=result.get("text", "Unknown error after retries")
        )

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
    retries: int = 3,
    delay: int = 3,
    user_id: int = None
):
    """
    Постбэк в Chatterfy при событии FTM (First Time Message)

    v2.6: При фейле — в очередь
    """
    from config import CHATTERFY_POSTBACK_URL
    from postback_queue import postback_queue

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
        user_id=user_id,
        semaphore=_chatterfy_semaphore,
        use_shared_session_first=False,
        timeout_total=20,
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
        # v2.6: В очередь при фейле
        postback_queue.enqueue(
            target="chatterfy",
            action="ftm",
            user_id=user_id or 0,
            payload={
                "event": "new_postback_event_7",
                "clickid": clickid,
                "company": company_value,
            },
            last_error=result.get("text", "Unknown error after retries")
        )

    return result