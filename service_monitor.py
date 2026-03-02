"""
Service Monitor v1.0

1. Периодические health-check пинги к Keitaro и Chatterfy
2. Rate limiter для Keitaro (token bucket)
3. Автоматическая пауза отправки при обнаружении даунтайма
4. Запись результатов в health_checks таблицу

Keitaro проверяется каждые 60 секунд.
Если 3 подряд health-check фейлятся → отправка ставится на паузу,
постбэки копятся в очереди. Когда health-check OK → отправка возобновляется.
"""

import asyncio
import aiohttp
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

from service_logger import slog


class KeitaroHealthMonitor:
    """
    Мониторит доступность Keitaro.
    Определяет: можно ли сейчас отправлять постбэки или лучше в очередь.
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self._healthy = True                # Текущий статус
        self._consecutive_failures = 0      # Подряд упавших проверок
        self._last_check_time = 0           # Timestamp последней проверки
        self._last_check_result = None      # Результат последней проверки
        self._pause_threshold = 3           # После N фейлов → пауза
        self._check_interval = 60           # Проверка каждые 60 сек
        self._worker_task = None
        self._initialized = True

    @property
    def is_healthy(self) -> bool:
        """Можно ли отправлять постбэки в Keitaro прямо сейчас?"""
        return self._healthy

    @property
    def status(self) -> Dict[str, Any]:
        """Текущий статус для API ответов"""
        return {
            "healthy": self._healthy,
            "consecutive_failures": self._consecutive_failures,
            "last_check": self._last_check_result,
            "last_check_time": self._last_check_time,
        }

    def start_worker(self):
        """Запускает фоновый мониторинг"""
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = asyncio.create_task(self._monitor_loop())
            print("[HEALTH] ✓ Keitaro health monitor запущен")

    async def stop_worker(self):
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        print("[HEALTH] ✓ Keitaro health monitor остановлен")

    async def _monitor_loop(self):
        """Основной цикл мониторинга"""
        # Ждём 10 сек после старта чтобы всё инициализировалось
        await asyncio.sleep(10)

        while True:
            try:
                result = await self._check_keitaro()
                self._last_check_time = time.time()
                self._last_check_result = result

                # Записываем в БД
                await self._save_health_check("keitaro", result)

                if result.get("status") == "ok":
                    if not self._healthy:
                        # Восстановление!
                        await slog.info("HEALTH", "KEITARO_RECOVERED",
                                       f"Keitaro снова доступен после {self._consecutive_failures} неудач",
                                       extra={"response_ms": result.get("response_ms")},
                                       send_telegram=True)
                    self._consecutive_failures = 0
                    self._healthy = True
                else:
                    self._consecutive_failures += 1
                    if self._consecutive_failures >= self._pause_threshold:
                        if self._healthy:
                            # Переходим в режим паузы
                            self._healthy = False
                            await slog.critical("HEALTH", "KEITARO_DOWN",
                                              f"Keitaro недоступен {self._consecutive_failures} проверок подряд! "
                                              f"Постбэки будут в очереди.",
                                              extra={
                                                  "consecutive_failures": self._consecutive_failures,
                                                  "last_error": result.get("error"),
                                              })
                    else:
                        await slog.warning("HEALTH", "KEITARO_CHECK_FAIL",
                                          f"Keitaro health-check #{self._consecutive_failures} не прошёл: {result.get('error')}",
                                          extra={"response_ms": result.get("response_ms")})

            except Exception as e:
                print(f"[HEALTH] ✗ Ошибка мониторинга: {e}")

            await asyncio.sleep(self._check_interval)

    async def _check_keitaro(self) -> Dict[str, Any]:
        """
        Пингует Keitaro (простой GET на postback URL без параметров).
        Keitaro обычно отвечает 200 или 400 — оба значат что сервер жив.
        """
        from config import KEITARO_POSTBACK_URL

        start = time.time()
        try:
            connector = aiohttp.TCPConnector(force_close=True)
            timeout = aiohttp.ClientTimeout(total=10, connect=5)

            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                # Пингуем без параметров — Keitaro ответит ошибкой, но покажет что жив
                async with session.get(KEITARO_POSTBACK_URL) as resp:
                    elapsed_ms = int((time.time() - start) * 1000)
                    text = await resp.text()

                    # Любой ответ (даже 400/404) = сервер жив
                    if resp.status < 500:
                        return {
                            "status": "ok",
                            "http_status": resp.status,
                            "response_ms": elapsed_ms,
                        }
                    else:
                        return {
                            "status": "error",
                            "http_status": resp.status,
                            "response_ms": elapsed_ms,
                            "error": f"HTTP {resp.status}: {text[:200]}",
                        }

        except asyncio.TimeoutError:
            elapsed_ms = int((time.time() - start) * 1000)
            return {
                "status": "timeout",
                "response_ms": elapsed_ms,
                "error": "Таймаут 10 сек",
            }
        except Exception as e:
            elapsed_ms = int((time.time() - start) * 1000)
            return {
                "status": "error",
                "response_ms": elapsed_ms,
                "error": f"{type(e).__name__}: {e}",
            }

    async def _save_health_check(self, target: str, result: dict):
        """Записывает результат health-check в БД"""
        try:
            from db import DataBase
            db = DataBase()

            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO health_checks (target, status, response_ms, http_status, error_message)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        target,
                        result.get("status"),
                        result.get("response_ms"),
                        result.get("http_status"),
                        result.get("error"),
                    ))
        except Exception as e:
            print(f"[HEALTH] ✗ Ошибка записи health check: {e}")


class RateLimiter:
    """
    Token Bucket rate limiter для Keitaro.
    Ограничивает количество запросов в секунду.

    Текущий лимит: 5 запросов в секунду к Keitaro.
    (Keitaro за Cloudflare, который может резать при burst > ~10 rps)
    """

    def __init__(self, rate: float = 5.0, burst: int = 10):
        """
        Args:
            rate: Сколько токенов в секунду (запросов/сек)
            burst: Максимальный burst (размер ведра)
        """
        self._rate = rate
        self._burst = burst
        self._tokens = burst
        self._last_refill = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, timeout: float = 10.0) -> bool:
        """
        Получить разрешение на запрос.
        Блокирует до появления токена или до timeout.

        Returns:
            True если получен, False если таймаут
        """
        deadline = time.time() + timeout

        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return True

            # Нет токенов — ждём
            if time.time() >= deadline:
                return False

            # Ждём появления следующего токена
            wait = 1.0 / self._rate
            await asyncio.sleep(min(wait, deadline - time.time()))

    def _refill(self):
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        self._refill()
        return self._tokens


# Глобальные экземпляры
keitaro_monitor = KeitaroHealthMonitor()
keitaro_rate_limiter = RateLimiter(rate=5.0, burst=10)
