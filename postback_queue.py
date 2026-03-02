"""
Postback Queue v1.0

Очередь повторной отправки постбэков.
Если Keitaro/Chatterfy недоступен (таймаут, 5xx), постбэк попадает в очередь
и переотправляется через экспоненциально растущие интервалы.

Расписание retry:
  attempt 1: через 30 сек
  attempt 2: через 1 мин
  attempt 3: через 2 мин
  attempt 4: через 5 мин
  attempt 5: через 10 мин
  attempt 6-10: через 15 мин

После 10 попыток → статус failed, уведомление в Telegram.

Фоновый воркер проверяет очередь каждые 15 секунд.
"""

import asyncio
import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

from service_logger import slog


# Интервалы retry (секунды) по номеру попытки
RETRY_DELAYS = {
    1: 30,
    2: 60,
    3: 120,
    4: 300,
    5: 600,
}
DEFAULT_RETRY_DELAY = 900  # 15 минут для попыток 6+
MAX_ATTEMPTS = 10
QUEUE_CHECK_INTERVAL = 15  # секунд


class PostbackQueue:
    """
    Управляет очередью повторных постбэков.
    Пишет в таблицу postback_queue, фоновый воркер обрабатывает pending записи.
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
        self._worker_task: Optional[asyncio.Task] = None
        self._running = False
        self._initialized = True

    def start_worker(self):
        """Запускает фоновый воркер обработки очереди"""
        if self._worker_task is None or self._worker_task.done():
            self._running = True
            self._worker_task = asyncio.create_task(self._process_loop())
            print("[QUEUE] ✓ Queue worker запущен")

    async def stop_worker(self):
        """Останавливает воркер"""
        self._running = False
        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        print("[QUEUE] ✓ Queue worker остановлен")

    def enqueue(
        self,
        target: str,
        action: str,
        user_id: int,
        payload: Dict[str, Any],
        last_error: str = None,
    ):
        """
        Добавляет постбэк в очередь retry.

        Args:
            target: 'keitaro' или 'chatterfy'
            action: 'ftm', 'reg', 'dep', 'redep', 'revenue', 'withdraw'
            user_id: Telegram User ID
            payload: Полные параметры для повторной отправки
            last_error: Текст последней ошибки
        """
        try:
            from db import DataBase
            db = DataBase()

            next_retry = datetime.now(timezone.utc) + timedelta(seconds=RETRY_DELAYS.get(1, 30))

            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO postback_queue 
                        (target, action, user_id, payload, status, attempts, max_attempts, 
                         last_error, next_retry_at)
                        VALUES (%s, %s, %s, %s, 'pending', 0, %s, %s, %s)
                        RETURNING id
                    """, (
                        target,
                        action,
                        user_id,
                        json.dumps(payload),
                        MAX_ATTEMPTS,
                        last_error,
                        next_retry,
                    ))
                    queue_id = cursor.fetchone()[0]

            print(f"[QUEUE] ✓ Постбэк добавлен в очередь: id={queue_id}, target={target}, action={action}, user={user_id}")

            # Логируем асинхронно (не блокируя)
            asyncio.create_task(slog.log_queue_event(
                "ENQUEUED", target, queue_id=queue_id,
                extra={"action": action, "user_id": user_id, "last_error": last_error}
            ))

            return queue_id

        except Exception as e:
            print(f"[QUEUE] ✗ Ошибка добавления в очередь: {e}")
            return None

    async def _process_loop(self):
        """Основной цикл обработки очереди"""
        while self._running:
            try:
                processed = await self._process_pending()
                if processed > 0:
                    print(f"[QUEUE] Обработано {processed} постбэков из очереди")
            except Exception as e:
                print(f"[QUEUE] ✗ Ошибка в цикле обработки: {e}")
                await slog.error("QUEUE", "WORKER_ERROR", f"Ошибка воркера очереди: {e}",
                                include_traceback=True)

            await asyncio.sleep(QUEUE_CHECK_INTERVAL)

    async def _process_pending(self) -> int:
        """Обрабатывает все pending записи, у которых наступило время retry"""
        try:
            from db import DataBase
            db = DataBase()

            # Берём записи для обработки
            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        UPDATE postback_queue 
                        SET status = 'processing'
                        WHERE id IN (
                            SELECT id FROM postback_queue
                            WHERE status = 'pending' 
                            AND next_retry_at <= NOW()
                            ORDER BY next_retry_at
                            LIMIT 10
                            FOR UPDATE SKIP LOCKED
                        )
                        RETURNING id, target, action, user_id, payload, attempts, max_attempts
                    """)
                    rows = cursor.fetchall()

            if not rows:
                return 0

            processed = 0
            for row in rows:
                queue_id, target, action, user_id, payload_json, attempts, max_attempts = row

                if isinstance(payload_json, str):
                    payload = json.loads(payload_json)
                else:
                    payload = payload_json

                success = await self._retry_postback(target, action, user_id, payload)
                attempts += 1

                if success:
                    await self._mark_completed(db, queue_id)
                    await slog.info("QUEUE", "RETRY_OK",
                                   f"Постбэк из очереди отправлен: id={queue_id}, target={target}, action={action}",
                                   user_id=user_id,
                                   extra={"queue_id": queue_id, "attempt": attempts})
                elif attempts >= max_attempts:
                    await self._mark_failed(db, queue_id, "Max attempts reached")
                    await slog.error("QUEUE", "MAX_ATTEMPTS",
                                    f"Постбэк из очереди ОКОНЧАТЕЛЬНО провалился: id={queue_id}, target={target}",
                                    user_id=user_id,
                                    extra={"queue_id": queue_id, "attempts": attempts, "action": action})
                else:
                    delay = RETRY_DELAYS.get(attempts + 1, DEFAULT_RETRY_DELAY)
                    next_retry = datetime.now(timezone.utc) + timedelta(seconds=delay)
                    await self._mark_pending_retry(db, queue_id, attempts, next_retry)

                processed += 1

                # Пауза между retry чтобы не спамить
                await asyncio.sleep(2)

            return processed

        except Exception as e:
            print(f"[QUEUE] ✗ Ошибка обработки pending: {e}")
            return 0

    async def _retry_postback(self, target: str, action: str, user_id: int, payload: dict) -> bool:
        """
        Повторно отправляет постбэк.
        Возвращает True если успешно.
        """
        try:
            if target == "keitaro":
                from api_request import send_keitaro_postback
                result = await send_keitaro_postback(
                    subid=payload.get("subid"),
                    status=payload.get("status"),
                    payout=payload.get("payout"),
                    tid=payload.get("tid"),
                    retries=1,  # Одна попытка на retry из очереди
                    delay=0,
                    user_id=user_id,
                )
                return result.get("ok", False)

            elif target == "chatterfy":
                from api_request import send_chatterfy_postback, send_chatterfy_ftm_postback, send_chatterfy_withdraw_postback

                event = payload.get("event")

                if event == "new_postback_event_7":
                    result = await send_chatterfy_ftm_postback(
                        clickid=payload.get("clickid"),
                        company=payload.get("company"),
                        retries=1, delay=0, user_id=user_id,
                    )
                elif event == "withdraw":
                    result = await send_chatterfy_withdraw_postback(
                        clickid=payload.get("clickid"),
                        withdraw_amount=payload.get("withdraw_amount"),
                        retries=1, delay=0, user_id=user_id,
                    )
                else:
                    result = await send_chatterfy_postback(
                        clickid=payload.get("clickid"),
                        sumdep=payload.get("sumdep"),
                        previous_dep=payload.get("previous_dep"),
                        is_redep=payload.get("is_redep", False),
                        retries=1, delay=0, user_id=user_id,
                    )
                return result.get("ok", False)

            else:
                print(f"[QUEUE] ⚠️ Неизвестный target: {target}")
                return False

        except Exception as e:
            print(f"[QUEUE] ✗ Ошибка retry: {e}")
            return False

    async def _mark_completed(self, db, queue_id: int):
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE postback_queue 
                    SET status = 'completed', completed_at = NOW(), attempts = attempts + 1
                    WHERE id = %s
                """, (queue_id,))

    async def _mark_failed(self, db, queue_id: int, error: str):
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE postback_queue 
                    SET status = 'failed', last_error = %s, attempts = attempts + 1
                    WHERE id = %s
                """, (error, queue_id))

    async def _mark_pending_retry(self, db, queue_id: int, attempts: int, next_retry: datetime):
        with db.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    UPDATE postback_queue 
                    SET status = 'pending', attempts = %s, next_retry_at = %s
                    WHERE id = %s
                """, (attempts, next_retry, queue_id))

    def get_stats(self) -> Dict[str, Any]:
        """Статистика по очереди"""
        try:
            from db import DataBase
            db = DataBase()

            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    stats = {}

                    cursor.execute("""
                        SELECT status, COUNT(*) FROM postback_queue GROUP BY status
                    """)
                    for row in cursor.fetchall():
                        stats[f"queue_{row[0]}"] = row[1]

                    cursor.execute("""
                        SELECT COUNT(*) FROM postback_queue 
                        WHERE status = 'pending' AND next_retry_at <= NOW()
                    """)
                    stats["queue_ready_for_retry"] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM postback_queue 
                        WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'
                    """)
                    stats["queue_completed_last_hour"] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM postback_queue 
                        WHERE status = 'failed' AND created_at > NOW() - INTERVAL '24 hours'
                    """)
                    stats["queue_failed_last_24h"] = cursor.fetchone()[0]

                    return stats

        except Exception as e:
            return {"error": str(e)}


# Глобальный экземпляр
postback_queue = PostbackQueue()
