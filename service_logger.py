"""
Service Logger v1.0

Централизованное логирование:
- Все ошибки и предупреждения пишутся в таблицу service_logs
- CRITICAL и ERROR дублируются в Telegram
- Всё выводится в stdout для PM2 логов

Использование:
    from service_logger import slog

    # Простой лог
    await slog.error("KEITARO", "TIMEOUT", "Таймаут при отправке постбэка", user_id=123)

    # С доп. данными
    await slog.warning("POSTBACK", "DUPLICATE", "Дубликат транзакции",
                       user_id=123, endpoint="/postback/dep",
                       extra={"sum": 100, "action": "dep"})

    # Info (только БД + stdout, не Telegram)
    await slog.info("SYSTEM", "STARTUP", "Сервис запущен")
"""

import asyncio
import json
import traceback as tb_module
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from contextlib import contextmanager


class ServiceLogger:
    """
    Централизованный логгер сервиса.
    Пишет в PostgreSQL service_logs + опционально в Telegram.
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
        self._telegram_enabled = True
        self._db_enabled = True
        # Очередь для асинхронной записи — чтобы логирование не блокировало основной поток
        self._queue: asyncio.Queue = None
        self._worker_task: asyncio.Task = None
        self._initialized = True

    def start_worker(self):
        """Запускает фоновый воркер для записи логов в БД"""
        if self._worker_task is None or self._worker_task.done():
            self._queue = asyncio.Queue(maxsize=5000)
            self._worker_task = asyncio.create_task(self._log_worker())
            print("[SLOG] ✓ Log worker запущен")

    async def stop_worker(self):
        """Останавливает воркер и дожидается записи оставшихся логов"""
        if self._worker_task and not self._worker_task.done():
            # Даём 5 секунд на запись оставшихся логов
            try:
                await asyncio.wait_for(self._drain_queue(), timeout=5.0)
            except asyncio.TimeoutError:
                print(f"[SLOG] ⚠️ Таймаут drain queue, осталось {self._queue.qsize()} логов")
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            print("[SLOG] ✓ Log worker остановлен")

    async def _drain_queue(self):
        """Записывает все логи из очереди"""
        while not self._queue.empty():
            await asyncio.sleep(0.1)

    async def _log_worker(self):
        """Фоновый воркер — берёт логи из очереди и пишет в БД пачками"""
        while True:
            try:
                batch = []
                # Ждём первый элемент
                item = await self._queue.get()
                batch.append(item)

                # Забираем всё что накопилось (до 50 за раз)
                for _ in range(49):
                    try:
                        item = self._queue.get_nowait()
                        batch.append(item)
                    except asyncio.QueueEmpty:
                        break

                # Пишем пачку в БД
                await self._write_batch_to_db(batch)

                for _ in batch:
                    self._queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[SLOG] ✗ Ошибка в log worker: {e}")
                await asyncio.sleep(1)

    async def _write_batch_to_db(self, batch: list):
        """Пишет пачку логов в БД"""
        try:
            from db import DataBase
            db = DataBase()

            with db.get_connection() as conn:
                with conn.cursor() as cursor:
                    for log_entry in batch:
                        try:
                            cursor.execute("""
                                INSERT INTO service_logs 
                                (level, category, event_type, message, user_id, endpoint, 
                                 request_url, response_status, response_body, duration_ms,
                                 attempt, extra, traceback)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                log_entry.get("level"),
                                log_entry.get("category"),
                                log_entry.get("event_type"),
                                log_entry.get("message"),
                                log_entry.get("user_id"),
                                log_entry.get("endpoint"),
                                log_entry.get("request_url"),
                                log_entry.get("response_status"),
                                log_entry.get("response_body"),
                                log_entry.get("duration_ms"),
                                log_entry.get("attempt"),
                                json.dumps(log_entry.get("extra")) if log_entry.get("extra") else None,
                                log_entry.get("traceback"),
                            ))
                        except Exception as e:
                            print(f"[SLOG] ✗ Ошибка записи лога в БД: {e}")

        except Exception as e:
            print(f"[SLOG] ✗ Ошибка подключения к БД для логов: {e}")

    async def _send_to_telegram(self, level: str, category: str, event_type: str,
                                 message: str, user_id: int = None,
                                 extra: dict = None):
        """Отправляет критичные логи в Telegram"""
        try:
            from logger_bot import send_error_log, send_warning_log
            from config import ENABLE_TELEGRAM_LOGS

            if not ENABLE_TELEGRAM_LOGS:
                return

            if level in ("ERROR", "CRITICAL"):
                await send_error_log(
                    error_type=f"{category}_{event_type}",
                    error_message=message,
                    user_id=user_id,
                    additional_info=extra,
                    full_traceback=False
                )
            elif level == "WARNING":
                await send_warning_log(
                    warning_type=f"{category}_{event_type}",
                    message=message,
                    user_id=user_id,
                    additional_info=extra
                )
        except Exception as e:
            print(f"[SLOG] ✗ Ошибка отправки в Telegram: {e}")

    async def log(
        self,
        level: str,
        category: str,
        event_type: str,
        message: str,
        user_id: int = None,
        endpoint: str = None,
        request_url: str = None,
        response_status: int = None,
        response_body: str = None,
        duration_ms: int = None,
        attempt: int = None,
        extra: Dict[str, Any] = None,
        include_traceback: bool = False,
        send_telegram: bool = None,  # None = auto (ERROR/CRITICAL → yes)
    ):
        """
        Основной метод логирования.

        Args:
            level: DEBUG, INFO, WARNING, ERROR, CRITICAL
            category: KEITARO, CHATTERFY, POSTBACK, DB, SYSTEM, HEALTH, QUEUE
            event_type: TIMEOUT, HTTP_ERROR, CONNECTION_ERROR, DUPLICATE, STARTUP, etc.
            message: Человекочитаемое сообщение
            user_id: Telegram User ID (если относится к юзеру)
            endpoint: Эндпоинт (/postback/dep, etc.)
            request_url: Полный URL запроса
            response_status: HTTP статус ответа
            response_body: Тело ответа (будет обрезано до 500 символов)
            duration_ms: Длительность в миллисекундах
            attempt: Номер попытки
            extra: Произвольные доп. данные
            include_traceback: Добавить текущий traceback
            send_telegram: Принудительно отправить/не отправить в Telegram
        """
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S UTC")

        # Формируем traceback если нужен
        traceback_str = None
        if include_traceback:
            tb = tb_module.format_exc()
            if tb and tb != "NoneType: None\n":
                traceback_str = tb[-3000:]  # Ограничиваем

        # Обрезаем response_body
        if response_body and len(response_body) > 500:
            response_body = response_body[:500] + "..."

        # Stdout (всегда)
        emoji = {"DEBUG": "🔍", "INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "🔴", "CRITICAL": "🚨"}.get(level, "📝")
        user_str = f" user={user_id}" if user_id else ""
        duration_str = f" {duration_ms}ms" if duration_ms else ""
        attempt_str = f" attempt={attempt}" if attempt else ""
        print(f"[{timestamp}] {emoji} [{level}] [{category}] {event_type}: {message}{user_str}{duration_str}{attempt_str}")

        # Формируем запись
        log_entry = {
            "level": level,
            "category": category,
            "event_type": event_type,
            "message": message,
            "user_id": user_id,
            "endpoint": endpoint,
            "request_url": request_url,
            "response_status": response_status,
            "response_body": response_body,
            "duration_ms": duration_ms,
            "attempt": attempt,
            "extra": extra,
            "traceback": traceback_str,
        }

        # Пишем в БД через очередь (non-blocking)
        if self._db_enabled and self._queue is not None:
            try:
                self._queue.put_nowait(log_entry)
            except asyncio.QueueFull:
                print(f"[SLOG] ⚠️ Очередь логов переполнена, лог пропущен")

        # Telegram (ERROR/CRITICAL по умолчанию)
        should_telegram = send_telegram if send_telegram is not None else (level in ("ERROR", "CRITICAL"))
        if self._telegram_enabled and should_telegram:
            # Не блокируем основной поток
            asyncio.create_task(self._send_to_telegram(
                level, category, event_type, message, user_id,
                extra=extra
            ))

    # ==========================================
    # Удобные обёртки
    # ==========================================

    async def debug(self, category: str, event_type: str, message: str, **kwargs):
        await self.log("DEBUG", category, event_type, message, **kwargs)

    async def info(self, category: str, event_type: str, message: str, **kwargs):
        await self.log("INFO", category, event_type, message, **kwargs)

    async def warning(self, category: str, event_type: str, message: str, **kwargs):
        await self.log("WARNING", category, event_type, message, **kwargs)

    async def error(self, category: str, event_type: str, message: str, **kwargs):
        await self.log("ERROR", category, event_type, message, **kwargs)

    async def critical(self, category: str, event_type: str, message: str, **kwargs):
        await self.log("CRITICAL", category, event_type, message, **kwargs)

    # ==========================================
    # Специализированные методы для частых кейсов
    # ==========================================

    async def log_http_request(
        self,
        target: str,
        url: str,
        status: int = None,
        duration_ms: int = None,
        attempt: int = None,
        success: bool = True,
        error_msg: str = None,
        user_id: int = None,
        postback_type: str = None,
        response_body: str = None,
    ):
        """
        Логирует HTTP запрос к внешнему сервису (Keitaro, Chatterfy).
        Вызывается из api_request.py.
        """
        if success:
            await self.info(
                target.upper(), "HTTP_OK",
                f"{target} OK: {postback_type or 'request'} → HTTP {status}",
                user_id=user_id,
                request_url=url,
                response_status=status,
                response_body=response_body,
                duration_ms=duration_ms,
                attempt=attempt,
                extra={"postback_type": postback_type},
            )
        else:
            level = "ERROR" if attempt and attempt >= 3 else "WARNING"
            event = "TIMEOUT" if "Таймаут" in (error_msg or "") or "timeout" in (error_msg or "").lower() else "HTTP_ERROR"
            await self.log(
                level, target.upper(), event,
                f"{target} FAIL: {error_msg}",
                user_id=user_id,
                request_url=url,
                response_status=status,
                response_body=response_body,
                duration_ms=duration_ms,
                attempt=attempt,
                extra={"postback_type": postback_type},
            )

    async def log_postback_event(
        self,
        action: str,
        user_id: int,
        success: bool,
        endpoint: str,
        extra: dict = None,
        error_msg: str = None,
    ):
        """
        Логирует обработку постбэка (приём запроса).
        """
        if success:
            await self.info(
                "POSTBACK", f"{action.upper()}_OK",
                f"Постбэк {action} обработан успешно",
                user_id=user_id,
                endpoint=endpoint,
                extra=extra,
            )
        else:
            await self.error(
                "POSTBACK", f"{action.upper()}_FAIL",
                f"Ошибка обработки постбэка {action}: {error_msg}",
                user_id=user_id,
                endpoint=endpoint,
                extra=extra,
                include_traceback=True,
            )

    async def log_queue_event(
        self,
        action: str,
        target: str,
        queue_id: int = None,
        success: bool = True,
        error_msg: str = None,
        extra: dict = None,
    ):
        """Логирует события очереди постбэков"""
        if success:
            await self.info(
                "QUEUE", f"{action.upper()}",
                f"Queue {action}: target={target}, id={queue_id}",
                extra=extra,
            )
        else:
            await self.warning(
                "QUEUE", f"{action.upper()}_FAIL",
                f"Queue {action} failed: {error_msg}",
                extra={**(extra or {}), "queue_id": queue_id, "target": target},
            )


# Глобальный экземпляр
slog = ServiceLogger()
