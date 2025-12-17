import psycopg2
import psycopg2.extras
from psycopg2 import pool
from config import DB_CONFIG
from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from contextlib import contextmanager


class DataBase:
    """
    Singleton-подобный класс для работы с PostgreSQL через connection pool.
    Используется один экземпляр на всё приложение.
    """
    _instance = None
    _pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """
        Инициализация connection pool для надежной работы с PostgreSQL.
        Создается только один раз благодаря singleton паттерну.
        """
        if self._initialized:
            return

        try:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,
                **DB_CONFIG
            )
            print("[DB] ✓ Connection pool создан успешно")
            self._initialized = True
        except Exception as e:
            print(f"[DB] ✗ Ошибка создания connection pool: {e}")
            raise

    @property
    def connection_pool(self):
        return self._pool

    @contextmanager
    def get_connection(self):
        """
        Context manager для безопасного получения и возврата соединения из пула
        """
        conn = None
        try:
            conn = self._pool.getconn()
            conn.autocommit = True
            yield conn
        except Exception as e:
            print(f"[DB] ✗ Ошибка при работе с соединением: {e}")
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def close_all_connections(self):
        """
        Закрыть все соединения в пуле (для graceful shutdown)
        """
        if self._pool:
            self._pool.closeall()
            print("[DB] ✓ Все соединения закрыты")
            DataBase._instance = None
            DataBase._pool = None

    # ==========================================
    # МЕТОДЫ ДЛЯ СОЗДАНИЯ ПОЛЬЗОВАТЕЛЕЙ
    # ==========================================

    def create_user_if_not_exists(
        self,
        user_id: int,
        subscriber_id: str = None,
        trader_id: str = None,
        clickid_chatterfry: str = None
    ) -> Dict[str, Any]:
        """
        Создает пользователя в БД если его еще нет.

        Args:
            user_id: Telegram ID пользователя
            subscriber_id: UUID идентификатор (опционально)
            trader_id: ID трейдера из платформы (опционально)
            clickid_chatterfry: Click ID из трекера (опционально)

        Returns:
            Dict с результатом: created (bool), user_id, existed (bool)
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Проверяем существование пользователя
                    cursor.execute(
                        "SELECT id FROM users WHERE id = %s", (user_id,))
                    existing = cursor.fetchone()

                    if existing:
                        print(f"[DB] Пользователь {user_id} уже существует")
                        return {
                            "success": True,
                            "created": False,
                            "existed": True,
                            "user_id": user_id
                        }

                    # Создаем нового пользователя с минимальными данными
                    cursor.execute("""
                        INSERT INTO users (id, subscriber_id, trader_id, clickid_chatterfry, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING
                        RETURNING id
                    """, (
                        user_id,
                        subscriber_id,
                        trader_id,
                        clickid_chatterfry,
                        datetime.now()
                    ))

                    result = cursor.fetchone()

                    if result:
                        print(f"[DB] ✓ Создан новый пользователь {user_id}")
                        return {
                            "success": True,
                            "created": True,
                            "existed": False,
                            "user_id": user_id
                        }
                    else:
                        # Был race condition, пользователь уже создан
                        return {
                            "success": True,
                            "created": False,
                            "existed": True,
                            "user_id": user_id
                        }

        except Exception as e:
            print(f"[DB] ✗ Ошибка создания пользователя: {e}")
            return {"success": False, "error": str(e)}

    def ensure_user_exists(
        self,
        user_id: int = None,
        subscriber_id: str = None,
        trader_id: str = None,
        clickid_chatterfry: str = None
    ) -> Dict[str, Any]:
        """
        Гарантирует что пользователь существует в БД.
        Ищет по user_id или subscriber_id, создает если не найден.

        Returns:
            Dict с user_id и статусом
        """
        try:
            # Сначала пытаемся найти существующего пользователя
            found_user_id = None

            if user_id:
                with self.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(
                            "SELECT id FROM users WHERE id = %s", (user_id,))
                        result = cursor.fetchone()
                        if result:
                            found_user_id = result[0]

            # Если не нашли по user_id, ищем по subscriber_id
            if not found_user_id and subscriber_id:
                found_user_id = self.get_user_by_subscriber_id(subscriber_id)

            # Если пользователь найден - возвращаем его
            if found_user_id:
                return {
                    "success": True,
                    "user_id": found_user_id,
                    "created": False,
                    "existed": True
                }

            # Если user_id не передан и пользователь не найден - ошибка
            if not user_id:
                return {
                    "success": False,
                    "error": "Cannot create user without user_id"
                }

            # Создаем нового пользователя
            return self.create_user_if_not_exists(
                user_id=user_id,
                subscriber_id=subscriber_id,
                trader_id=trader_id,
                clickid_chatterfry=clickid_chatterfry
            )

        except Exception as e:
            print(f"[DB] ✗ Ошибка в ensure_user_exists: {e}")
            return {"success": False, "error": str(e)}

    # ==========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С CLICKID
    # ==========================================

    def update_user_clickid(self, user_id: int, clickid_chatterfry: str) -> Dict[str, Any]:
        """
        Обновляет clickid_chatterfry пользователя.
        Обновляет только если поле пустое (не перезаписывает существующее).

        Args:
            user_id: ID пользователя
            clickid_chatterfry: Click ID из трекера Chatterfry

        Returns:
            Dict с результатом операции
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Обновляем только если clickid_chatterfry пустой
                    cursor.execute("""
                        UPDATE users 
                        SET clickid_chatterfry = %s 
                        WHERE id = %s 
                        AND (clickid_chatterfry IS NULL OR clickid_chatterfry = '')
                    """, (clickid_chatterfry, user_id))

                    if cursor.rowcount > 0:
                        print(
                            f"[DB] ✓ Обновлен clickid_chatterfry для user {user_id}: {clickid_chatterfry}")
                        return {"success": True, "updated": True}
                    else:
                        # Проверяем, существует ли пользователь
                        cursor.execute(
                            "SELECT clickid_chatterfry FROM users WHERE id = %s",
                            (user_id,)
                        )
                        result = cursor.fetchone()

                        if result:
                            existing_clickid = result[0]
                            if existing_clickid:
                                print(
                                    f"[DB] clickid_chatterfry уже установлен для user {user_id}: {existing_clickid}")
                                return {"success": True, "updated": False, "reason": "already_set", "existing": existing_clickid}
                            else:
                                return {"success": True, "updated": False, "reason": "no_change"}
                        else:
                            print(f"[DB] Пользователь {user_id} не найден")
                            return {"success": False, "error": "User not found"}

        except Exception as e:
            print(f"[DB] ✗ Ошибка обновления clickid_chatterfry: {e}")
            return {"success": False, "error": str(e)}

    def get_user_clickid(self, user_id: int) -> Optional[str]:
        """
        Получает clickid_chatterfry пользователя из БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT clickid_chatterfry FROM users WHERE id = %s",
                        (user_id,)
                    )
                    result = cursor.fetchone()

                    if result and result[0]:
                        return result[0]
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения clickid_chatterfry: {e}")
            return None

    # ==========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С ТРАНЗАКЦИЯМИ
    # ==========================================

    def create_transaction(
        self,
        user_id: int,
        action: str,
        sum_amount: float = None,
        raw_data: dict = None
    ) -> Dict[str, Any]:
        """
        Создает запись о транзакции в таблице transactions
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO transactions (user_id, action, sum, raw_data)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id, created_at
                    """, (
                        user_id,
                        action,
                        sum_amount,
                        json.dumps(raw_data) if raw_data else None
                    ))

                    result = cursor.fetchone()
                    transaction_id = result[0]
                    created_at = result[1]

                    print(
                        f"[DB] ✓ Создана транзакция #{transaction_id}: user={user_id}, action={action}, sum={sum_amount}")

                    return {
                        "success": True,
                        "transaction_id": transaction_id,
                        "created_at": created_at
                    }

        except Exception as e:
            print(f"[DB] ✗ Ошибка создания транзакции: {e}")
            return {"success": False, "error": str(e)}

    def update_user_event(
        self,
        user_id: int,
        action: str,
        sum_amount: float = None
    ) -> Dict[str, Any]:
        """
        Обновляет поля событий в таблице users (ftm, reg, dep, redep)
        """
        try:
            with self.get_connection() as conn:
                update_fields = []
                params = []

                if action == "ftm":
                    update_fields = ["ftm_time = %s"]
                    params = [datetime.now()]

                elif action == "reg":
                    update_fields = ["reg = TRUE", "reg_time = %s"]
                    params = [datetime.now()]

                elif action == "dep":
                    update_fields = ["dep = TRUE",
                                     "dep_time = %s", "dep_sum = %s"]
                    params = [datetime.now(), sum_amount]

                elif action == "redep":
                    update_fields = ["redep = TRUE",
                                     "redep_time = %s", "redep_sum = %s"]
                    params = [datetime.now(), sum_amount]

                else:
                    return {"success": True, "message": "Custom action, only transaction created"}

                params.append(user_id)

                query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"

                with conn.cursor() as cursor:
                    cursor.execute(query, params)

                    if cursor.rowcount > 0:
                        print(f"[DB] ✓ Обновлен user {user_id}: {action}")
                        return {"success": True, "updated_rows": cursor.rowcount}
                    else:
                        print(f"[DB] ✗ Пользователь {user_id} не найден")
                        return {"success": False, "error": "User not found"}

        except Exception as e:
            print(f"[DB] ✗ Ошибка обновления события: {e}")
            return {"success": False, "error": str(e)}

    def process_postback(
        self,
        user_id: int,
        action: str,
        sum_amount: float = None,
        raw_data: dict = None
    ) -> Dict[str, Any]:
        """
        Полная обработка постбэка: создает транзакцию + обновляет users
        """
        try:
            # 1. Создаем запись в транзакциях
            transaction_result = self.create_transaction(
                user_id=user_id,
                action=action,
                sum_amount=sum_amount,
                raw_data=raw_data
            )

            if not transaction_result.get("success"):
                return transaction_result

            # 2. Обновляем поля в users для основных событий
            user_result = self.update_user_event(
                user_id=user_id,
                action=action,
                sum_amount=sum_amount
            )

            return {
                "success": True,
                "transaction_id": transaction_result.get("transaction_id"),
                "user_updated": user_result.get("success")
            }

        except Exception as e:
            print(f"[DB] ✗ Ошибка обработки постбэка: {e}")
            return {"success": False, "error": str(e)}

    def get_user_deposits_count(self, user_id: int) -> int:
        """
        Подсчитывает количество депозитов (dep + redep) пользователя
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM transactions 
                        WHERE user_id = %s 
                        AND action IN ('dep', 'redep')
                    """, (user_id,))

                    count = cursor.fetchone()[0]
                    print(
                        f"[DB] Найдено {count} депозитов для пользователя {user_id}")
                    return count

        except Exception as e:
            print(f"[DB] ✗ Ошибка подсчета депозитов: {e}")
            return 0

    def get_user_transactions(self, user_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Получает историю транзакций пользователя
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT id, action, sum, created_at, raw_data
                        FROM transactions
                        WHERE user_id = %s
                        ORDER BY created_at DESC
                        LIMIT %s
                    """, (user_id, limit))

                    transactions = cursor.fetchall()
                    return [dict(t) for t in transactions]

        except Exception as e:
            print(f"[DB] ✗ Ошибка получения транзакций: {e}")
            return []

    def get_transactions_stats(self) -> Dict[str, Any]:
        """
        Получает статистику по транзакциям
        """
        try:
            with self.get_connection() as conn:
                stats = {}

                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM transactions")
                    stats['total_transactions'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT action, COUNT(*) as count, SUM(sum) as total_sum
                        FROM transactions
                        GROUP BY action
                        ORDER BY count DESC
                    """)

                    action_stats = cursor.fetchall()
                    stats['by_action'] = [
                        {
                            "action": row[0],
                            "count": row[1],
                            "total_sum": float(row[2]) if row[2] else 0
                        }
                        for row in action_stats
                    ]

                    cursor.execute(
                        "SELECT COUNT(DISTINCT user_id) FROM transactions")
                    stats['unique_users'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT user_id, action, sum, created_at
                        FROM transactions
                        ORDER BY created_at DESC
                        LIMIT 10
                    """)

                    recent = cursor.fetchall()
                    stats['recent_transactions'] = [
                        {
                            "user_id": row[0],
                            "action": row[1],
                            "sum": float(row[2]) if row[2] else None,
                            "created_at": row[3].isoformat() if row[3] else None
                        }
                        for row in recent
                    ]

                return stats

        except Exception as e:
            print(f"[DB] ✗ Ошибка получения статистики транзакций: {e}")
            return {}

    def get_user_events_summary(self, user_id: int) -> Dict[str, Any]:
        """
        Получает сводку по событиям пользователя из таблицы users
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT ftm_time, reg, reg_time, dep, dep_time, dep_sum, 
                               redep, redep_time, redep_sum, subscriber_id, trader_id,
                               clickid_chatterfry, sub_3
                        FROM users
                        WHERE id = %s
                    """, (user_id,))

                    result = cursor.fetchone()

                    if result:
                        return {
                            "user_id": user_id,
                            "ftm_time": result[0].isoformat() if result[0] else None,
                            "reg": result[1],
                            "reg_time": result[2].isoformat() if result[2] else None,
                            "dep": result[3],
                            "dep_time": result[4].isoformat() if result[4] else None,
                            "dep_sum": float(result[5]) if result[5] else None,
                            "redep": result[6],
                            "redep_time": result[7].isoformat() if result[7] else None,
                            "redep_sum": float(result[8]) if result[8] else None,
                            "subscriber_id": result[9],
                            "trader_id": result[10],
                            "clickid_chatterfry": result[11],
                            "sub_3": result[12]
                        }
                    else:
                        return {"error": "User not found"}

        except Exception as e:
            print(f"[DB] ✗ Ошибка получения событий пользователя: {e}")
            return {"error": str(e)}

    def get_user_by_subscriber_id(self, subscriber_id: str) -> Optional[int]:
        """
        Получает user_id по subscriber_id (UUID формат: 1cd38701-7e6e-4ce7-8161-9ce3011a0cfb)
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT id FROM users WHERE subscriber_id = %s",
                        (subscriber_id,)
                    )
                    result = cursor.fetchone()

                    if result:
                        user_id = result[0]
                        print(
                            f"[DB] Найден пользователь {user_id} по subscriber_id={subscriber_id}")
                        return user_id
                    else:
                        print(
                            f"[DB] Пользователь с subscriber_id={subscriber_id} не найден")
                        return None

        except Exception as e:
            print(f"[DB] Ошибка поиска пользователя по subscriber_id: {e}")
            return None

    def find_user_by_any_id(
        self,
        user_id: int = None,
        subscriber_id: str = None
    ) -> Optional[Dict[str, Any]]:
        """
        Ищет пользователя по любому из идентификаторов.
        Возвращает user_id и способ нахождения.

        Args:
            user_id: Telegram ID (числовой)
            subscriber_id: UUID идентификатор

        Returns:
            Dict с user_id и found_by или None
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Сначала ищем по user_id (приоритет)
                    if user_id:
                        cursor.execute(
                            "SELECT id FROM users WHERE id = %s", (user_id,))
                        result = cursor.fetchone()
                        if result:
                            print(f"[DB] Найден пользователь по id={user_id}")
                            return {"user_id": result[0], "found_by": "user_id"}

                    # Затем ищем по subscriber_id
                    if subscriber_id:
                        cursor.execute(
                            "SELECT id FROM users WHERE subscriber_id = %s",
                            (subscriber_id,)
                        )
                        result = cursor.fetchone()
                        if result:
                            print(
                                f"[DB] Найден пользователь по subscriber_id={subscriber_id}")
                            return {"user_id": result[0], "found_by": "subscriber_id"}

                    print(
                        f"[DB] Пользователь не найден: id={user_id}, subscriber_id={subscriber_id}")
                    return None

        except Exception as e:
            print(f"[DB] Ошибка поиска пользователя: {e}")
            return None

    # ==========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С TRADER_ID
    # ==========================================

    def update_user_trader_id(self, user_id: int, trader_id: str) -> Dict[str, Any]:
        """
        Обновляет trader_id пользователя в БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "UPDATE users SET trader_id = %s WHERE id = %s",
                        (trader_id, user_id)
                    )

                    if cursor.rowcount > 0:
                        print(
                            f"[DB] ✓ Обновлен trader_id для user {user_id}: {trader_id}")
                        return {"success": True, "updated_rows": cursor.rowcount}
                    else:
                        print(f"[DB] ✗ Пользователь {user_id} не найден")
                        return {"success": False, "error": "User not found"}

        except Exception as e:
            print(f"[DB] ✗ Ошибка обновления trader_id: {e}")
            return {"success": False, "error": str(e)}

    def get_user_trader_id(self, user_id: int) -> Optional[str]:
        """
        Получает trader_id пользователя из БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT trader_id FROM users WHERE id = %s",
                        (user_id,)
                    )
                    result = cursor.fetchone()

                    if result and result[0]:
                        return result[0]
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения trader_id: {e}")
            return None

    # ==========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С KEITARO
    # ==========================================

    def get_user_sub_id(self, user_id: int) -> Optional[str]:
        """
        Получает sub_id (sub_3) пользователя из БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT sub_3 FROM users WHERE id = %s", (user_id,))
                    result = cursor.fetchone()

                    if result and result[0]:
                        sub_id = result[0]
                        print(
                            f"[DB] Найден sub_id для пользователя {user_id}: {sub_id}")
                        return sub_id
                    else:
                        print(
                            f"[DB] sub_id не найден для пользователя {user_id}")
                        return None

        except Exception as e:
            print(f"[DB] Ошибка получения sub_id: {e}")
            return None

    def get_all_users_with_sub_id(self) -> List[Dict[str, Any]]:
        """
        Получает всех пользователей с sub_id из БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, sub_3
                        FROM users
                        WHERE sub_3 IS NOT NULL AND sub_3 != ''
                    """)
                    results = cursor.fetchall()

                    users = []
                    for row in results:
                        users.append({
                            "user_id": row[0],
                            "sub_id": row[1]
                        })

                    print(f"[DB] Найдено {len(users)} пользователей с sub_id")
                    return users

        except Exception as e:
            print(f"[DB] ✗ Ошибка получения пользователей с sub_id: {e}")
            return []

    def get_campaign_data_stats(self) -> Dict[str, int]:
        """
        Получает статистику по заполненности данных кампаний
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM users")
                    total_users = cursor.fetchone()[0]

                    cursor.execute(
                        "SELECT COUNT(*) FROM users WHERE sub_3 IS NOT NULL AND sub_3 != ''")
                    users_with_sub_id = cursor.fetchone()[0]

                    cursor.execute(
                        "SELECT COUNT(*) FROM users WHERE company IS NOT NULL OR company_id IS NOT NULL")
                    users_with_campaign = cursor.fetchone()[0]

                    cursor.execute(
                        "SELECT COUNT(*) FROM users WHERE company IS NOT NULL AND company_id IS NOT NULL")
                    users_with_full_campaign = cursor.fetchone()[0]

                    return {
                        "total_users": total_users,
                        "users_with_sub_id": users_with_sub_id,
                        "users_with_campaign_data": users_with_campaign,
                        "users_with_full_campaign_data": users_with_full_campaign
                    }

        except Exception as e:
            print(f"[DB] Ошибка получения статистики: {e}")
            return {}

    def update_user_campaign_landing_data(self, user_id: int,
                                          company: str = None, company_id: int = None,
                                          landing: str = None, landing_id: int = None,
                                          country: str = None):
        """
        Обновляет данные кампании для пользователя
        """
        try:
            with self.get_connection() as conn:
                print(f"[DB UPDATE] Начинаем обновление user_id={user_id}")

                update_fields = []
                params = []

                if company is not None:
                    update_fields.append("company = %s")
                    params.append(company)

                if company_id is not None:
                    update_fields.append("company_id = %s")
                    params.append(company_id)

                if landing is not None:
                    update_fields.append("landing = %s")
                    params.append(landing)

                if landing_id is not None:
                    update_fields.append("landing_id = %s")
                    params.append(landing_id)

                if country is not None:
                    update_fields.append("country = %s")
                    params.append(country)

                if not update_fields:
                    return {"success": False, "error": "No fields to update"}

                params.append(user_id)
                query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"

                with conn.cursor() as cursor:
                    cursor.execute(query, params)

                    if cursor.rowcount > 0:
                        print(
                            f"[DB UPDATE] ✓ Успешно обновлен user_id={user_id}")
                        return {"success": True, "updated_rows": cursor.rowcount}
                    else:
                        print(
                            f"[DB UPDATE] ✗ Пользователь {user_id} не найден в БД")
                        return {"success": False, "error": "User not found"}

        except Exception as e:
            print(
                f"[DB UPDATE] ✗ Исключение при обновлении user_id={user_id}: {e}")
            return {"success": False, "error": str(e)}

    def get_users_without_campaign_landing_data(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с sub_id, у которых нет данных кампании
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, sub_3
                        FROM users
                        WHERE 
                            sub_3 IS NOT NULL 
                            AND sub_3 != ''
                            AND (
                                company IS NULL 
                                OR company_id IS NULL 
                                OR landing IS NULL 
                                OR landing_id IS NULL
                                OR country IS NULL
                            )
                        ORDER BY id
                        LIMIT 1000
                    """)
                    results = cursor.fetchall()

                    users = []
                    for row in results:
                        users.append({
                            "user_id": row[0],
                            "sub_id": row[1]
                        })

                    print(
                        f"[DB] Найдено {len(users)} пользователей для обработки")
                    return users

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей: {e}")
            return []

    def get_users_with_empty_markers_extended(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с пустыми маркерами для повторной проверки
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, sub_3
                        FROM users 
                        WHERE 
                            sub_3 IS NOT NULL 
                            AND sub_3 != ''
                            AND (
                                (company = %s AND company_id = %s) 
                                OR 
                                (landing = %s AND landing_id = %s)
                            )
                    """, ('None', -1, 'None', -1))
                    results = cursor.fetchall()

                    users = []
                    for row in results:
                        users.append({
                            "user_id": row[0],
                            "sub_id": row[1]
                        })

                    print(
                        f"[DB] Найдено {len(users)} пользователей с пустыми маркерами")
                    return users

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей с маркерами: {e}")
            return []

    def get_campaign_landing_stats(self) -> Dict[str, Any]:
        """
        Получает расширенную статистику по кампаниям и лендингам
        """
        try:
            with self.get_connection() as conn:
                stats = {}

                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM users")
                    stats['total_users'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE company IS NOT NULL 
                        AND company != 'None' 
                        AND company_id IS NOT NULL 
                        AND company_id != -1
                        AND landing IS NOT NULL 
                        AND landing != 'None'
                        AND landing_id IS NOT NULL 
                        AND landing_id != -1
                    """)
                    stats['users_with_full_data'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE (company = 'None' AND company_id = -1)
                        OR (landing = 'None' AND landing_id = -1)
                    """)
                    stats['users_with_empty_markers'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE (company IS NULL OR company_id IS NULL 
                            OR landing IS NULL OR landing_id IS NULL OR country IS NULL)
                    """)
                    stats['users_with_null_data'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE country IS NOT NULL AND country != 'None'
                    """)
                    stats['users_with_country'] = cursor.fetchone()[0]

                return stats

        except Exception as e:
            print(f"[DB] Ошибка получения статистики: {e}")
            return {}

    def get_users_with_null_campaign_landing_data(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с NULL полями и sub_id
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT id, sub_3
                        FROM users
                        WHERE 
                            sub_3 IS NOT NULL 
                            AND sub_3 != ''
                            AND (
                                company IS NULL 
                                OR company_id IS NULL 
                                OR landing IS NULL 
                                OR landing_id IS NULL
                                OR country IS NULL
                            )
                            AND NOT (
                                company = 'None' 
                                AND company_id = -1 
                                AND landing = 'None' 
                                AND landing_id = -1
                            )
                        ORDER BY id
                        LIMIT 1000
                    """)
                    results = cursor.fetchall()

                    users = []
                    for row in results:
                        users.append({
                            "user_id": row[0],
                            "sub_id": row[1]
                        })

                    print(
                        f"[DB] Найдено {len(users)} пользователей для обработки")
                    return [{"user_id": u["user_id"], "sub_id": u["sub_id"]} for u in users]

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей с NULL полями: {e}")
            return []

    def get_detailed_users_stats(self) -> Dict[str, Any]:
        """
        Детальная статистика по пользователям и их статусам
        """
        try:
            with self.get_connection() as conn:
                stats = {}

                with conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM users")
                    stats['total_users'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE company IS NOT NULL 
                        AND company != 'None' 
                        AND company_id IS NOT NULL 
                        AND company_id != -1
                        AND landing IS NOT NULL 
                        AND landing != 'None'
                        AND landing_id IS NOT NULL 
                        AND landing_id != -1
                    """)
                    stats['users_with_full_data'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE company = 'None' 
                        AND company_id = -1
                        AND landing = 'None' 
                        AND landing_id = -1
                    """)
                    stats['users_marked_as_empty'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE clickid_chatterfry IS NOT NULL 
                        AND clickid_chatterfry != ''
                    """)
                    stats['users_with_clickid'] = cursor.fetchone()[0]

                    cursor.execute("""
                        SELECT COUNT(*) FROM users 
                        WHERE country IS NOT NULL AND country != 'None'
                    """)
                    stats['users_with_country'] = cursor.fetchone()[0]

                    if stats['total_users'] > 0:
                        stats['percent_with_data'] = round(
                            (stats['users_with_full_data'] /
                             stats['total_users']) * 100, 2
                        )
                        stats['percent_with_clickid'] = round(
                            (stats['users_with_clickid'] /
                             stats['total_users']) * 100, 2
                        )

                    cursor.execute("""
                        SELECT company, COUNT(*) as count
                        FROM users 
                        WHERE company IS NOT NULL AND company != 'None'
                        GROUP BY company 
                        ORDER BY count DESC 
                        LIMIT 5
                    """)
                    top_campaigns = cursor.fetchall()
                    stats['top_campaigns'] = [
                        {"name": row[0], "count": row[1]} for row in top_campaigns
                    ]

                    cursor.execute("""
                        SELECT country, COUNT(*) as count
                        FROM users 
                        WHERE country IS NOT NULL AND country != 'None'
                        GROUP BY country 
                        ORDER BY count DESC 
                        LIMIT 10
                    """)
                    top_countries = cursor.fetchall()
                    stats['top_countries'] = [
                        {"country": row[0], "count": row[1]} for row in top_countries
                    ]

                return stats

        except Exception as e:
            print(f"[DB] Ошибка получения детальной статистики: {e}")
            return {}

    def get_user_country(self, user_id: int) -> Optional[str]:
        """
        Получает страну пользователя из БД
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT country FROM users WHERE id = %s", (user_id,))
                    result = cursor.fetchone()

                    if result:
                        return result[0]
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения страны: {e}")
            return None

    def check_duplicate_transaction(
        self,
        user_id: int,
        action: str,
        sum_amount: float = None,
        time_window_seconds: int = 60
    ) -> bool:
        """
        Проверяет наличие дублирующей транзакции в заданном временном окне.
        Помогает избежать дублей при повторных запросах.

        Args:
            user_id: ID пользователя
            action: Тип действия (ftm, reg, dep, redep)
            sum_amount: Сумма (для dep/redep)
            time_window_seconds: Временное окно в секундах

        Returns:
            True если дубликат найден, False если нет
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    if sum_amount is not None:
                        cursor.execute("""
                            SELECT COUNT(*) FROM transactions 
                            WHERE user_id = %s 
                            AND action = %s 
                            AND sum = %s
                            AND created_at > NOW() - INTERVAL '%s seconds'
                        """, (user_id, action, sum_amount, time_window_seconds))
                    else:
                        cursor.execute("""
                            SELECT COUNT(*) FROM transactions 
                            WHERE user_id = %s 
                            AND action = %s 
                            AND created_at > NOW() - INTERVAL '%s seconds'
                        """, (user_id, action, time_window_seconds))

                    count = cursor.fetchone()[0]

                    if count > 0:
                        print(
                            f"[DB] ⚠️ Найден дубликат транзакции: user={user_id}, action={action}, sum={sum_amount}")
                        return True
                    return False

        except Exception as e:
            print(f"[DB] Ошибка проверки дубликата: {e}")
            return False
