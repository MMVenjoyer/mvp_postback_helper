import psycopg2
import psycopg2.extras
from config import DB_CONFIG
from typing import List, Dict, Any, Optional
from datetime import datetime
import json


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

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
            with self.conn.cursor() as cursor:
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
            update_fields = []
            params = []

            if action == "ftm":
                update_fields = ["ftm_time = %s"]
                params = [datetime.now()]

            elif action == "reg":
                update_fields = ["reg = TRUE", "reg_time = %s"]
                params = [datetime.now()]

            elif action == "dep":
                update_fields = ["dep = TRUE", "dep_time = %s", "dep_sum = %s"]
                params = [datetime.now(), sum_amount]

            elif action == "redep":
                update_fields = ["redep = TRUE",
                                 "redep_time = %s", "redep_sum = %s"]
                params = [datetime.now(), sum_amount]

            else:
                # Для кастомных целей просто записываем в транзакции, users не трогаем
                return {"success": True, "message": "Custom action, only transaction created"}

            params.append(user_id)

            query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"

            with self.conn.cursor() as cursor:
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
        Возвращает количество для вычисления tid
        """
        try:
            with self.conn.cursor() as cursor:
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
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
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
            with self.conn.cursor() as cursor:
                stats = {}

                # Общее количество транзакций
                cursor.execute("SELECT COUNT(*) FROM transactions")
                stats['total_transactions'] = cursor.fetchone()[0]

                # Статистика по действиям
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

                # Статистика по пользователям
                cursor.execute("""
                    SELECT COUNT(DISTINCT user_id) FROM transactions
                """)
                stats['unique_users'] = cursor.fetchone()[0]

                # Последние транзакции
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
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT ftm_time, reg, reg_time, dep, dep_time, dep_sum, 
                           redep, redep_time, redep_sum, subscriber_id
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
                        "subscriber_id": result[9]
                    }
                else:
                    return {"error": "User not found"}

        except Exception as e:
            print(f"[DB] ✗ Ошибка получения событий пользователя: {e}")
            return {"error": str(e)}

    def get_user_by_subscriber_id(self, subscriber_id: str) -> Optional[int]:
        """
        Получает user_id по subscriber_id (для постбэков от MVP)
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT id FROM users WHERE subscriber_id = %s", (subscriber_id,))
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

    # ==========================================
    # МЕТОДЫ ДЛЯ РАБОТЫ С KEITARO
    # ==========================================

    def get_user_sub_id(self, user_id: int) -> Optional[str]:
        """
        Получает sub_id (sub_3) пользователя из БД
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT sub_3 FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

                if result and result[0]:
                    sub_id = result[0]
                    print(
                        f"[DB] Найден sub_id для пользователя {user_id}: {sub_id}")
                    return sub_id
                else:
                    print(f"[DB] sub_id не найден для пользователя {user_id}")
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения sub_id: {e}")
            return None

    def get_all_users_with_sub_id(self) -> List[Dict[str, Any]]:
        """
        Получает всех пользователей с sub_id из БД
        """
        try:
            with self.conn.cursor() as cursor:
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
            with self.conn.cursor() as cursor:
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
            print(f"[DB UPDATE] Начинаем обновление user_id={user_id}")
            print(
                f"[DB UPDATE] Данные: company={company}, company_id={company_id}, landing={landing}, landing_id={landing_id}, country={country}")

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
                print(f"[DB UPDATE] Нет полей для обновления!")
                return {"success": False, "error": "No fields to update"}

            params.append(user_id)
            query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"

            print(f"[DB UPDATE] SQL: {query}")
            print(f"[DB UPDATE] Параметры: {params}")

            with self.conn.cursor() as cursor:
                cursor.execute(query, params)

                cursor.execute("""
                    SELECT company, company_id, landing, landing_id, country
                    FROM users WHERE id = %s
                """, (user_id,))
                result = cursor.fetchone()

                if cursor.rowcount > 0:
                    print(f"[DB UPDATE] ✓ Успешно обновлен user_id={user_id}")
                    if result:
                        print(
                            f"[DB UPDATE] Новые значения: company={result[0]}, company_id={result[1]}, landing={result[2]}, landing_id={result[3]}, country={result[4]}")
                    return {"success": True, "updated_rows": cursor.rowcount}
                else:
                    print(
                        f"[DB UPDATE] ✗ Пользователь {user_id} не найден в БД")
                    return {"success": False, "error": "User not found"}

        except Exception as e:
            print(
                f"[DB UPDATE] ✗ Исключение при обновлении user_id={user_id}: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def get_users_without_campaign_landing_data(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с sub_id, у которых нет данных кампании
        """
        try:
            with self.conn.cursor() as cursor:
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

                print(f"[DB] Найдено {len(users)} пользователей для обработки")

                if len(users) > 0:
                    print(
                        f"[DB] Первые 5 для обработки: {[(u['user_id'], u['sub_id']) for u in users[:5]]}")

                return users

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей: {e}")
            return []

    def get_users_with_empty_markers_extended(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с пустыми маркерами для повторной проверки
        """
        try:
            with self.conn.cursor() as cursor:
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
            with self.conn.cursor() as cursor:
                stats = {}

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
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NOT NULL 
                    AND sub_3 != ''
                    AND (
                        company IS NULL 
                        OR company_id IS NULL 
                        OR landing IS NULL 
                        OR landing_id IS NULL
                        OR country IS NULL
                    )
                """)
                total_with_null = cursor.fetchone()[0]
                print(
                    f"[DB] Всего пользователей с NULL полями: {total_with_null}")

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company = 'None' 
                    AND company_id = -1 
                    AND landing = 'None' 
                    AND landing_id = -1
                """)
                with_markers = cursor.fetchone()[0]
                print(
                    f"[DB] Пользователей с маркерами None/-1: {with_markers}")

                cursor.execute("""
                    SELECT id, sub_3, company, company_id, landing, landing_id, country
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
                        "sub_id": row[1],
                        "company": row[2],
                        "company_id": row[3],
                        "landing": row[4],
                        "landing_id": row[5],
                        "country": row[6]
                    })

                print(f"[DB] Найдено {len(users)} пользователей для обработки")

                if len(users) > 0:
                    print(f"[DB] Примеры первых 3 записей:")
                    for u in users[:3]:
                        print(
                            f"  - ID: {u['user_id']}, sub_id: {u['sub_id']}, company: {u['company']}, country: {u['country']}")

                return [{"user_id": u["user_id"], "sub_id": u["sub_id"]} for u in users]

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей с NULL полями: {e}")
            import traceback
            traceback.print_exc()
            return []

    def get_detailed_users_stats(self) -> Dict[str, Any]:
        """
        Детальная статистика по пользователям и их статусам
        """
        try:
            with self.conn.cursor() as cursor:
                stats = {}

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
                    WHERE (
                        company IS NULL 
                        OR company_id IS NULL 
                        OR landing IS NULL 
                        OR landing_id IS NULL
                        OR country IS NULL
                    )
                    AND NOT (
                        company = 'None' 
                        OR company_id = -1 
                        OR landing = 'None' 
                        OR landing_id = -1
                    )
                """)
                stats['users_not_processed'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE (
                        (company IS NOT NULL AND company != 'None')
                        OR (company_id IS NOT NULL AND company_id != -1)
                        OR (landing IS NOT NULL AND landing != 'None')
                        OR (landing_id IS NOT NULL AND landing_id != -1)
                    )
                    AND NOT (
                        company IS NOT NULL AND company != 'None'
                        AND company_id IS NOT NULL AND company_id != -1
                        AND landing IS NOT NULL AND landing != 'None'
                        AND landing_id IS NOT NULL AND landing_id != -1
                    )
                """)
                stats['users_partially_filled'] = cursor.fetchone()[0]

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
                    stats['percent_marked_empty'] = round(
                        (stats['users_marked_as_empty'] /
                         stats['total_users']) * 100, 2
                    )
                    stats['percent_not_processed'] = round(
                        (stats['users_not_processed'] /
                         stats['total_users']) * 100, 2
                    )
                    stats['percent_with_country'] = round(
                        (stats['users_with_country'] /
                         stats['total_users']) * 100, 2
                    )

                cursor.execute("""
                    SELECT company, COUNT(*) as count
                    FROM users 
                    WHERE company IS NOT NULL 
                    AND company != 'None'
                    GROUP BY company 
                    ORDER BY count DESC 
                    LIMIT 5
                """)
                top_campaigns = cursor.fetchall()
                stats['top_campaigns'] = [
                    {"name": row[0], "count": row[1]} for row in top_campaigns
                ]

                cursor.execute("""
                    SELECT landing, COUNT(*) as count
                    FROM users 
                    WHERE landing IS NOT NULL 
                    AND landing != 'None'
                    GROUP BY landing 
                    ORDER BY count DESC 
                    LIMIT 5
                """)
                top_landings = cursor.fetchall()
                stats['top_landings'] = [
                    {"name": row[0], "count": row[1]} for row in top_landings
                ]

                cursor.execute("""
                    SELECT country, COUNT(*) as count
                    FROM users 
                    WHERE country IS NOT NULL 
                    AND country != 'None'
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
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT country FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

                if result:
                    country = result[0]
                    print(
                        f"[DB] Найдена страна для пользователя {user_id}: {country}")
                    return country
                else:
                    print(f"[DB] Пользователь {user_id} не найден")
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения страны: {e}")
            return None
