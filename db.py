import psycopg2
from config import DB_CONFIG
from typing import List, Dict, Any, Optional


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def get_all_users_with_sub_3s(self) -> List[Dict[str, Any]]:
        """
        Получает всех пользователей с sub_3 из БД
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
                        "sub_3": row[1]
                    })

                print(f"[DB] Найдено {len(users)} пользователей с sub_3")
                return users

        except Exception as e:
            print(f"[DB] Ошибка получения пользователей с sub_3: {e}")
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
                users_with_sub_3 = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT COUNT(*) FROM users WHERE company IS NOT NULL OR company_id IS NOT NULL")
                users_with_campaign = cursor.fetchone()[0]

                cursor.execute(
                    "SELECT COUNT(*) FROM users WHERE company IS NOT NULL AND company_id IS NOT NULL")
                users_with_full_campaign = cursor.fetchone()[0]

                return {
                    "total_users": total_users,
                    "users_with_sub_3": users_with_sub_3,
                    "users_with_campaign_data": users_with_campaign,
                    "users_with_full_campaign_data": users_with_full_campaign
                }

        except Exception as e:
            print(f"[DB] Ошибка получения статистики: {e}")
            return {}

    def get_user_sub_3(self, user_id: int) -> Optional[str]:
        """
        Получает sub_3 пользователя из БД
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(
                    "SELECT sub_3 FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

                if result:
                    sub_3 = result[0]
                    print(
                        f"[DB] Найден sub_3 для пользователя {user_id}: {sub_3}")
                    return sub_3
                else:
                    print(f"[DB] sub_3 не найден для пользователя {user_id}")
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения sub_3: {e}")
            return None

    def update_user_campaign_data(self, user_id: int, company: str = None, company_id: int = None):
        """
        Обновляет данные кампании (company, company_id) для пользователя
        """
        try:
            update_fields = []
            params = [user_id]

            if company is not None:
                update_fields.append("company = %s")
                params.insert(-1, company)

            if company_id is not None:
                update_fields.append("company_id = %s")
                params.insert(-1, company_id)

            if not update_fields:
                return {"success": False, "error": "No fields to update"}

            query = f"UPDATE users SET {', '.join(update_fields)} WHERE id = %s"

            with self.conn.cursor() as cursor:
                cursor.execute(query, params)

                if cursor.rowcount > 0:
                    print(
                        f"[DB] Данные кампании обновлены для пользователя {user_id}")
                    return {"success": True, "updated_rows": cursor.rowcount}
                else:
                    print(f"[DB] Пользователь {user_id} не найден")
                    return {"success": False, "error": "User not found"}

        except Exception as e:
            print(f"[DB] Ошибка обновления данных кампании: {e}")
            return {"success": False, "error": str(e)}

    def get_users_without_campaign_data(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей БЕЗ данных кампании
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, sub_3
                    FROM users
                    WHERE sub_3 IS NOT NULL
                    AND sub_3 != ''
                    AND company = %s 
                    AND company_id = %s
                """, ('None', -1))
                results = cursor.fetchall()

                users = []
                for row in results:
                    users.append({
                        "user_id": row[0],
                        "sub_3": row[1]
                    })

                print(
                    f"[DB] Найдено {len(users)} пользователей с пустыми маркерами (None/-1)")
                return users

        except Exception as e:
            print(
                f"[DB] Ошибка получения пользователей с пустыми маркерами: {e}")
            return []

    def get_users_with_empty_markers(self) -> List[Dict[str, Any]]:
        """
        Получает пользователей с пустыми маркерами для повторной проверки
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, sub_3 
                    FROM users 
                    WHERE sub_3 IS NOT NULL 
                    AND sub_3 != ''
                    AND company = %s 
                    AND company_id = %s
                """, ('None', -1))
                results = cursor.fetchall()

                users = []
                for row in results:
                    users.append({
                        "user_id": row[0],
                        "sub_3": row[1]
                    })

                print(
                    f"[DB] Найдено {len(users)} пользователей с пустыми маркерами")
                return users

        except Exception as e:
            print(
                f"[DB] Ошибка получения пользователей с пустыми маркерами: {e}")
            return []

    def get_detailed_campaign_stats(self) -> Dict[str, int]:
        """
        ОБНОВЛЕННАЯ статистика с новой логикой
        """
        try:
            with self.conn.cursor() as cursor:
                stats = {}

                cursor.execute("SELECT COUNT(*) FROM users")
                stats['total_users'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NOT NULL AND sub_3 != ''
                """)
                stats['users_with_sub_3'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NOT NULL 
                    AND sub_3 != ''
                    AND (company IS NULL AND company_id IS NULL)
                """)
                stats['users_with_really_empty_data'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company = %s AND company_id = %s
                """, ('None', -1))
                stats['users_with_empty_markers'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company IS NOT NULL 
                    AND company != 'None' 
                    AND company_id IS NOT NULL 
                    AND company_id != -1
                """)
                stats['users_with_real_campaign_data'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NULL OR sub_3 = ''
                """)
                stats['users_without_sub_3'] = cursor.fetchone()[0]

                cursor.execute("""
                    SELECT company, COUNT(*) as count
                    FROM users 
                    WHERE company IS NOT NULL 
                    AND company != 'None'
                    GROUP BY company 
                    ORDER BY count DESC 
                    LIMIT 10
                """)
                top_campaigns = cursor.fetchall()
                stats['top_campaigns'] = [
                    {"name": row[0], "count": row[1]} for row in top_campaigns]

                return stats

        except Exception as e:
            print(f"[DB] Ошибка получения детальной статистики: {e}")
            return {}

    def update_user_campaign_landing_data(self, user_id: int,
                                          company: str = None, company_id: int = None,
                                          landing: str = None, landing_id: int = None,
                                          country: str = None):
        """
        ОБНОВЛЕННЫЙ метод с поддержкой country
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
        Получает всех пользователей у которых хотя бы одно поле NULL или имеет маркер None/-1
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id
                    FROM users
                    WHERE 
                        company IS NULL 
                        OR company_id IS NULL 
                        OR landing IS NULL 
                        OR landing_id IS NULL
                        OR country IS NULL
                    ORDER BY id
                    LIMIT 1000
                """)
                results = cursor.fetchall()

                users = []
                for row in results:
                    users.append({"user_id": row[0]})

                print(f"[DB] Найдено {len(users)} пользователей для обработки")

                if len(users) > 0:
                    print(
                        f"[DB] Первые 5 ID для обработки: {[u['user_id'] for u in users[:5]]}")

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
                    SELECT id
                    FROM users 
                    WHERE (
                        (company = %s AND company_id = %s) 
                        OR 
                        (landing = %s AND landing_id = %s)
                    )
                """, ('None', -1, 'None', -1))
                results = cursor.fetchall()

                users = []
                for row in results:
                    users.append({"user_id": row[0]})

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

                # Статистика по странам
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
        Получает ТОЛЬКО пользователей с NULL полями, 
        БЕЗ тех, кто уже помечен маркерами None/-1
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company IS NULL 
                    OR company_id IS NULL 
                    OR landing IS NULL 
                    OR landing_id IS NULL
                    OR country IS NULL
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
                    SELECT id, company, company_id, landing, landing_id, country
                    FROM users
                    WHERE (
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
                        "company": row[1],
                        "company_id": row[2],
                        "landing": row[3],
                        "landing_id": row[4],
                        "country": row[5]
                    })

                print(f"[DB] Найдено {len(users)} пользователей для обработки")

                if len(users) > 0:
                    print(f"[DB] Примеры первых 3 записей:")
                    for u in users[:3]:
                        print(
                            f"  - ID: {u['user_id']}, company: {u['company']}, company_id: {u['company_id']}, landing: {u['landing']}, landing_id: {u['landing_id']}, country: {u['country']}")

                return [{"user_id": u["user_id"]} for u in users]

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

                # Статистика по странам
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

                # Топ страны
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
        НОВЫЙ МЕТОД: Получает страну пользователя из БД
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
