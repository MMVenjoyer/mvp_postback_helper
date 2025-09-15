import psycopg2
from config import DB_CONFIG
from typing import List, Dict, Any, Optional


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def update_started_chat(self, user_id: int, status: int):
        with self.conn.cursor() as cur:
            # Сначала проверим, что пользователь действительно есть
            print(
                f"[DB] Ищем пользователя с id={user_id} (тип: {type(user_id)})")
            cur.execute(
                "SELECT id, started_chat FROM users WHERE id = %s", (user_id,))
            user_before = cur.fetchone()
            print(f"[DB] Пользователь до обновления: {user_before}")

            if not user_before:
                print(f"[DB] ВНИМАНИЕ: Пользователь не найден при поиске!")
                # Попробуем найти похожих пользователей
                cur.execute(
                    "SELECT id FROM users WHERE CAST(id AS TEXT) LIKE %s LIMIT 5", (f"%{user_id}%",))
                similar = cur.fetchall()
                print(f"[DB] Похожие ID в БД: {similar}")
                return 0

            # Выполняем UPDATE
            print(
                f"[DB] Выполняем UPDATE для user_id={user_id}, status={status}")
            cur.execute("""
                UPDATE users SET started_chat = %s WHERE id = %s;
            """, (status, user_id))

            rows_affected = cur.rowcount
            print(f"[DB] Обновлено строк: {rows_affected}")

            # Проверяем результат после UPDATE
            cur.execute(
                "SELECT id, started_chat FROM users WHERE id = %s", (user_id,))
            user_after = cur.fetchone()
            print(f"[DB] Пользователь после обновления: {user_after}")

            return rows_affected

    def get_all_users_with_sub_3s(self) -> List[Dict[str, Any]]:
        """
        Получает всех пользователей с sub_3 из БД
        """
        try:
            with self.conn.cursor() as cursor:  # ИСПРАВЛЕНО
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
            with self.conn.cursor() as cursor:  # ИСПРАВЛЕНО
                # Всего пользователей
                cursor.execute("SELECT COUNT(*) FROM users")
                total_users = cursor.fetchone()[0]

                # Пользователи с sub_3
                cursor.execute(
                    "SELECT COUNT(*) FROM users WHERE sub_3 IS NOT NULL AND sub_3 != ''")
                users_with_sub_3 = cursor.fetchone()[0]

                # Пользователи с данными кампаний
                cursor.execute(
                    "SELECT COUNT(*) FROM users WHERE company IS NOT NULL OR company_id IS NOT NULL")
                users_with_campaign = cursor.fetchone()[0]

                # Пользователи с полными данными кампаний
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
            with self.conn.cursor() as cursor:  # ИСПРАВЛЕНО
                cursor.execute(
                    "SELECT sub_3 FROM users WHERE id = %s", (user_id,))
                result = cursor.fetchone()

                if result:
                    sub_3 = result[0]
                    print(
                        f"[DB] Найден sub_3 для пользователя {user_id}: {sub_3}")
                    return sub_3
                else:
                    print(
                        f"[DB] sub_3 не найден для пользователя {user_id}")
                    return None

        except Exception as e:
            print(f"[DB] Ошибка получения sub_3: {e}")
            return None

    def update_user_campaign_data(self, user_id: int, company: str = None, company_id: int = None):
        """
        Обновляет данные кампании (company, company_id) для пользователя
        """
        try:
            # Формируем динамический запрос в зависимости от переданных параметров
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
                # НЕ нужно conn.commit() - у вас autocommit = True

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
        Получает пользователей без данных кампании (company IS NULL и company_id IS NULL)
        """
        # try:
        with self.conn.cursor() as cursor:
            cursor.execute("""
                    SELECT id, sub_3
                    FROM users
                    WHERE sub_3 IS NOT NULL
                    AND sub_3 != ''
                    AND (company IS NULL AND company_id IS NULL)
                """)
            results = cursor.fetchall()

            users = []
            for row in results:
                users.append({
                    "user_id": row[0],
                    "sub_3": row[1]
                })

            print(
                f"[DB] Найдено {len(users)} пользователей без данных кампании")
            return users

        # except Exception as e:
        #     print(
        #         f"[DB] Ошибка получения пользователей без данных кампании: {e}")
        #     return []

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
        Получает детальную статистику по данным кампаний
        """
        try:
            with self.conn.cursor() as cursor:
                stats = {}

                # Всего пользователей
                cursor.execute("SELECT COUNT(*) FROM users")
                stats['total_users'] = cursor.fetchone()[0]

                # Пользователи с sub_3
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NOT NULL AND sub_3 != ''
                """)
                stats['users_with_sub_3'] = cursor.fetchone()[0]

                # Пользователи без данных кампании (нужно обработать)
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NOT NULL 
                    AND sub_3 != ''
                    AND (company IS NULL AND company_id IS NULL)
                """)
                stats['users_without_campaign_data'] = cursor.fetchone()[0]

                # Пользователи с реальными данными кампании
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company IS NOT NULL 
                    AND company != 'None' 
                    AND company_id IS NOT NULL 
                    AND company_id != -1
                """)
                stats['users_with_real_campaign_data'] = cursor.fetchone()[0]

                # Пользователи с пустыми маркерами (данных не найдено в Keitaro)
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE company = 'None' AND company_id = -1
                """)
                stats['users_with_empty_markers'] = cursor.fetchone()[0]

                # Пользователи без sub_3
                cursor.execute("""
                    SELECT COUNT(*) FROM users 
                    WHERE sub_3 IS NULL OR sub_3 = ''
                """)
                stats['users_without_sub_3'] = cursor.fetchone()[0]

                # Топ кампании
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
