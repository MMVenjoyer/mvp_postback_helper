import psycopg2
from config import DB_CONFIG


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
