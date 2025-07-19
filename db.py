import psycopg2
from config import DB_CONFIG


class DataBase:
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.conn.autocommit = True

    def update_started_chat(self, user_id: int, status: int):
        with self.conn.cursor() as cur:
            cur.execute("""
                UPDATE users SET started_chat = %s WHERE id = %s;
            """, (status, user_id))
