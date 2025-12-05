import os
from dotenv import load_dotenv

# Загружаем переменные из .env файла
load_dotenv()

# ===========================================
# DATABASE CONFIGURATION
# ===========================================
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "your_db")
}

# ===========================================
# KEITARO CONFIGURATION
# ===========================================
KEITARO_DOMAIN = os.getenv("KEITARO_DOMAIN", "https://test.com")
KEITARO_ADMIN_API_KEY = os.getenv("KEITARO_ADMIN_API_KEY", "test")
KEITARO_POSTBACK_URL = os.getenv(
    "KEITARO_POSTBACK_URL", "https://ytgtech.com/e87f58c/postback")

# ===========================================
# SYNC SETTINGS
# ===========================================
MAX_USERS_PER_SECOND = int(os.getenv("MAX_USERS_PER_SECOND", 2))
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", 0.5))
AUTO_CHECK_INTERVAL = int(os.getenv("AUTO_CHECK_INTERVAL", 3600))

# ===========================================
# API SETTINGS
# ===========================================
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", 8000))

# ===========================================
# TELEGRAM BOT SETTINGS (для логов ошибок)
# ===========================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "your_bot_token_here")
CHAT_ID = os.getenv("CHAT_ID", "your_chat_id_here")
ENABLE_TELEGRAM_LOGS = os.getenv(
    "ENABLE_TELEGRAM_LOGS", "true").lower() == "true"

# Выводим конфигурацию при загрузке (без пароля и API ключа)
print("=" * 50)
print("📋 ЗАГРУЖЕННАЯ КОНФИГУРАЦИЯ")
print("=" * 50)
print(f"DB Host: {DB_CONFIG['host']}")
print(f"DB Port: {DB_CONFIG['port']}")
print(f"DB User: {DB_CONFIG['user']}")
print(f"DB Name: {DB_CONFIG['database']}")
print(f"DB Password: {'*' * len(DB_CONFIG['password'])}")
print("-" * 50)
print(f"Keitaro Domain: {KEITARO_DOMAIN}")
print(f"Keitaro API Key: {KEITARO_ADMIN_API_KEY[:10]}...")
print("-" * 50)
print(f"Max Users/Second: {MAX_USERS_PER_SECOND}")
print(f"Delay Between Requests: {DELAY_BETWEEN_REQUESTS}s")
print(
    f"Auto Check Interval: {AUTO_CHECK_INTERVAL}s ({AUTO_CHECK_INTERVAL // 60} min)")
print("-" * 50)
print(f"API Host: {API_HOST}")
print(f"API Port: {API_PORT}")
print("-" * 50)
print(f"Telegram Bot Enabled: {ENABLE_TELEGRAM_LOGS}")
if ENABLE_TELEGRAM_LOGS:
    if BOT_TOKEN != "your_bot_token_here":
        print(f"Bot Token: {BOT_TOKEN[:10]}...")
    else:
        print("Bot Token: ⚠️ НЕ НАСТРОЕН")
    if CHAT_ID != "your_chat_id_here":
        print(f"Chat ID: {CHAT_ID}")
    else:
        print("Chat ID: ⚠️ НЕ НАСТРОЕН")
print("=" * 50)
