#!/bin/bash

APP_NAME="deeplink-service"
APP_ENTRY="main:app"
PORT=8000

# НЕ используем --workers с psycopg2 ThreadedConnectionPool (не fork-safe)
# Один процесс uvicorn с async — достаточно для текущей нагрузки

echo "📦 Установка зависимостей..."
pip install -r requirements.txt

echo "🚀 Запуск через PM2: $APP_NAME на порту $PORT (single worker)"
pm2 start "uvicorn $APP_ENTRY --host 0.0.0.0 --port $PORT --timeout-keep-alive 15" \
  --name "$APP_NAME" \
  --restart-delay=5000 \
  --max-restarts=10

echo "✅ Готово. Статус:"
pm2 status "$APP_NAME"