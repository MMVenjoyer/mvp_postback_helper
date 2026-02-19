#!/bin/bash

APP_NAME="deeplink-service"
APP_ENTRY="main:app"
PORT=8000
WORKERS=4

echo "📦 Установка зависимостей..."
pip install -r requirements.txt

echo "🚀 Запуск через PM2: $APP_NAME на порту $PORT ($WORKERS воркеров)"
pm2 start "uvicorn $APP_ENTRY --host 0.0.0.0 --port $PORT --workers $WORKERS --timeout-keep-alive 30" \
  --name "$APP_NAME" \
  --restart-delay=5000 \
  --max-restarts=10

echo "✅ Готово. Статус:"
pm2 status "$APP_NAME"