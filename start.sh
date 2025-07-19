#!/bin/bash

APP_NAME="deeplink-service"
APP_ENTRY="main:app"
PORT=8000

echo "📦 Установка зависимостей..."
pip install -r requirements.txt

echo "🚀 Запуск через PM2: $APP_NAME на порту $PORT"
pm2 start "uvicorn $APP_ENTRY --host 0.0.0.0 --port $PORT" \
  --name "$APP_NAME" \
  --interpreter python3 \
  --restart-delay=5000 \
  --max-restarts=10

echo "✅ Готово. Статус:"
pm2 status "$APP_NAME"
