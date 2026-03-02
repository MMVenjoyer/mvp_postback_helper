# Keitaro Postback Service v2.1.0

Сервис для обработки постбэков от внешних систем (Chatterfry, MVP и др.) с интеграцией в Keitaro.

## 📋 Содержание

- [Архитектура](#архитектура)
- [API Endpoints](#api-endpoints)
- [Маппинг параметров и полей БД](#маппинг-параметров-и-полей-бд)
- [Идентификаторы пользователей](#идентификаторы-пользователей)
- [Потоки данных](#потоки-данных)
- [Установка и запуск](#установка-и-запуск)
- [Изменения в версии 2.1.0](#изменения-в-версии-210)

---

## 🏗️ Архитектура

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────┐
│   Chatterfry    │────▶│  Postback API    │────▶│  PostgreSQL │
│   MVP Platform  │     │  (FastAPI)       │     │  (users,    │
│   Other Sources │     │                  │     │  transactions)
└─────────────────┘     └────────┬─────────┘     └─────────────┘
                                 │
                                 ▼
                        ┌────────────────┐
                        │    Keitaro     │
                        │  (Postbacks)   │
                        └────────────────┘
```

---

## 🔌 API Endpoints

### Postback Endpoints

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `/postback/ftm` | GET | First Time Message - первое сообщение пользователя |
| `/postback/reg` | GET | Registration - регистрация на платформе |
| `/postback/dep` | GET | Deposit - первый депозит (SALE в Keitaro) |
| `/postback/redep` | GET | Redeposit - повторный депозит (DEP в Keitaro) |

### Вспомогательные Endpoints

| Endpoint | Метод | Описание |
|----------|-------|----------|
| `/postback/test/{user_id}` | GET | Тестовый просмотр данных пользователя |
| `/postback/stats` | GET | Статистика по транзакциям |
| `/postback/user/{user_id}/history` | GET | История транзакций пользователя |
| `/postback/lookup` | GET | Поиск пользователя по id или subscriber_id |

---

## 🗺️ Маппинг параметров и полей БД

### Таблица `users`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ТАБЛИЦА users                                       │
├─────────────────────┬────────────────────┬──────────────────────────────────────┤
│ Поле в БД           │ Параметр API       │ Описание                             │
├─────────────────────┼────────────────────┼──────────────────────────────────────┤
│ id                  │ id, {chatId}       │ Telegram User ID (PRIMARY KEY)       │
│ subscriber_id       │ subscriber_id      │ UUID от MVP (для обратной совмест.)  │
│ trader_id           │ trader_id          │ ID трейдера из MVP платформы         │
│ clickid_chatterfry  │ clickid            │ Click ID из трекера Chatterfry       │
│ sub_3               │ -                  │ Sub ID для Keitaro (из бота)         │
├─────────────────────┼────────────────────┼──────────────────────────────────────┤
│ ftm_time            │ -                  │ Время первого сообщения              │
│ reg                 │ -                  │ Флаг регистрации (BOOLEAN)           │
│ reg_time            │ -                  │ Время регистрации                    │
│ dep                 │ -                  │ Флаг первого депозита (BOOLEAN)      │
│ dep_time            │ -                  │ Время первого депозита               │
│ dep_sum             │ sum                │ Сумма первого депозита               │
│ redep               │ -                  │ Флаг редепозита (BOOLEAN)            │
│ redep_time          │ -                  │ Время редепозита                     │
│ redep_sum           │ sum                │ Сумма редепозита                     │
├─────────────────────┼────────────────────┼──────────────────────────────────────┤
│ company             │ -                  │ Название кампании из Keitaro         │
│ company_id          │ -                  │ ID кампании из Keitaro               │
│ landing             │ -                  │ Название лендинга из Keitaro         │
│ landing_id          │ -                  │ ID лендинга из Keitaro               │
│ country             │ -                  │ Страна пользователя из Keitaro       │
│ created_at          │ -                  │ Дата создания записи                 │
└─────────────────────┴────────────────────┴──────────────────────────────────────┘
```

### Таблица `transactions`

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           ТАБЛИЦА transactions                                   │
├─────────────────────┬────────────────────┬──────────────────────────────────────┤
│ Поле в БД           │ Источник           │ Описание                             │
├─────────────────────┼────────────────────┼──────────────────────────────────────┤
│ id                  │ AUTO               │ ID транзакции (PRIMARY KEY)          │
│ user_id             │ id параметр        │ Telegram User ID                     │
│ action              │ endpoint           │ ftm / reg / dep / redep              │
│ sum                 │ sum параметр       │ Сумма (для dep/redep)                │
│ raw_data            │ все параметры      │ JSON с полными данными запроса       │
│ created_at          │ AUTO               │ Дата создания транзакции             │
└─────────────────────┴────────────────────┴──────────────────────────────────────┘
```

---

## 🆔 Идентификаторы пользователей

### Схема идентификаторов

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        ИДЕНТИФИКАТОРЫ ПОЛЬЗОВАТЕЛЯ                              │
├─────────────────────┬─────────────────────┬─────────────────────────────────────┤
│ Идентификатор       │ Формат              │ Источник / Назначение               │
├─────────────────────┼─────────────────────┼─────────────────────────────────────┤
│ id (chatId)         │ 123456789           │ Telegram User ID - основной ключ    │
│                     │ (числовой)          │ Приходит во всех постбэках          │
├─────────────────────┼─────────────────────┼─────────────────────────────────────┤
│ subscriber_id       │ UUID v4             │ ID из старой системы MVP            │
│                     │ 1cd38701-7e6e-...   │ Для обратной совместимости          │
│                     │                     │ Используется в dep/redep            │
├─────────────────────┼─────────────────────┼─────────────────────────────────────┤
│ trader_id           │ TRD_12345           │ ID трейдера из MVP платформы        │
│                     │ (строковой)         │ Приходит при регистрации            │
├─────────────────────┼─────────────────────┼─────────────────────────────────────┤
│ clickid_chatterfry  │ abc123xyz           │ Click ID из трекера Chatterfry      │
│                     │ (строковой)         │ {tracker.clickid} в постбэках       │
├─────────────────────┼─────────────────────┼─────────────────────────────────────┤
│ sub_3 (subid)       │ 3tse38v.5c.507c     │ Sub ID для Keitaro                  │
│                     │ (Keitaro format)    │ Записывается ботом при старте       │
│                     │                     │ Используется для постбэков в Keitaro│
└─────────────────────┴─────────────────────┴─────────────────────────────────────┘
```

### Приоритет поиска пользователя

1. **По `id`** (Telegram User ID) - основной способ
2. **По `subscriber_id`** (UUID) - для обратной совместимости со старой системой

```python
# Логика поиска в dep/redep:
user = find_by_telegram_id(id)
if not user and subscriber_id:
    user = find_by_subscriber_id(subscriber_id)
if not user and id:
    user = create_new_user(id)
```

---

## 📊 Потоки данных

### FTM Postback

```
POST /postback/ftm?id=123456&clickid=abc123

1. Создать пользователя если не существует
2. Обновить clickid_chatterfry (если пустой)
3. Проверить на дубликат (30 сек окно)
4. Записать транзакцию (action=ftm)
5. Обновить ftm_time в users
6. Отправить постбэк в Keitaro (tid=4, status=ftm)
```

### REG Postback

```
POST /postback/reg?id=123456&trader_id=TRD_123&clickid=abc123

1. Создать пользователя если не существует
2. Обновить trader_id и clickid_chatterfry
3. Проверить на дубликат (30 сек окно)
4. Записать транзакцию (action=reg)
5. Обновить reg=TRUE, reg_time в users
6. Отправить постбэк в Keitaro (tid=5, status=reg)
```

### DEP Postback

```
POST /postback/dep?id=123456&sum=100&clickid=abc123
     или
POST /postback/dep?subscriber_id=uuid-xxx&sum=100

1. Найти пользователя по id ИЛИ subscriber_id
2. Создать если не найден (только если есть id)
3. Обновить clickid_chatterfry
4. Проверить на дубликат (60 сек окно)
5. Посчитать tid = 6 + количество предыдущих депозитов
6. Записать транзакцию (action=dep, sum)
7. Обновить dep=TRUE, dep_time, dep_sum в users
8. Отправить постбэк в Keitaro (status=sale, payout, tid)
```

### REDEP Postback

```
POST /postback/redep?id=123456&sum=200&clickid=abc123

Аналогично DEP, но:
- action=redep
- status=dep в Keitaro (не sale!)
- Обновляет redep=TRUE, redep_time, redep_sum
```

---

## 🔗 Интеграция с Keitaro

### Маппинг событий

| Наш action | Keitaro status | Keitaro tid | Описание |
|------------|----------------|-------------|----------|
| ftm | ftm | 4 | First Time Message |
| reg | reg | 5 | Registration |
| dep | **sale** | 6+ | Первый депозит |
| redep | **dep** | 6+ | Повторный депозит |

### Формат постбэка в Keitaro

```
https://ytgtech.com/e87f58c/postback?subid={sub_3}&status={status}&payout={sum}&tid={tid}
```

### Расчет TID для депозитов

```python
tid = 6 + count(previous_deposits)

# Примеры:
# Первый депозит: tid = 6 + 0 = 6
# Второй депозит: tid = 6 + 1 = 7
# Третий депозит: tid = 6 + 2 = 8
```

---

## 💾 Примеры запросов

### FTM
```bash
curl "https://tylerwhite.icu/postback/ftm?id=123456789"
curl "https://tylerwhite.icu/postback/ftm?id=123456789&clickid=tracker_click_123"
```

### REG
```bash
curl "https://tylerwhite.icu/postback/reg?id=123456789&trader_id=TRD_98765"
curl "https://tylerwhite.icu/postback/reg?id=123456789&trader_id=TRD_98765&clickid=abc123"
```

### DEP (с id)
```bash
curl "https://tylerwhite.icu/postback/dep?id=123456789&sum=100.50"
curl "https://tylerwhite.icu/postback/dep?id=123456789&sum=100.50&clickid=abc123"
```

### DEP (с subscriber_id для обратной совместимости)
```bash
curl "https://tylerwhite.icu/postback/dep?subscriber_id=1cd38701-7e6e-4ce7-8161-9ce3011a0cfb&sum=100"
```

### REDEP
```bash
curl "https://tylerwhite.icu/postback/redep?id=123456789&sum=250.00"
curl "https://tylerwhite.icu/postback/redep?subscriber_id=uuid-xxx&sum=250&clickid=xyz789"
```

### Поиск пользователя
```bash
curl "https://tylerwhite.icu/postback/lookup?id=123456789"
curl "https://tylerwhite.icu/postback/lookup?subscriber_id=1cd38701-7e6e-4ce7-8161-9ce3011a0cfb"
```

---

## 🛠️ Установка и запуск

### 1. Применить миграцию БД

```bash
psql -U postgres -d your_db -f migrations/001_add_clickid_chatterfry.sql
```

### 2. Настроить .env

```env
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=your_db

KEITARO_DOMAIN=https://your-keitaro.com
KEITARO_ADMIN_API_KEY=your_api_key
KEITARO_POSTBACK_URL=https://ytgtech.com/e87f58c/postback

BOT_TOKEN=your_telegram_bot_token
CHAT_ID=your_chat_id
ENABLE_TELEGRAM_LOGS=true
```

### 3. Запустить

```bash
pip install -r requirements.txt
python main.py
# или через PM2:
./start.sh
```

---

## 📝 Изменения в версии 2.1.0

### Новые функции

1. **Автоматическое создание пользователей**
   - Если пользователь не найден в БД - создается автоматически
   - Больше не теряем постбэки из-за отсутствия пользователя

2. **Поддержка clickid из Chatterfry**
   - Новое поле `clickid_chatterfry` в таблице users
   - Параметр `clickid` во всех постбэках
   - Записывается только если поле пустое (не перезаписывает)

3. **Поиск по subscriber_id для dep/redep**
   - Обратная совместимость со старой системой
   - Поиск сначала по id, затем по subscriber_id
   - Поддержка обоих идентификаторов одновременно

4. **Защита от дубликатов**
   - Проверка на повторные запросы в временном окне
   - FTM/REG: 30 секунд
   - DEP/REDEP: 60 секунд

### Исправления

1. **Singleton для DataBase**
   - Один connection pool на всё приложение
   - Исправлена утечка соединений

2. **Улучшенное логирование**
   - Подробные логи для отладки
   - Информация о создании пользователей

3. **Новый endpoint /postback/lookup**
   - Поиск пользователя по любому идентификатору
   - Полезно для отладки

---

## 🔍 Диагностика

### Проверить пользователя

```bash
curl "https://tylerwhite.icu/postback/test/123456789"
```

### Найти по subscriber_id

```bash
curl "https://tylerwhite.icu/postback/lookup?subscriber_id=1cd38701-7e6e-4ce7-8161-9ce3011a0cfb"
```
---

## ⚠️ Важные замечания

1. **Параметр sum**: если не передан или невалидный - используется 59 по умолчанию
2. **subscriber_id**: только для обратной совместимости, основной идентификатор - `id`
3. **clickid**: записывается только один раз, не перезаписывается
4. **Дубликаты**: повторные запросы в коротком окне игнорируются
5. **Keitaro**: dep отправляется как `sale`, redep как `dep`