#!/usr/bin/env python3
"""
Тестовый скрипт для проверки получения расширенных данных из Keitaro API
(страна, город, устройство, ОС, браузер)

Использование: python test_keitaro_extended.py
"""
import asyncio
import httpx
import sys
from datetime import datetime
from typing import Dict, Any

# Конфигурация (замените на свои данные)
KEITARO_DOMAIN = "https://ytgtech.com"  # Ваш домен Keitaro
KEITARO_ADMIN_API_KEY = "a3087a02038972201d55ab50b1d40143"  # Ваш API ключ

# Тестовые sub_id для проверки
TEST_SUB_IDS = [
    "25ndli0.92.9upr",  # Пример из вашего запроса
    # Добавьте больше sub_id для тестирования
]


async def get_conversion_data_extended(sub_id: str) -> Dict[str, Any]:
    """
    Получает расширенные данные конверсии из Keitaro API по sub_id
    """
    headers = {
        "Api-Key": KEITARO_ADMIN_API_KEY,
        "Content-Type": "application/json"
    }

    payload = {
        "limit": 1,
        "columns": [
            "campaign_id",
            "campaign",
            "landing_id",
            "landing",
            "country_flag",  # Код страны (US вместо United States)
            "city",          # Город
            "device_type",   # Тип устройства (desktop, mobile, tablet)
            "os",            # Операционная система
            "browser"        # Браузер
        ],
        "filters": [
            {
                "name": "sub_id",
                "operator": "EQUALS",
                "expression": sub_id
            }
        ]
    }

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{KEITARO_DOMAIN}/admin_api/v1/conversions/log",
                headers=headers,
                json=payload
            )

            print(f"\n📊 Запрос для sub_id: {sub_id}")
            print(f"   URL: {KEITARO_DOMAIN}/admin_api/v1/conversions/log")
            print(f"   Status Code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()

                # Показываем сырой ответ для отладки
                print(f"\n📦 Сырой ответ от Keitaro:")
                print(f"   Найдено записей: {len(data.get('rows', []))}")

                if data.get("rows") and len(data["rows"]) > 0:
                    row = data["rows"][0]

                    # Форматированный вывод всех данных
                    print(f"\n✅ ДАННЫЕ НАЙДЕНЫ:")
                    print(
                        f"   ├─ Campaign: {row.get('campaign')} (ID: {row.get('campaign_id')})")
                    print(
                        f"   ├─ Landing: {row.get('landing')} (ID: {row.get('landing_id')})")
                    print(f"   ├─ 🌍 Country: {row.get('country_flag')}")
                    print(f"   ├─ 🏙️  City: {row.get('city')}")
                    print(f"   ├─ 📱 Device Type: {row.get('device_type')}")
                    print(f"   ├─ 💻 OS: {row.get('os')}")
                    print(f"   └─ 🌐 Browser: {row.get('browser')}")

                    return {
                        "campaign_id": row.get("campaign_id"),
                        "campaign": row.get("campaign"),
                        "landing_id": row.get("landing_id"),
                        "landing": row.get("landing"),
                        "country": row.get("country_flag"),
                        "city": row.get("city"),
                        "device_type": row.get("device_type"),
                        "os": row.get("os"),
                        "browser": row.get("browser"),
                        "found": True
                    }
                else:
                    print(f"\n⚠️ Данные не найдены в ответе")
                    return {"found": False, "reason": "No data in response"}
            else:
                print(f"\n❌ HTTP Error: {response.status_code}")
                print(f"   Response: {response.text[:200]}")
                return {"found": False, "reason": f"API error: {response.status_code}"}

    except Exception as e:
        print(f"\n❌ Exception: {e}")
        import traceback
        traceback.print_exc()
        return {"found": False, "reason": str(e)}


async def test_multiple_sub_ids():
    """
    Тестирует получение данных для нескольких sub_id
    """
    print("=" * 80)
    print("🧪 ТЕСТИРОВАНИЕ РАСШИРЕННЫХ ДАННЫХ ИЗ KEITARO")
    print("=" * 80)
    print(f"\n🕐 Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🌐 Keitaro Domain: {KEITARO_DOMAIN}")
    print(f"🔑 API Key: {KEITARO_ADMIN_API_KEY[:10]}..." if KEITARO_ADMIN_API_KEY !=
          "your_api_key_here" else "⚠️ API Key не настроен!")

    if KEITARO_ADMIN_API_KEY == "your_api_key_here":
        print("\n❌ ОШИБКА: Не настроен KEITARO_ADMIN_API_KEY!")
        print("   Откройте файл test_keitaro_extended.py и установите ваш API ключ")
        return

    print(f"\n📋 Тестируем {len(TEST_SUB_IDS)} sub_id:")
    for i, sub_id in enumerate(TEST_SUB_IDS, 1):
        print(f"   {i}. {sub_id}")

    print("\n" + "=" * 80)

    results = []

    for sub_id in TEST_SUB_IDS:
        result = await get_conversion_data_extended(sub_id)
        results.append({
            "sub_id": sub_id,
            "data": result
        })

        # Небольшая задержка между запросами
        if len(TEST_SUB_IDS) > 1:
            await asyncio.sleep(1)

    # Итоговая статистика
    print("\n" + "=" * 80)
    print("📊 ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 80)

    found_count = sum(1 for r in results if r["data"].get("found"))
    not_found_count = len(results) - found_count

    print(f"\n✅ Найдено данных: {found_count}")
    print(f"❌ Не найдено: {not_found_count}")

    if found_count > 0:
        print(f"\n🎯 УСПЕШНЫЕ РЕЗУЛЬТАТЫ:")
        for r in results:
            if r["data"].get("found"):
                data = r["data"]
                print(f"\n📍 {r['sub_id']}:")
                print(f"   Country: {data.get('country')}")
                print(f"   City: {data.get('city')}")
                print(f"   Device: {data.get('device_type')}")
                print(f"   OS: {data.get('os')}")
                print(f"   Browser: {data.get('browser')}")

    print("\n" + "=" * 80)


async def test_single_sub_id(sub_id: str):
    """
    Тест для одного конкретного sub_id
    """
    print("=" * 80)
    print(f"🧪 ТЕСТ ОДНОГО SUB_ID: {sub_id}")
    print("=" * 80)

    result = await get_conversion_data_extended(sub_id)

    print("\n" + "=" * 80)
    print("📋 ФИНАЛЬНЫЙ РЕЗУЛЬТАТ (JSON)")
    print("=" * 80)

    import json
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\n" + "=" * 80)


async def main():
    """
    Главная функция
    """
    print("\n🚀 Выберите режим тестирования:")
    print("   1. Тест всех sub_id из списка")
    print("   2. Тест одного sub_id (ввести вручную)")
    print("   3. Быстрый тест с примером из вашего запроса")

    # В этом скрипте просто запускаем тест всех sub_id
    # Можно раскомментировать для интерактивного режима:
    # choice = input("\nВведите номер (1-3): ").strip()

    # Для автоматического тестирования используем режим 3 (быстрый тест)
    print("\n✨ Запуск быстрого теста с примером...")

    if TEST_SUB_IDS:
        await test_single_sub_id(TEST_SUB_IDS[0])
    else:
        print("❌ Добавьте хотя бы один sub_id в TEST_SUB_IDS")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n❌ Прервано пользователем")
        sys.exit(0)
