"""
Интерактивный логин в Циан.
Запусти один раз — откроется браузер, залогинься вручную.
Сессия сохранится в файл и будет использоваться ботом.
"""
import os
from pathlib import Path
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()

SESSION_FILE = os.getenv("CIAN_SESSION_FILE", "cian_session.json")
SESSION_DIR = Path("cian_storage")


def main():
    print("=" * 50)
    print("🔐 Логин в Циан")
    print("=" * 50)
    print()
    print("Сейчас откроется браузер.")
    print("1. Залогинься в Циан (телефон + SMS)")
    print("2. Убедись что ты на главной странице залогиненным")
    print("3. Закрой браузер или нажми Enter в терминале")
    print()

    with sync_playwright() as p:
        # Используем persistent context — он сохраняет всё: cookies, localStorage, sessionStorage
        browser_context = p.chromium.launch_persistent_context(
            user_data_dir=str(SESSION_DIR),
            headless=False,  # Нужен GUI для ручного логина
            viewport={"width": 1280, "height": 800},
            locale="ru-RU",
            timezone_id="Europe/Moscow",
        )

        page = browser_context.new_page()
        page.goto("https://www.cian.ru/authenticate/", wait_until="domcontentloaded")

        print()
        print("⏳ Браузер открыт. Залогинься и нажми Enter здесь...")
        input()

        # Проверяем что залогинились — пробуем зайти в диалоги
        page.goto("https://www.cian.ru/dialogs/", wait_until="domcontentloaded")
        page.wait_for_timeout(3000)

        # Сохраняем cookies отдельно для надёжности
        cookies = browser_context.cookies()
        import json
        with open(SESSION_FILE, "w") as f:
            json.dump(cookies, f, ensure_ascii=False, indent=2)

        print(f"✅ Сессия сохранена в {SESSION_DIR}/ и {SESSION_FILE}")
        print(f"   Cookies: {len(cookies)} шт.")

        browser_context.close()

    print()
    print("Готово! Теперь можешь запускать main.py")


if __name__ == "__main__":
    main()
