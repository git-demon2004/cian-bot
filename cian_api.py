"""
Модуль отправки сообщений через API Циан.
Не требует браузера — работает через HTTP-запросы с cookies.
"""
import json
import logging
import os
import threading
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

SESSION_FILE = os.getenv("CIAN_SESSION_FILE", "cian_session.json")
API_BASE = "https://api.cian.ru/chats/v1"

# Файл для трекинга уже уведомлённых чатов
_NOTIFIED_FILE = Path("notified_chats.json")

# Лок для Chromium-профиля — только один процесс за раз может открыть cian_storage
BROWSER_LOCK = threading.Lock()


def _load_notified() -> dict:
    """Загружает словарь {sender_name: last_preview} уже уведомлённых."""
    if _NOTIFIED_FILE.exists():
        with open(_NOTIFIED_FILE) as f:
            return json.load(f)
    return {}


def _save_notified(data: dict):
    """Сохраняет словарь уведомлённых."""
    with open(_NOTIFIED_FILE, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _get_session() -> requests.Session:
    """Создаёт requests.Session с cookies из файла."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        "Origin": "https://www.cian.ru",
        "Referer": "https://www.cian.ru/dialogs/",
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
    })

    cookie_file = Path(SESSION_FILE)
    if not cookie_file.exists():
        logger.error(f"Файл cookies не найден: {SESSION_FILE}")
        return session

    with open(cookie_file) as f:
        cookies_list = json.load(f)

    for c in cookies_list:
        session.cookies.set(
            c["name"],
            c["value"],
            domain=c.get("domain", ""),
        )

    logger.info(f"Загружено {len(cookies_list)} cookies")
    return session


def _extract_offer_id(offer_url: str) -> str:
    """Извлекает offer_id из URL Циан."""
    import re
    match = re.search(r'/(?:sale|rent)/[\w-]+/(\d+)', offer_url)
    return match.group(1) if match else ""


def send_message(offer_url: str, message_text: str) -> dict:
    """
    Отправляет сообщение собственнику через API Циан.

    Первое сообщение отправляется с analyticsKey для обхода капчи.
    Последующие — свободным текстом.

    Возвращает:
        {"success": True/False, "error": str|None}
    """
    result = {"success": False, "error": None}

    offer_id_str = _extract_offer_id(offer_url)
    if not offer_id_str:
        result["error"] = f"Не удалось извлечь offer_id из URL: {offer_url}"
        return result

    offer_id = int(offer_id_str)
    session = _get_session()

    # Пробуем отправить кастомный текст
    payload = {"offerId": offer_id, "text": message_text}
    try:
        resp = session.post(
            f"{API_BASE}/messages/offer/",
            json=payload,
            timeout=15,
        )
    except Exception as e:
        result["error"] = f"Ошибка сети: {e}"
        logger.error(result["error"])
        return result

    # Если needCaptcha — отправляем первое сообщение через hint
    if resp.status_code == 400:
        resp_data = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else {}
        if resp_data.get("message") == "needCaptcha":
            logger.info(f"Капча — отправляю первое сообщение через hint для offerId={offer_id}")
            return _send_first_via_hint(session, offer_id, message_text)

    if resp.status_code == 200:
        result["success"] = True
        logger.info(f"Сообщение отправлено через API: offerId={offer_id}")
        return result

    # Другие ошибки
    result["error"] = f"API ответил {resp.status_code}: {resp.text[:200]}"
    logger.error(result["error"])
    return result


def _send_first_via_hint(session: requests.Session, offer_id: int, message_text: str) -> dict:
    """
    Отправляет первое сообщение с analyticsKey для обхода капчи.
    Текст шаблона передаётся напрямую — лишних сообщений нет.
    """
    result = {"success": False, "error": None}

    payload = {
        "offerId": offer_id,
        "text": message_text,
        "analyticsKey": "whenSee",
    }

    try:
        resp = session.post(
            f"{API_BASE}/messages/offer/",
            json=payload,
            timeout=15,
        )
    except Exception as e:
        result["error"] = f"Ошибка hint-запроса: {e}"
        logger.error(result["error"])
        return result

    if resp.status_code != 200:
        result["error"] = f"Hint не прошёл: {resp.status_code} {resp.text[:200]}"
        logger.error(result["error"])
        return result

    logger.info(f"Сообщение отправлено через hint: offerId={offer_id}")
    result["success"] = True
    return result


def _get_browser_context():
    """Создаёт Playwright browser context с cookies и stealth."""
    import time
    from pathlib import Path
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    stealth = Stealth(
        navigator_languages_override=("ru-RU", "ru"),
        navigator_platform_override="Linux x86_64",
    )

    p = sync_playwright().start()
    context = p.chromium.launch_persistent_context(
        user_data_dir=str(Path("cian_storage")),
        headless=False,
        viewport={"width": 1280, "height": 800},
        locale="ru-RU",
        timezone_id="Europe/Moscow",
        user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
        args=[
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox", "--disable-infobars", "--disable-gpu",
        ],
        ignore_default_args=["--enable-automation"],
    )
    stealth.apply_stealth_sync(context)

    cookie_file = Path(SESSION_FILE)
    if cookie_file.exists():
        with open(cookie_file) as f:
            cookies = json.load(f)
        context.add_cookies(cookies)

    return p, context


def check_replies() -> list[dict]:
    """
    Проверяет непрочитанные сообщения через браузер.
    Запускает браузер в отдельном потоке, чтобы избежать конфликта
    sync_playwright с asyncio event loop из APScheduler.

    Возвращает:
        [{"offer_url": str, "reply_text": str, "sender": str}, ...]
    """
    import threading

    result_holder = []
    error_holder = []

    def _run_in_thread():
        try:
            with BROWSER_LOCK:
                result_holder.extend(_check_replies_impl())
        except Exception as e:
            error_holder.append(e)

    t = threading.Thread(target=_run_in_thread, daemon=True)
    t.start()
    t.join(timeout=300)  # макс 5 минут

    if error_holder:
        raise error_holder[0]

    return result_holder


def _check_replies_impl() -> list[dict]:
    """Внутренняя реализация проверки ответов (выполняется в чистом потоке)."""
    import re
    import time

    replies = []

    try:
        p, context = _get_browser_context()
    except Exception as e:
        logger.error(f"Не удалось запустить браузер: {e}")
        return replies

    try:
        page = context.new_page()
        page.goto(
            "https://www.cian.ru/dialogs/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        time.sleep(5)

        # Парсим список чатов — ищем span с username, поднимаемся к родителю
        chats_data = page.evaluate("""() => {
            const chats = [];
            const usernames = document.querySelectorAll('span[class*="username"]');

            for (const span of usernames) {
                const name = (span.innerText || '').trim();
                if (!name || name.length < 2 || name.length > 50) continue;
                if (name.includes('Поддержка') || name.includes('помощник')) continue;

                // Поднимаемся к родительскому item
                let item = span;
                for (let i = 0; i < 6; i++) {
                    item = item.parentElement;
                    if (!item) break;
                    if ((item.className || '').includes('item') && item.children.length > 0) break;
                }
                if (!item) continue;

                const text = (item.innerText || '').trim();
                const lines = text.split('\\n').map(l => l.trim()).filter(Boolean);

                // Badge непрочитанных — последняя строка если число
                const lastLine = lines[lines.length - 1];
                const unreadCount = /^\\d+$/.test(lastLine) ? parseInt(lastLine) : 0;

                // Превью
                const previewIdx = unreadCount > 0 ? lines.length - 2 : lines.length - 1;
                const preview = lines[previewIdx] || '';

                chats.push({
                    name: name,
                    preview: preview,
                    unread: unreadCount,
                });
            }
            return chats;
        }""")

        logger.info(f"Найдено чатов: {len(chats_data)}")

        # Дедупликация по имени
        seen_names = set()
        unique_chats = []
        for c in chats_data:
            if c["name"] not in seen_names:
                seen_names.add(c["name"])
                unique_chats.append(c)
        chats_data = unique_chats

        unread_chats = [c for c in chats_data if c["unread"] > 0]
        logger.info(f"Непрочитанных: {len(unread_chats)}")

        # Загружаем данные о ранее уведомлённых
        notified = _load_notified()

        if not unread_chats:
            context.close()
            p.stop()
            return replies

        # Для каждого непрочитанного — кликаем и читаем
        for chat in unread_chats:
            sender_name = chat["name"]
            logger.info(f"Читаю чат: {sender_name} ({chat['unread']} непрочитанных)")

            try:
                # Кликаем по имени в списке
                name_el = page.query_selector(f"text={sender_name}")
                if not name_el:
                    logger.warning(f"Не нашёл элемент для: {sender_name}")
                    continue

                name_el.click()
                time.sleep(4)

                # Извлекаем offer URL из ссылок в открытом чате
                offer_url = "unknown"
                all_links = page.query_selector_all("a")
                for link_el in all_links:
                    href = link_el.get_attribute("href") or ""
                    match = re.search(r'cian\.ru/((?:sale|rent)/[\w-]+/\d+)', href)
                    if match:
                        offer_url = f"https://www.cian.ru/{match.group(1)}/"
                        break

                # Читаем сообщения — ищем после "Непрочитанные сообщения"
                all_text = page.evaluate("document.body.innerText")
                lines = [l.strip() for l in all_text.split("\n") if l.strip()]

                # UI-лейблы Циана, которые не являются сообщениями
                ui_labels = {
                    "Объявление", "Написать сообщение", "Показать телефон",
                    "Пожаловаться", "Заблокировать", "Перейти в профиль",
                    "Добавить в избранное", "Поделиться", "Онлайн",
                    "Был(а) недавно", "Оффлайн",
                }

                # Находим блок непрочитанных
                unread_msgs = []
                found_unread_marker = False
                for line in lines:
                    if "Непрочитанные сообщения" in line:
                        found_unread_marker = True
                        continue
                    if found_unread_marker:
                        # Пропускаем служебные строки
                        if line in ("Вчера", "Сегодня") or re.match(r"^\d{2}:\d{2}$", line):
                            continue
                        if any(label in line for label in ui_labels):
                            continue
                        if "Риелтор" in line or re.match(r"^\+7", line):
                            break
                        if len(line) > 1:
                            unread_msgs.append(line)

                reply_text = "\n".join(unread_msgs) if unread_msgs else chat["preview"]

                # Дедупликация: сравниваем с реальным текстом ответа
                prev_data = notified.get(sender_name, {})
                prev_reply = prev_data if isinstance(prev_data, str) else prev_data.get("reply_text", "")
                if reply_text and reply_text == prev_reply:
                    logger.debug(f"Пропускаю {sender_name} — тот же текст ответа")
                    continue

                if reply_text:
                    replies.append({
                        "offer_url": offer_url,
                        "reply_text": reply_text[:500],
                        "sender": sender_name,
                    })
                    # Сохраняем реальный текст ответа для дедупликации
                    notified[sender_name] = {
                        "reply_text": reply_text[:500],
                        "offer_url": offer_url,
                    }
                    _save_notified(notified)
                    logger.info(f"Ответ от {sender_name}: {reply_text[:80]}")

            except Exception as e:
                logger.warning(f"Ошибка чтения чата {sender_name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Ошибка проверки ответов: {e}")

    finally:
        try:
            context.close()
            p.stop()
        except:
            pass

    return replies
