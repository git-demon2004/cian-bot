"""
Модуль отправки сообщений через API Циан.
Не требует браузера — работает через HTTP-запросы с cookies.
"""
import json
import logging
import os
import threading
import time
from pathlib import Path

import requests

import session_health

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

    # Auth-ошибка — сессия протухла
    if session_health.is_auth_error(resp.status_code, resp.text):
        result["auth_expired"] = True
        result["error"] = f"Сессия Циана протухла ({resp.status_code}): {resp.text[:200]}"
        logger.error(result["error"])
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
        if session_health.is_auth_error(resp.status_code, resp.text):
            result["auth_expired"] = True
            result["error"] = f"Сессия Циана протухла ({resp.status_code}): {resp.text[:200]}"
        else:
            result["error"] = f"Hint не прошёл: {resp.status_code} {resp.text[:200]}"
        logger.error(result["error"])
        return result

    logger.info(f"Сообщение отправлено через hint: offerId={offer_id}")
    result["success"] = True
    return result


def _cleanup_stale_singleton_locks() -> None:
    """
    Aggressive cleanup перед launch_persistent_context (cian_api).

    BROWSER_LOCK гарантирует что в нашем процессе только один поток держит
    chromium → любой найденный chromium с user_data_dir=cian_storage
    = осиротевший от прежнего креша/SIGKILL. SIGTERM → 3s → SIGKILL → unlink locks.

    Раньше функция пропускала cleanup при pgrep-match → 04-30 12:00 МСК cron
    упал 0/60 sent из-за orphan от ночного парсинга.
    """
    import os
    import signal
    import subprocess
    import time
    from pathlib import Path
    storage = Path("cian_storage")
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"user-data-dir={storage}"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(x) for x in result.stdout.split() if x.strip().isdigit()]
    except Exception as e:
        logger.warning(f"_cleanup (cian_api): pgrep err {e}")
        pids = []

    if pids:
        logger.warning(
            f"⚠️  CHROMIUM_ORPHAN_DETECTED (cian_api): {len(pids)} процесса "
            f"на {storage} (PIDs: {pids}) — kill"
        )
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        time.sleep(3)
        for pid in pids:
            try:
                os.kill(pid, 0)
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        time.sleep(1)

    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (storage / name).unlink(missing_ok=True)
        except Exception:
            pass
    try:
        for p in storage.glob(".org.chromium.*"):
            try:
                p.unlink()
            except Exception:
                pass
    except Exception:
        pass
    if pids:
        logger.info("✅ Cleanup завершён (cian_api)")


def _get_browser_context():
    """
    Создаёт Playwright browser context с cookies и stealth.

    Retry-логика: если launch_persistent_context падает (TargetClosedError
    "Opening in existing browser session" обычно от orphan chromium), второй
    проход cleanup + повторная попытка. На второй неудаче пробрасываем
    исключение и аккуратно останавливаем playwright (избегаем p-leak).
    """
    import time
    from pathlib import Path
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    stealth = Stealth(
        navigator_languages_override=("ru-RU", "ru"),
        navigator_platform_override="Linux x86_64",
    )

    launch_args = dict(
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

    last_err = None
    for attempt in range(2):
        _cleanup_stale_singleton_locks()
        p = sync_playwright().start()
        try:
            context = p.chromium.launch_persistent_context(**launch_args)
            stealth.apply_stealth_sync(context)
            cookie_file = Path(SESSION_FILE)
            if cookie_file.exists():
                with open(cookie_file) as f:
                    cookies = json.load(f)
                context.add_cookies(cookies)
            return p, context
        except Exception as e:
            last_err = e
            logger.warning(
                f"launch_persistent_context attempt {attempt + 1}/2 failed: {e}"
            )
            try:
                p.stop()
            except Exception:
                pass
            time.sleep(2)

    raise RuntimeError(f"_get_browser_context: launch failed after 2 attempts: {last_err}")


def refresh_session() -> dict:
    """
    Keepalive: открывает браузер с текущим профилем, заходит на /dialogs/,
    даёт Cian'у обновить DMIR_AUTH и пере-экспортирует cookies в SESSION_FILE.

    Работает в отдельном потоке + под BROWSER_LOCK, чтобы не конфликтовать
    с check_replies и sync_playwright/asyncio.

    Возвращает:
        {"success": bool, "auth_expired": bool, "error": str|None,
         "cookies_count": int}
    """
    result = {
        "success": False,
        "auth_expired": False,
        "error": None,
        "cookies_count": 0,
    }

    holder: dict = {}

    def _run_in_thread():
        try:
            with BROWSER_LOCK:
                holder.update(_refresh_session_impl())
        except Exception as e:  # noqa: BLE001
            holder["error"] = f"Исключение: {e}"

    t = threading.Thread(target=_run_in_thread, daemon=True)
    t.start()
    t.join(timeout=120)

    if t.is_alive():
        result["error"] = "Keepalive: таймаут 120 сек"
        logger.error(result["error"])
        return result

    result.update(holder)
    return result


def _refresh_session_impl() -> dict:
    """Внутренняя реализация keepalive (выполняется в отдельном потоке)."""
    out = {
        "success": False,
        "auth_expired": False,
        "error": None,
        "cookies_count": 0,
    }

    try:
        p, context = _get_browser_context()
    except Exception as e:  # noqa: BLE001
        out["error"] = f"Не удалось запустить браузер: {e}"
        logger.error(out["error"])
        return out

    try:
        page = context.new_page()
        page.goto(
            "https://www.cian.ru/dialogs/",
            wait_until="domcontentloaded",
            timeout=30000,
        )
        # Дадим JS отработать и серверу обновить DMIR_AUTH
        time.sleep(8)

        current_url = page.url or ""
        # Если Циан редиректнул на authenticate — значит сессия мертва,
        # keepalive не помогает, нужен ручной релогин
        if "authenticate" in current_url or "login" in current_url:
            out["auth_expired"] = True
            out["error"] = f"Редирект на логин: {current_url}"
            logger.error(out["error"])
            return out

        cookies = context.cookies()
        if not cookies:
            out["error"] = "Cookies пустые после keepalive"
            logger.error(out["error"])
            return out

        # Пере-экспортируем — оставляем тот же формат, что login_cian.py
        Path(SESSION_FILE).write_text(
            json.dumps(cookies, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        out["success"] = True
        out["cookies_count"] = len(cookies)
        logger.info(f"Keepalive: обновлено {len(cookies)} cookies")
        return out

    except Exception as e:  # noqa: BLE001
        out["error"] = f"Ошибка keepalive: {e}"
        logger.error(out["error"])
        return out

    finally:
        try:
            context.close()
            p.stop()
        except Exception:  # noqa: BLE001
            pass


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
