"""
Модуль автоматизации Циан через Playwright.
Отправка сообщений и проверка ответов.
"""
import json
import logging
import os
import random
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, Page, BrowserContext
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

SESSION_DIR = Path("cian_storage")
SESSION_FILE = os.getenv("CIAN_SESSION_FILE", "cian_session.json")


def _human_delay(min_sec=1.0, max_sec=3.0):
    """Рандомная задержка, имитация человека."""
    time.sleep(random.uniform(min_sec, max_sec))


def _get_browser_context(playwright) -> BrowserContext:
    """Создаёт browser context с сохранённой сессией и антидетектом."""
    context = playwright.chromium.launch_persistent_context(
        user_data_dir=str(SESSION_DIR),
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
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-infobars",
            "--window-size=1280,800",
        ],
        ignore_default_args=["--enable-automation"],
    )

    # Применяем stealth к контексту
    stealth = Stealth(
        navigator_languages_override=("ru-RU", "ru"),
        navigator_platform_override="Linux x86_64",
    )
    stealth.apply_stealth_sync(context)

    # Подгружаем cookies из файла если есть (бэкап)
    if Path(SESSION_FILE).exists():
        try:
            with open(SESSION_FILE) as f:
                cookies = json.load(f)
            context.add_cookies(cookies)
            logger.info(f"Загружено {len(cookies)} cookies из {SESSION_FILE}")
        except Exception as e:
            logger.warning(f"Не удалось загрузить cookies: {e}")

    return context


def _apply_stealth(page: Page):
    """Применяет stealth и патчит navigator.webdriver."""
    stealth = Stealth(
        navigator_languages_override=("ru-RU", "ru"),
        navigator_platform_override="Linux x86_64",
    )
    stealth.apply_stealth_sync(page)


def _has_captcha(page: Page) -> bool:
    """Проверяет наличие капчи — URL, inline, и внутри iframes."""
    if "cian-captcha" in page.url or "captcha" in page.url.lower():
        return True
    # Inline капча на странице
    captcha_el = page.query_selector('text=Я не робот')
    if captcha_el and captcha_el.is_visible():
        return True
    # Капча в iframe (SmartCaptcha)
    checkbox = page.query_selector('input[type="checkbox"]')
    if checkbox and checkbox.is_visible():
        return True
    # Контейнер SmartCaptcha
    container = page.query_selector('[class*="captcha" i], [class*="Captcha"]')
    if container and container.is_visible():
        return True
    return False


def _solve_captcha_2captcha(page: Page) -> bool:
    """Решает Yandex SmartCaptcha через сервис 2captcha."""
    api_key = os.getenv("TWOCAPTCHA_API_KEY", "")
    if not api_key:
        logger.error("TWOCAPTCHA_API_KEY не задан в .env")
        return False

    try:
        from twocaptcha import TwoCaptcha
        solver = TwoCaptcha(api_key)

        # Извлекаем sitekey из страницы
        sitekey = page.evaluate('''() => {
            // Ищем sitekey в атрибутах или скриптах
            const el = document.querySelector('[data-sitekey]');
            if (el) return el.getAttribute('data-sitekey');
            // Ищем в iframe src
            const iframe = document.querySelector('iframe[src*="smartcaptcha"]');
            if (iframe) {
                const url = new URL(iframe.src);
                return url.searchParams.get('sitekey') || '';
            }
            // Ищем в скриптах
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const match = s.textContent?.match(/sitekey['"\\s:]+['"]([^'"]+)['"]/);
                if (match) return match[1];
            }
            return '';
        }''')

        if not sitekey:
            # Для SmartCaptcha — используем sitekey по умолчанию
            # или пробуем извлечь из URL
            logger.warning("sitekey не найден, пробую стандартный подход")
            # Делаем скриншот и отправляем как обычную капчу
            page.screenshot(path="captcha_screenshot.png")
            result = solver.normal("captcha_screenshot.png")
            logger.info(f"2captcha решил капчу (normal): {result['code'][:20]}...")
            return True

        logger.info(f"Решаю SmartCaptcha через 2captcha (sitekey: {sitekey[:20]}...)")
        result = solver.yandex(
            sitekey=sitekey,
            url=page.url,
        )
        token = result["code"]
        logger.info(f"2captcha решил капчу, токен: {token[:30]}...")

        # Вставляем токен в форму
        page.evaluate(f'''(token) => {{
            // Ищем hidden input для ответа капчи
            const inputs = document.querySelectorAll('input[name*="captcha"], input[name*="smart-token"], textarea[name*="captcha"]');
            inputs.forEach(i => {{ i.value = token; }});
            // Пробуем callback
            if (window.smartCaptcha) {{
                window.smartCaptcha.execute();
            }}
            // Пробуем сабмит формы
            const form = document.querySelector('form');
            if (form) form.submit();
        }}''', token)

        _human_delay(3, 5)
        return not _has_captcha(page)

    except Exception as e:
        logger.error(f"Ошибка 2captcha: {e}")
        return False


def _handle_captcha(page: Page) -> bool:
    """
    Обнаруживает и пробует пройти капчу Yandex SmartCaptcha.
    Сначала пробует кликнуть (бесплатно), потом 2captcha (платно).
    Возвращает True если капча пройдена или её не было.
    """
    if not _has_captcha(page):
        return True

    # Попытка 1: клик по чекбоксу (бесплатно)
    for attempt in range(2):
        logger.warning(f"Капча обнаружена, пробую кликнуть (попытка {attempt + 1}/2)")
        try:
            captcha_el = page.query_selector('text=Я не робот')
            if captcha_el and captcha_el.is_visible():
                _human_delay(1, 3)
                captcha_el.click()
                _human_delay(4, 6)
                if not _has_captcha(page):
                    logger.info("Капча пройдена кликом")
                    return True

            for frame in page.frames:
                try:
                    checkbox = frame.query_selector('input[type="checkbox"]')
                    if checkbox and checkbox.is_visible():
                        _human_delay(1, 3)
                        checkbox.click()
                        _human_delay(4, 6)
                        if not _has_captcha(page):
                            logger.info("Капча пройдена через iframe checkbox")
                            return True
                except:
                    continue

            # Перезагрузка
            page.reload(wait_until="domcontentloaded", timeout=20000)
            _human_delay(3, 5)
            if not _has_captcha(page):
                logger.info("Капча исчезла после перезагрузки")
                return True

        except Exception as e:
            logger.warning(f"Ошибка клика: {e}")

    # Попытка 2: решение через 2captcha
    if os.getenv("TWOCAPTCHA_API_KEY"):
        logger.info("Кликом не прошла, отправляю в 2captcha")
        if _solve_captcha_2captcha(page):
            return True

    logger.error("Не удалось пройти капчу")
    page.screenshot(path=f"debug_captcha_{int(time.time())}.png")
    return False


def _is_logged_in(page: Page) -> bool:
    """Проверяет, залогинены ли мы на Циан."""
    try:
        _apply_stealth(page)
        page.goto("https://www.cian.ru/dialogs/", wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(3000)

        # Обрабатываем капчу если появилась
        if not _handle_captcha(page):
            logger.error("Капча не пройдена при проверке логина")
            return False

        # Если есть форма логина — не залогинены
        login_prompt = page.query_selector("text=Войдите, чтобы писать сообщения")
        if login_prompt:
            return False
        # Если есть список диалогов или пустой чат — залогинены
        return True
    except Exception as e:
        logger.error(f"Ошибка проверки логина: {e}")
        return False


def _extract_offer_id(offer_url: str) -> str:
    """Извлекает offer_id из URL Циан."""
    import re
    match = re.search(r'/(?:sale|rent)/[\w-]+/(\d+)', offer_url)
    return match.group(1) if match else ""


def send_message(offer_url: str, message_text: str) -> dict:
    """
    Отправляет сообщение собственнику по ссылке на объявление.
    Использует прямую навигацию в диалоговый URL — работает стабильнее
    чем клик по кнопке на странице объявления.

    Возвращает:
        {"success": True/False, "error": str|None}
    """
    result = {"success": False, "error": None}

    offer_id = _extract_offer_id(offer_url)
    if not offer_id:
        result["error"] = f"Не удалось извлечь offer_id из URL: {offer_url}"
        return result

    with sync_playwright() as p:
        context = _get_browser_context(p)
        page = context.new_page()

        try:
            _apply_stealth(page)

            # 1. Проверяем логин
            page.goto("https://www.cian.ru/", wait_until="domcontentloaded", timeout=30000)
            _human_delay(2, 3)

            if not _handle_captcha(page):
                result["error"] = "Капча на главной не пройдена"
                return result

            login_btn = page.query_selector('text=Войти')
            if login_btn and login_btn.is_visible():
                result["auth_expired"] = True
                result["error"] = "Не залогинены в Циан. Запусти login_cian.py"
                logger.error(result["error"])
                return result

            # 2. Переходим напрямую в диалог с объявлением
            deal_type = "rent" if "/rent/" in offer_url else "sale"
            dialog_url = (
                f"https://www.cian.ru/dialogs/"
                f"?hostType=frame&offerId={offer_id}"
                f"&dealType={deal_type}&offerType=flat"
            )
            logger.info(f"Открываю чат: offerId={offer_id}")
            page.goto(dialog_url, wait_until="domcontentloaded", timeout=30000)
            _human_delay(5, 8)

            # 3. Проверяем и проходим капчу если есть
            if not _handle_captcha(page):
                result["error"] = "Капча в чате не пройдена"
                logger.error(result["error"])
                page.screenshot(path=f"debug_captcha_fail_{offer_id}.png")
                return result

            # 4. Ищем textarea
            textarea = None
            textarea_selectors = [
                'textarea[placeholder*="Написать сообщение"]',
                'textarea[placeholder*="сообщение"]',
                'textarea',
            ]

            for sel in textarea_selectors:
                try:
                    el = page.wait_for_selector(sel, timeout=10000)
                    if el and el.is_visible():
                        textarea = el
                        logger.info(f"Нашёл поле ввода: {sel}")
                        break
                except:
                    continue

            if not textarea:
                result["error"] = "Поле ввода не найдено в чате"
                logger.error(result["error"])
                page.screenshot(path=f"debug_no_textarea_{int(time.time())}.png")
                return result

            # 4. Вводим текст с человеческой скоростью
            textarea.click()
            _human_delay(0.5, 1.0)

            for char in message_text:
                textarea.type(char, delay=random.randint(30, 80))

            _human_delay(1, 2)

            # 5. Отправляем Enter
            textarea.press("Enter")
            _human_delay(3, 5)

            # 6. Проверяем что сообщение появилось в чате
            try:
                page.screenshot(path=f"debug_sent_{offer_id}.png", timeout=10000)
            except Exception:
                pass  # Скриншот не критичен
            result["success"] = True
            logger.info(f"Сообщение отправлено: offerId={offer_id}")

        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Ошибка отправки: {e}")
            try:
                page.screenshot(path=f"debug_error_{int(time.time())}.png")
            except:
                pass

        finally:
            context.close()

    return result


def check_replies() -> list[dict]:
    """
    Проверяет входящие сообщения на Циан.

    Возвращает список:
        [{"offer_url": str, "reply_text": str, "sender": str}, ...]
    """
    replies = []

    with sync_playwright() as p:
        context = _get_browser_context(p)
        page = context.new_page()

        try:
            if not _is_logged_in(page):
                logger.error("Не залогинены, пропускаю проверку ответов")
                return replies

            # Переходим в диалоги
            page.goto("https://www.cian.ru/dialogs/", wait_until="domcontentloaded", timeout=15000)
            _human_delay(3, 5)

            # Ищем непрочитанные диалоги
            # Циан помечает непрочитанные — ищем индикаторы
            unread_selectors = [
                '[class*="unread"]',
                '[class*="Unread"]',
                '[data-name="UnreadBadge"]',
                '.new-message-indicator',
            ]

            unread_dialogs = []
            for sel in unread_selectors:
                elements = page.query_selector_all(sel)
                if elements:
                    unread_dialogs = elements
                    logger.info(f"Найдено {len(elements)} непрочитанных диалогов")
                    break

            # Если нашли непрочитанные — кликаем по каждому и читаем
            for dialog_el in unread_dialogs[:10]:  # Максимум 10 за раз
                try:
                    dialog_el.click()
                    _human_delay(2, 3)

                    # Пытаемся извлечь ссылку на объявление из диалога
                    offer_link = page.query_selector('a[href*="/sale/"], a[href*="/rent/"]')
                    offer_url = offer_link.get_attribute("href") if offer_link else "unknown"

                    # Читаем последнее сообщение
                    messages = page.query_selector_all('[class*="message"], [data-name="Message"]')
                    last_msg = messages[-1].inner_text() if messages else "Новое сообщение"

                    replies.append({
                        "offer_url": offer_url,
                        "reply_text": last_msg[:200],  # Обрезаем для Telegram
                        "sender": "собственник",
                    })

                except Exception as e:
                    logger.warning(f"Ошибка чтения диалога: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка проверки ответов: {e}")

        finally:
            # Сохраняем обновлённые cookies
            try:
                cookies = context.cookies()
                with open(SESSION_FILE, "w") as f:
                    json.dump(cookies, f, ensure_ascii=False, indent=2)
            except:
                pass
            context.close()

    return replies
