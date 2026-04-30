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


def _cleanup_stale_singleton_locks() -> None:
    """
    Aggressive cleanup перед launch_persistent_context.

    На этом VPS только cian-bot пользуется data_dir cian_storage. BROWSER_LOCK
    в cian_api.BROWSER_LOCK гарантирует что внутри нашего Python-процесса
    единовременно только один поток держит chromium. Поэтому ЛЮБОЙ найденный
    chromium с нашей user_data_dir = осиротевший от прежнего креша/SIGKILL/
    upstream-падения, блокирующий новый launch с TargetClosedError "Opening
    in existing browser session".

    Логика:
      1. pgrep на user-data-dir=cian_storage
      2. Если есть PID-ы — SIGTERM, ждём 3 сек, SIGKILL
      3. Удаляем SingletonLock/SingletonCookie/SingletonSocket
      4. Доп. удаляем .org.chromium.* (мусор от прерванной сессии)

    Раньше функция пропускала cleanup при pgrep-match и Browser-collision
    (item #16) проявлялся как 0/60 sent на cron 12:00 МСК 2026-04-30.
    """
    import signal
    import subprocess
    try:
        result = subprocess.run(
            ["pgrep", "-f", f"user-data-dir={SESSION_DIR}"],
            capture_output=True, text=True, timeout=5,
        )
        pids = [int(x) for x in result.stdout.split() if x.strip().isdigit()]
    except Exception as e:
        logger.warning(f"_cleanup: pgrep error {e}, продолжаем")
        pids = []

    if pids:
        logger.warning(
            f"⚠️  CHROMIUM_ORPHAN_DETECTED: найдено {len(pids)} chromium-процесса "
            f"на {SESSION_DIR} (PIDs: {pids}) — убиваю перед launch"
        )
        # SIGTERM первым шагом
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
        time.sleep(3)
        # SIGKILL для тех что выжили
        for pid in pids:
            try:
                os.kill(pid, 0)  # check alive
                os.kill(pid, signal.SIGKILL)
                logger.warning(f"  SIGKILL для PID {pid}")
            except (ProcessLookupError, PermissionError):
                pass
        time.sleep(1)

    # Удаляем lock-файлы (могут остаться даже после KILL)
    for name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        try:
            (SESSION_DIR / name).unlink(missing_ok=True)
        except Exception:
            pass
    # Дополнительный мусор от прерванных сессий
    try:
        for p in SESSION_DIR.glob(".org.chromium.*"):
            try:
                p.unlink()
            except Exception:
                pass
    except Exception:
        pass
    if pids:
        logger.info(f"✅ Cleanup завершён, готов к launch_persistent_context")


def _get_browser_context(playwright) -> BrowserContext:
    """
    Создаёт browser context с сохранённой сессией и антидетектом.

    Retry-логика: если launch_persistent_context падает (TargetClosedError
    обычно от orphan chromium), делает второй проход cleanup + повтор.
    """
    launch_args = dict(
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

    last_err = None
    for attempt in range(2):
        _cleanup_stale_singleton_locks()
        try:
            context = playwright.chromium.launch_persistent_context(**launch_args)
            break
        except Exception as e:
            last_err = e
            logger.warning(
                f"launch_persistent_context (cian_browser) attempt {attempt + 1}/2 failed: {e}"
            )
            time.sleep(2)
    else:
        raise RuntimeError(
            f"_get_browser_context: launch failed after 2 attempts: {last_err}"
        )

    stealth = Stealth(
        navigator_languages_override=("ru-RU", "ru"),
        navigator_platform_override="Linux x86_64",
    )
    stealth.apply_stealth_sync(context)

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

        # Извлекаем sitekey из страницы — с retry, т.к. iframe SmartCaptcha
        # может рендериться с задержкой после первоначального _has_captcha=True.
        sitekey_js = '''() => {
            const el = document.querySelector('[data-sitekey]');
            if (el) return el.getAttribute('data-sitekey');
            const iframe = document.querySelector('iframe[src*="smartcaptcha"], iframe[src*="captcha"]');
            if (iframe) {
                try {
                    const url = new URL(iframe.src);
                    return url.searchParams.get('sitekey') || '';
                } catch(e) {}
            }
            const scripts = document.querySelectorAll('script');
            for (const s of scripts) {
                const match = s.textContent?.match(/sitekey['"\\s:]+['"]([^'"]+)['"]/);
                if (match) return match[1];
            }
            return '';
        }'''

        sitekey = page.evaluate(sitekey_js)
        if not sitekey:
            logger.warning("⚠️  CAPTCHA_SITEKEY_RETRY: sitekey не найден сразу, жду до 5с")
            for retry in range(5):
                time.sleep(1)
                sitekey = page.evaluate(sitekey_js)
                if sitekey:
                    logger.info(f"sitekey найден после retry {retry + 1}")
                    break

        if not sitekey:
            # КРИТИЧНО: solver.normal() для interactive Yandex SmartCaptcha
            # НЕ РАБОТАЕТ — image OCR не может пройти behavioral check.
            # Раньше тут был silent false-success → клиент получал
            # out-of-context сообщение (incident 2026-04-30, offerId 328356333).
            # Возвращаем False — пусть post-Enter verification заблокирует send.
            logger.error("⚠️  CAPTCHA_NO_SITEKEY: sitekey не найден за 5с — fail (отправка будет заблокирована)")
            try:
                page.screenshot(path=f"debug_no_sitekey_{int(time.time())}.png", timeout=5000)
            except Exception:
                pass
            return False

        logger.info(f"Решаю SmartCaptcha через 2captcha (sitekey: {sitekey[:20]}...)")
        # twocaptcha-python 1.5.0+ не имеет метода .yandex(), используем
        # generic .solve(method="yandex", ...). Параметр URL — pageurl.
        result = solver.solve(
            method="yandex",
            sitekey=sitekey,
            pageurl=page.url,
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

    Telemetry-маркеры (для grep): CAPTCHA_ENCOUNTERED, CAPTCHA_SOLVED_CLICK,
    CAPTCHA_SOLVED_2CAPTCHA, CAPTCHA_FAILED, CAPTCHA_NO_SITEKEY.
    """
    if not _has_captcha(page):
        return True

    logger.warning(f"⚠️  CAPTCHA_ENCOUNTERED: url={page.url[:80]}")

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
                    logger.info("✅ CAPTCHA_SOLVED_CLICK: пройдена кликом по 'Я не робот'")
                    return True

            for frame in page.frames:
                try:
                    checkbox = frame.query_selector('input[type="checkbox"]')
                    if checkbox and checkbox.is_visible():
                        _human_delay(1, 3)
                        checkbox.click()
                        _human_delay(4, 6)
                        if not _has_captcha(page):
                            logger.info("✅ CAPTCHA_SOLVED_CLICK: пройдена iframe checkbox")
                            return True
                except:
                    continue

            # Перезагрузка
            page.reload(wait_until="domcontentloaded", timeout=20000)
            _human_delay(3, 5)
            if not _has_captcha(page):
                logger.info("✅ CAPTCHA_SOLVED_CLICK: исчезла после перезагрузки")
                return True

        except Exception as e:
            logger.warning(f"Ошибка клика: {e}")

    # Попытка 2: решение через 2captcha
    if os.getenv("TWOCAPTCHA_API_KEY"):
        logger.info("Кликом не прошла, отправляю в 2captcha")
        if _solve_captcha_2captcha(page):
            logger.info("✅ CAPTCHA_SOLVED_2CAPTCHA: пройдена через сервис")
            return True

    logger.error("❌ CAPTCHA_FAILED: не удалось пройти капчу (ни кликом, ни через 2captcha)")
    try:
        page.screenshot(path=f"debug_captcha_{int(time.time())}.png", timeout=5000)
    except Exception:
        pass
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


def _type_message_with_newlines(textarea, message_text: str) -> None:
    """Печатает текст в textarea с человеческой скоростью.

    В чате Циана голый Enter отправляет сообщение, поэтому переносы строк
    вставляем через Shift+Enter — это добавляет литеральный \\n в поле
    без отправки. Финальный Enter (отправку) делает вызывающий код.
    """
    lines = message_text.split("\n")
    for line_idx, line in enumerate(lines):
        if line_idx > 0:
            textarea.press("Shift+Enter")
        for char in line:
            textarea.type(char, delay=random.randint(30, 80))


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

            # 4. Вводим текст. См. _type_message_with_newlines: \n → Shift+Enter,
            # чтобы перенос строки не сработал как отправка.
            textarea.click()
            _human_delay(0.5, 1.0)

            _type_message_with_newlines(textarea, message_text)

            _human_delay(1, 2)

            # 5. Финальный Enter — отправка целого сообщения одним блоком
            textarea.press("Enter")
            _human_delay(3, 5)

            # 6. Post-submit verification — убеждаемся что отправка реально
            # произошла, а не была проглочена капчей или anti-bot. Несколько
            # независимых сигналов:
            verify_ok = True
            verify_reason = ""

            # 6a. Если после Enter снова появилась капча → submit перехвачен
            try:
                if _has_captcha(page):
                    verify_ok = False
                    verify_reason = "после Enter появилась капча"
            except Exception as e:
                logger.warning(f"verify captcha check err: {e}")

            # 6b. textarea должен очиститься после успешного submit
            if verify_ok:
                try:
                    ta_value = textarea.input_value() if textarea else ""
                    if ta_value and ta_value.strip():
                        verify_ok = False
                        verify_reason = f"textarea не очистился (осталось {len(ta_value)} символов)"
                except Exception as e:
                    logger.warning(f"verify textarea check err: {e}")

            # 6c. Текст нашего сообщения должен появиться в DOM
            # (приходящее сообщение рендерится как пузырь)
            if verify_ok:
                first_line = (message_text.split("\n", 1)[0] or "").strip()
                if first_line:
                    probe = first_line[:60]
                    try:
                        found = page.evaluate(
                            "(text) => Array.from(document.querySelectorAll('div, span, p')).some(el => (el.innerText || '').includes(text))",
                            probe,
                        )
                        if not found:
                            verify_ok = False
                            verify_reason = "текст сообщения не найден в DOM после Enter"
                    except Exception as e:
                        logger.warning(f"verify DOM-text check err: {e}")

            # 7. Скриншот для архива (полезно при разборе fail-кейсов)
            try:
                page.screenshot(path=f"debug_sent_{offer_id}.png", timeout=10000)
            except Exception:
                pass

            if verify_ok:
                result["success"] = True
                logger.info(f"Сообщение отправлено: offerId={offer_id}")
            else:
                result["success"] = False
                result["error"] = f"submit_verification_failed: {verify_reason}"
                logger.error(f"⚠️  Отправка НЕ подтверждена: offerId={offer_id} — {verify_reason}")

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
