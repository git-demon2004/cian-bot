"""
Главный файл — запускает планировщик.

Задачи:
1. Каждый день в 12:00 — отправка сообщений по расписанию
2. Каждый час — проверка ответов от собственников
3. Каждый день в 20:00 — дневная сводка в Telegram
"""
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Загружаем .env до импорта модулей
load_dotenv()

LAST_SEND_MARKER = Path("/root/cian-bot/.last_send_attempted")
LOG_PATH_FOR_CATCHUP = Path("/root/cian-bot/cian_bot.log")

import sheets
import cian_api
import cian_browser
import session_health
import telegram_notify
import telegram_bot

SESSION_FILE = os.getenv("CIAN_SESSION_FILE", "cian_session.json")

# SEND_VIA=browser — отправка через Playwright (надёжнее при усилении анти-бота Циана)
# SEND_VIA=api     — отправка через HTTP API (быстрее, но требует стабильного API)
_SEND_VIA = os.getenv("SEND_VIA", "browser").strip().lower()
_send_impl = cian_browser if _SEND_VIA == "browser" else cian_api

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("cian_bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


def load_messages() -> list[str]:
    """Загружает шаблоны сообщений из файла."""
    msg_file = Path("messages.txt")
    if not msg_file.exists():
        logger.error("Файл messages.txt не найден!")
        return ["Здравствуйте! Интересует ваша квартира. Она ещё актуальна?"]

    text = msg_file.read_text(encoding="utf-8")
    messages = [m.strip() for m in text.split("---") if m.strip()]
    logger.info(f"Загружено {len(messages)} шаблонов сообщений")
    return messages


def _record_send_attempt():
    """Помечает что сегодня окно отправки уже отработано (для catch-up на рестарте)."""
    try:
        LAST_SEND_MARKER.write_text(datetime.now().strftime("%Y-%m-%d"))
    except Exception as e:
        logger.warning(f"Не удалось обновить {LAST_SEND_MARKER}: {e}")


def _today_send_already_done() -> bool:
    """
    True только если сегодня task_send_messages **успешно завершилась**.

    Маркер обновляется в `_record_send_attempt` ПОСЛЕ цикла отправки.
    Fallback — ищем в логе строку `Итого: отправлено N, ошибок M` за сегодня.
    Просто старт `🚀 Запуск отправки` НЕ считается success — задача могла
    крашнуться (например, TargetClosedError на 1-й итерации). В таком случае
    catch-up должен сработать.
    """
    today = datetime.now().strftime("%Y-%m-%d")
    if LAST_SEND_MARKER.exists():
        try:
            if LAST_SEND_MARKER.read_text().strip() == today:
                return True
        except Exception:
            pass
    if LOG_PATH_FOR_CATCHUP.exists():
        try:
            with LOG_PATH_FOR_CATCHUP.open("rb") as f:
                f.seek(0, 2)
                size = f.tell()
                f.seek(max(0, size - 200_000))
                tail = f.read().decode("utf-8", errors="ignore")
            for line in tail.splitlines():
                if line.startswith(today) and "Итого: отправлено" in line:
                    return True
        except Exception:
            pass
    return False


def task_send_messages():
    """Задача: отправка сообщений."""
    logger.info("=" * 40)
    logger.info("🚀 Запуск отправки сообщений")

    messages = load_messages()
    days_between = int(os.getenv("DAYS_BETWEEN_MESSAGES", 3))

    try:
        sheets.apply_stop_list()
    except Exception as e:
        logger.error(f"Ошибка обработки листа Стоп: {e}")

    try:
        pending = sheets.get_pending_sends(days_between)
    except Exception as e:
        logger.error(f"Ошибка чтения Google Sheets: {e}")
        telegram_notify.send_to_general(f"Ошибка чтения таблицы: {e}")
        return

    if not pending:
        logger.info("Нет объявлений для отправки")
        return

    sent_count = 0
    errors_count = 0

    for item in pending:
        url = item["url"]
        msg_index = item["sent_count"]  # 0-based: первое сообщение = индекс 0

        # Выбираем текст сообщения (циклически если шаблонов < 20)
        msg_text = messages[msg_index % len(messages)]

        logger.info(f"Отправляю сообщение #{msg_index + 1} → {url}")

        # Отправляем через выбранный транспорт (см. SEND_VIA в .env)
        result = _send_impl.send_message(url, msg_text)

        topic_id = item.get("topic_id")

        if result["success"]:
            new_count = msg_index + 1
            # mark_sent имеет 3-attempt retry внутри; если упадёт всё равно —
            # отправка уже прошла на Циан, поэтому батч не останавливаем
            # (alert уже отправлен из mark_sent в TG general).
            try:
                sheets.mark_sent(item["row"], new_count, days_between)
            except Exception as e:
                logger.error(
                    f"mark_sent crashed для row={item['row']}, "
                    f"send УЖЕ прошёл на Циан, продолжаю батч: {e}"
                )
            sent_count += 1
            if topic_id:
                try:
                    telegram_notify.notify_send_result(topic_id, url, new_count, True)
                except Exception as e:
                    logger.warning(f"notify_send_result fail topic={topic_id}: {e}")
        else:
            errors_count += 1
            if topic_id:
                telegram_notify.notify_send_result(topic_id, url, msg_index + 1, False, result["error"])

            # Если протухла сессия — нет смысла дальше слать, все упадут
            if result.get("auth_expired"):
                logger.error("Сессия Циана протухла — прерываю рассылку")
                if session_health.should_send_alert("auth_expired_runtime"):
                    telegram_notify.notify_session_expired(result.get("error", ""))
                break

        # Пауза между отправками — 30-90 секунд (антиспам)
        if pending.index(item) < len(pending) - 1:
            delay = random.randint(30, 90)
            logger.info(f"Пауза {delay} сек перед следующей отправкой...")
            time.sleep(delay)

    logger.info(f"Итого: отправлено {sent_count}, ошибок {errors_count}")
    _record_send_attempt()


def task_check_replies():
    """Задача: проверка ответов."""
    logger.info("📬 Проверяю ответы от собственников...")

    try:
        replies = cian_api.check_replies()
    except Exception as e:
        logger.error(f"Ошибка проверки ответов: {e}")
        return

    if not replies:
        logger.info("Новых ответов нет")
        return

    for reply in replies:
        offer_url = reply["offer_url"]
        reply_text = reply["reply_text"]
        sender = reply.get("sender", "собственник")

        logger.info(f"📩 Ответ от {sender}: {offer_url}")

        # Если URL не удалось извлечь — логируем, но не спамим в группу
        if offer_url == "unknown":
            logger.warning(f"Не удалось определить объявление для ответа от {sender}: {reply_text[:80]}")
            continue

        # Ставим на паузу в таблице
        try:
            sheets.mark_replied(offer_url, reply_text)
        except Exception as e:
            logger.error(f"Ошибка обновления таблицы: {e}")

        # Уведомляем в тему собственника
        topic_id = sheets.get_topic_id(offer_url)
        if topic_id:
            telegram_notify.notify_reply(topic_id, offer_url, reply_text)
        else:
            # Если тема не найдена — шлём в General
            telegram_notify.send_to_general(
                f"<b>Ответ от {sender}</b>\n\n"
                f"🏠 <a href=\"{offer_url}\">{offer_url}</a>\n\n"
                f"💬 {reply_text}"
            )


def task_process_collections():
    """Задача: парсинг подборок → База, затем промоут новых URL → Рассылка."""
    logger.info("📋 Проверяю лист Подборки...")
    try:
        sheets.process_collections()
    except Exception as e:
        logger.error(f"Ошибка обработки подборок: {e}")

    # Bridge: новые URL из Базы → в Рассылку (колонка A).
    # get_pending_sends при следующем запуске инициализирует строки sent_count=0,
    # next_send=today, status=active.
    try:
        result = sheets.promote_base_to_rassylka()
        if result.get("added", 0) > 0:
            try:
                telegram_notify.send_to_general(
                    f"🆕 В Рассылку добавлено {result['added']} новых объявлений из Базы"
                )
            except Exception as notify_err:
                logger.warning(f"Не удалось отправить уведомление о промоуте: {notify_err}")
    except Exception as e:
        logger.error(f"Ошибка promote_base_to_rassylka: {e}")

    # Heal: добиваем topic_id для active-строк, у которых он пустой
    # (исторический баг: create_topic мог вернуть None, get_pending_sends
    # больше не пытается). Лимит/паузы внутри ensure_topics.
    try:
        sheets.ensure_topics(pause_sec=5.0, max_per_run=30)
    except Exception as e:
        logger.error(f"Ошибка ensure_topics: {e}")


def task_check_session_health():
    """Задача: проверка срока жизни auth-cookie, алерт если осталось мало."""
    status = session_health.read_session_status(SESSION_FILE)

    if not status.cookie_found:
        logger.warning("Auth-cookie DMIR_AUTH не найдена в session-файле")
        if session_health.should_send_alert("auth_cookie_missing"):
            telegram_notify.notify_session_expired(
                "В cian_session.json нет cookie DMIR_AUTH — нужен релогин."
            )
        return

    if status.is_expired:
        logger.error(f"Cookie DMIR_AUTH истекла ({status.expires_at})")
        if session_health.should_send_alert("auth_expired_scheduled"):
            telegram_notify.notify_session_expired(
                f"DMIR_AUTH истекла {status.expires_at:%Y-%m-%d %H:%M UTC}."
            )
        return

    if status.needs_warning:
        logger.warning(f"Сессия Циана истекает через {status.days_left:.1f} дн.")
        # Алерты — не чаще раза в сутки, отдельный ключ на каждый «день остатка»
        key = f"session_warn_{int(status.days_left)}"
        if session_health.should_send_alert(key):
            telegram_notify.notify_session_expiring(status.days_left)
    else:
        logger.info(
            f"Сессия Циана жива: осталось ~{status.days_left:.1f} дн. "
            f"до {status.expires_at:%Y-%m-%d %H:%M UTC}"
        )


def task_keepalive_session():
    """Задача: keepalive — зайти на /dialogs/ и продлить DMIR_AUTH."""
    logger.info("🔄 Keepalive: обновляю сессию Циана...")
    try:
        result = cian_api.refresh_session()
    except Exception as e:  # noqa: BLE001
        logger.error(f"Keepalive упал с исключением: {e}")
        if session_health.should_send_alert("keepalive_failed"):
            telegram_notify.notify_keepalive_result(False, f"Исключение: {e}")
        return

    if result.get("success"):
        logger.info(f"Keepalive OK: {result.get('cookies_count', 0)} cookies")
        # После успешного keepalive сбрасываем флаги алертов — новый цикл
        session_health.reset_alert("keepalive_failed")
        session_health.reset_alert("auth_expired_runtime")
        session_health.reset_alert("auth_expired_scheduled")
        # Сразу перепроверим статус — вдруг порог warning ушёл дальше
        task_check_session_health()
        return

    if result.get("auth_expired"):
        logger.error("Keepalive: сессия мертва, нужен релогин")
        if session_health.should_send_alert("auth_expired_keepalive"):
            telegram_notify.notify_session_expired(result.get("error", ""))
        return

    logger.warning(f"Keepalive не удался: {result.get('error')}")
    if session_health.should_send_alert("keepalive_failed"):
        telegram_notify.notify_keepalive_result(False, result.get("error", ""))


def task_daily_stats():
    """Задача: ежедневная сводка."""
    try:
        stats = sheets.get_stats()
        telegram_notify.notify_daily_stats(stats)
    except Exception as e:
        logger.error(f"Ошибка отправки статистики: {e}")


def main():
    logger.info("=" * 50)
    logger.info("🏠 Cian Auto-Messenger запущен")
    logger.info("=" * 50)

    # Проверяем наличие cookies
    if not Path(SESSION_FILE).exists():
        logger.error("❌ Файл cian_session.json не найден! Сначала запусти: python login_cian.py")
        sys.exit(1)

    # Проверяем наличие credentials
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    if not Path(creds_file).exists():
        logger.error(f"❌ Файл {creds_file} не найден! Настрой Google Sheets API.")
        sys.exit(1)

    # Стартовая проверка здоровья сессии — алертим сразу, а не ждём расписания
    task_check_session_health()

    send_hour = int(os.getenv("SEND_HOUR", 12))
    send_minute = int(os.getenv("SEND_MINUTE", 0))
    check_interval = int(os.getenv("CHECK_REPLIES_INTERVAL_MINUTES", 60))
    keepalive_days = int(os.getenv("KEEPALIVE_INTERVAL_DAYS", 7))

    scheduler = BackgroundScheduler(timezone="Europe/Moscow")

    # 1. Отправка сообщений — каждый день в указанное время
    scheduler.add_job(
        task_send_messages,
        "cron",
        hour=send_hour,
        minute=send_minute,
        id="send_messages",
        name="Отправка сообщений",
    )

    # 2. Проверка ответов — каждый час
    scheduler.add_job(
        task_check_replies,
        "interval",
        minutes=check_interval,
        id="check_replies",
        name="Проверка ответов",
    )

    # 3. Парсинг подборок — каждые 30 минут
    scheduler.add_job(
        task_process_collections,
        "interval",
        minutes=30,
        id="process_collections",
        name="Парсинг подборок",
    )

    # 4. Дневная сводка — в 20:00
    scheduler.add_job(
        task_daily_stats,
        "cron",
        hour=20,
        minute=0,
        id="daily_stats",
        name="Дневная сводка",
    )

    # 5. Проверка здоровья сессии — каждый день в 11:55 (до отправки в 12:00)
    scheduler.add_job(
        task_check_session_health,
        "cron",
        hour=11,
        minute=55,
        id="session_health",
        name="Проверка здоровья сессии",
    )

    # 6. Keepalive — раз в N дней, в 03:30 ночью (без конфликта с другими задачами)
    scheduler.add_job(
        task_keepalive_session,
        "interval",
        days=keepalive_days,
        id="keepalive_session",
        name="Keepalive сессии Циана",
    )

    logger.info(f"📅 Отправка: каждый день в {send_hour}:{send_minute:02d} МСК (через {_SEND_VIA})")
    logger.info(f"📬 Проверка ответов: каждые {check_interval} мин")
    logger.info(f"📊 Сводка: каждый день в 20:00 МСК")
    logger.info(f"🔄 Keepalive сессии: раз в {keepalive_days} дн.")

    # Catch-up: если сегодняшнее cron-окно отправки прошло, а task_send_messages
    # ещё не отработала — догнать через 60 секунд после старта.
    now = datetime.now()
    today_send_at = now.replace(hour=send_hour, minute=send_minute, second=0, microsecond=0)
    if now >= today_send_at and not _today_send_already_done():
        catchup_at = now + timedelta(seconds=60)
        scheduler.add_job(
            task_send_messages,
            "date",
            run_date=catchup_at,
            id="catchup_send",
            name="Catch-up отправка сообщений",
        )
        logger.warning(
            f"Cron-окно отправки в {send_hour:02d}:{send_minute:02d} пропущено сегодня, "
            f"запланирован catch-up на {catchup_at.strftime('%H:%M:%S')} МСК"
        )
        try:
            telegram_notify.send_to_general(
                f"⏰ Сегодняшнее окно отправки ({send_hour:02d}:{send_minute:02d}) было пропущено — "
                f"запускаю догоняющую отправку через минуту."
            )
        except Exception as e:
            logger.warning(f"Не удалось уведомить о catch-up: {e}")

    logger.info("")
    logger.info("Жду расписания... (Ctrl+C для остановки)")

    # Стартовое уведомление
    telegram_notify.send_to_general("Cian Auto-Messenger запущен и работает!")

    # Запускаем scheduler в фоне
    scheduler.start()

    # Запускаем Telegram polling в основном потоке
    logger.info("Запускаю Telegram polling (ответы из группы → Циан)...")
    try:
        telegram_bot.run_polling()
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
        scheduler.shutdown()
        telegram_notify.send_to_general("Cian Auto-Messenger остановлен")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        scheduler.shutdown()
        telegram_notify.send_to_general(f"Критическая ошибка: {e}")
        raise


if __name__ == "__main__":
    # Если передан аргумент --test-send, отправляем одно сообщение для теста
    if len(sys.argv) > 1 and sys.argv[1] == "--test-send":
        logger.info("🧪 Тестовый режим: одна отправка")
        task_send_messages()
    elif len(sys.argv) > 1 and sys.argv[1] == "--test-replies":
        logger.info("🧪 Тестовый режим: проверка ответов")
        task_check_replies()
    elif len(sys.argv) > 1 and sys.argv[1] == "--stats":
        task_daily_stats()
    elif len(sys.argv) > 1 and sys.argv[1] == "--health-check":
        logger.info("🧪 Проверка здоровья сессии")
        task_check_session_health()
    elif len(sys.argv) > 1 and sys.argv[1] == "--keepalive":
        logger.info("🧪 Ручной keepalive")
        task_keepalive_session()
    else:
        main()
