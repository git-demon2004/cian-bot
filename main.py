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
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Загружаем .env до импорта модулей
load_dotenv()

import sheets
import cian_api
import telegram_notify
import telegram_bot

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

        # Отправляем через API
        result = cian_api.send_message(url, msg_text)

        topic_id = item.get("topic_id")

        if result["success"]:
            new_count = msg_index + 1
            sheets.mark_sent(item["row"], new_count, days_between)
            sent_count += 1
            if topic_id:
                telegram_notify.notify_send_result(topic_id, url, new_count, True)
        else:
            errors_count += 1
            if topic_id:
                telegram_notify.notify_send_result(topic_id, url, msg_index + 1, False, result["error"])

        # Пауза между отправками — 30-90 секунд (антиспам)
        if pending.index(item) < len(pending) - 1:
            delay = random.randint(30, 90)
            logger.info(f"Пауза {delay} сек перед следующей отправкой...")
            time.sleep(delay)

    logger.info(f"Итого: отправлено {sent_count}, ошибок {errors_count}")


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
    if not Path(os.getenv("CIAN_SESSION_FILE", "cian_session.json")).exists():
        logger.error("❌ Файл cian_session.json не найден! Сначала запусти: python login_cian.py")
        sys.exit(1)

    # Проверяем наличие credentials
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    if not Path(creds_file).exists():
        logger.error(f"❌ Файл {creds_file} не найден! Настрой Google Sheets API.")
        sys.exit(1)

    send_hour = int(os.getenv("SEND_HOUR", 12))
    send_minute = int(os.getenv("SEND_MINUTE", 0))
    check_interval = int(os.getenv("CHECK_REPLIES_INTERVAL_MINUTES", 60))

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

    # 3. Дневная сводка — в 20:00
    scheduler.add_job(
        task_daily_stats,
        "cron",
        hour=20,
        minute=0,
        id="daily_stats",
        name="Дневная сводка",
    )

    logger.info(f"📅 Отправка: каждый день в {send_hour}:{send_minute:02d} МСК")
    logger.info(f"📬 Проверка ответов: каждые {check_interval} мин")
    logger.info(f"📊 Сводка: каждый день в 20:00 МСК")
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
    else:
        main()
