"""
Telegram бот — слушает сообщения в супергруппе.
Если участник отвечает в теме собственника, пересылает ответ на Циан.
"""
import logging
import os
import threading

import requests

logger = logging.getLogger(__name__)


def _get_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    group_id = os.getenv("TELEGRAM_GROUP_ID", "")
    return token, group_id


def _send_to_cian(offer_url: str, text: str) -> bool:
    """Отправляет сообщение на Циан."""
    import cian_api
    result = cian_api.send_message(offer_url, text)
    return result["success"]


def _process_update(update: dict):
    """Обрабатывает одно обновление от Telegram."""
    message = update.get("message")
    if not message:
        return

    token, group_id = _get_config()
    chat = message.get("chat", {})
    chat_id = str(chat.get("id", ""))

    # Только сообщения из нашей группы
    if chat_id != group_id:
        return

    # Только сообщения в темах (message_thread_id)
    topic_id = message.get("message_thread_id")
    if not topic_id:
        return

    # Игнорируем сообщения от бота
    from_user = message.get("from", {})
    if from_user.get("is_bot"):
        return

    text = message.get("text", "").strip()
    if not text:
        return

    # Ищем offer по topic_id
    import sheets
    offer_url = sheets.get_offer_url_by_topic(topic_id)
    if not offer_url:
        logger.debug(f"Topic {topic_id} не привязан к объявлению")
        return

    sender_name = from_user.get("first_name", "Участник")
    logger.info(f"Пересылаю на Циан от {sender_name}: {text[:50]}... → {offer_url}")

    success = _send_to_cian(offer_url, text)

    # Уведомляем в тему о результате
    import telegram_notify
    if success:
        telegram_notify.send_to_topic(topic_id, f"Отправлено на Циан от {sender_name}")
    else:
        telegram_notify.send_to_topic(topic_id, f"Ошибка отправки на Циан")


def run_polling():
    """Запускает long polling для получения сообщений."""
    token, group_id = _get_config()
    if not token or not group_id:
        logger.warning("Telegram polling не запущен: нет TOKEN или GROUP_ID")
        return

    logger.info("Telegram polling запущен")

    offset = 0
    url = f"https://api.telegram.org/bot{token}/getUpdates"

    while True:
        try:
            resp = requests.get(
                url,
                params={"offset": offset, "timeout": 30},
                timeout=35,
            )

            if resp.status_code != 200:
                logger.error(f"Polling ошибка: {resp.status_code}")
                continue

            data = resp.json()
            if not data.get("ok"):
                continue

            for update in data.get("result", []):
                update_id = update["update_id"]
                offset = update_id + 1

                try:
                    _process_update(update)
                except Exception as e:
                    logger.error(f"Ошибка обработки update {update_id}: {e}")

        except requests.exceptions.Timeout:
            continue
        except Exception as e:
            logger.error(f"Polling ошибка: {e}")
            import time
            time.sleep(5)


def start_in_background():
    """Запускает polling в фоновом потоке."""
    thread = threading.Thread(target=run_polling, daemon=True)
    thread.start()
    logger.info("Telegram polling запущен в фоне")
    return thread
