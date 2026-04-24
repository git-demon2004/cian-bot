"""
Telegram бот — слушает сообщения в супергруппе.

Функции:
- Любое сообщение в теме → пересылается на Циан
- /стоп — останавливает рассылку по объявлению и добавляет в лист Стоп
"""
import logging
import os
import threading
from datetime import datetime

import net_ipv4  # noqa: F401 — форсит IPv4 для requests/urllib3
import requests

logger = logging.getLogger(__name__)


def _get_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    group_id = os.getenv("TELEGRAM_GROUP_ID", "")
    return token, group_id


def _get_proxies() -> dict | None:
    proxy = os.getenv("SOCKS5_PROXY")
    if proxy:
        return {"http": f"socks5://{proxy}", "https": f"socks5://{proxy}"}
    return None


def _send_to_cian(offer_url: str, text: str) -> bool:
    import cian_api
    result = cian_api.send_message(offer_url, text)
    return result["success"]


def _handle_stop_command(topic_id: int, offer_url: str, sender_name: str):
    """Добавляет объявление в лист Стоп и останавливает рассылку."""
    import sheets
    import telegram_notify

    try:
        # Добавляем в лист Стоп
        sp = sheets._get_spreadsheet()
        stop_sheet = sp.worksheet("Стоп")
        stop_sheet.append_row([
            offer_url,
            f"Остановлено вручную ({sender_name})",
            datetime.now().strftime("%Y-%m-%d %H:%M"),
        ])

        # Помечаем в Рассылке как paused + серый цвет
        sheets.apply_stop_list()

        telegram_notify.send_to_topic(
            topic_id,
            f"Рассылка остановлена. Объявление добавлено в лист Стоп.",
        )
        logger.info(f"/стоп от {sender_name}: {offer_url}")
    except Exception as e:
        logger.error(f"Ошибка команды /стоп: {e}")
        import telegram_notify
        telegram_notify.send_to_topic(topic_id, f"Ошибка: {e}")


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

    sender_name = from_user.get("first_name", "Участник")

    # Ищем offer по topic_id
    import sheets
    offer_url = sheets.get_offer_url_by_topic(topic_id)

    # Обработка команд
    if text.lower() in ("/стоп", "/stop", "/стоп@" + token.split(":")[0]):
        if not offer_url:
            import telegram_notify
            telegram_notify.send_to_topic(topic_id, "Объявление не найдено для этой темы.")
            return
        _handle_stop_command(topic_id, offer_url, sender_name)
        return

    # Пересылаем на Циан только если тема привязана к объявлению
    if not offer_url:
        logger.debug(f"Topic {topic_id} не привязан к объявлению")
        return

    logger.info(f"Пересылаю на Циан от {sender_name}: {text[:50]} → {offer_url}")
    success = _send_to_cian(offer_url, text)

    import telegram_notify
    if success:
        telegram_notify.send_to_topic(topic_id, f"Отправлено на Циан.")
    else:
        telegram_notify.send_to_topic(topic_id, f"Ошибка отправки на Циан.")


def run_polling():
    """Запускает long polling для получения сообщений."""
    token, group_id = _get_config()
    if not token or not group_id:
        logger.warning("Telegram polling не запущен: нет TOKEN или GROUP_ID")
        return

    logger.info("Telegram polling запущен")

    offset = 0
    url = f"https://api.telegram.org/bot{token}/getUpdates"

    import time
    while True:
        try:
            resp = requests.get(
                url,
                params={"offset": offset, "timeout": 30},
                timeout=35,
                proxies=_get_proxies(),
            )

            if resp.status_code != 200:
                logger.error(f"Polling ошибка: {resp.status_code}")
                time.sleep(5)
                continue

            data = resp.json()
            if not data.get("ok"):
                time.sleep(5)
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
            time.sleep(5)


def start_in_background():
    thread = threading.Thread(target=run_polling, daemon=True)
    thread.start()
    logger.info("Telegram polling запущен в фоне")
    return thread
