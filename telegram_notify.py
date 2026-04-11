"""
Модуль Telegram уведомлений.
Работает с супергруппой с темами (Forum/Topics).
Каждый собственник = отдельная тема.
"""
import logging
import os
import re

import requests

logger = logging.getLogger(__name__)


def _get_config():
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    group_id = os.getenv("TELEGRAM_GROUP_ID", "")
    return token, group_id


def _api(method: str, **params) -> dict | None:
    """Вызов Telegram Bot API."""
    token, _ = _get_config()
    if not token:
        logger.warning("TELEGRAM_BOT_TOKEN не задан")
        return None

    url = f"https://api.telegram.org/bot{token}/{method}"
    try:
        resp = requests.post(url, json=params, timeout=15)
        data = resp.json()
        if not data.get("ok"):
            logger.error(f"Telegram API {method}: {data}")
            return None
        return data.get("result")
    except Exception as e:
        logger.error(f"Telegram API {method}: {e}")
        return None


def _extract_offer_id(url: str) -> str:
    """Извлекает offer_id из URL."""
    match = re.search(r'/(?:sale|rent)/[\w-]+/(\d+)', url)
    return match.group(1) if match else url[:30]


def create_topic(offer_url: str, offer_info: str = "") -> int | None:
    """
    Создаёт новую тему в супергруппе для собственника.
    Возвращает topic_id (message_thread_id).
    """
    _, group_id = _get_config()
    if not group_id:
        logger.warning("TELEGRAM_GROUP_ID не задан")
        return None

    offer_id = _extract_offer_id(offer_url)
    topic_name = f"#{offer_id}"
    if offer_info:
        topic_name = f"{offer_info} #{offer_id}"

    result = _api(
        "createForumTopic",
        chat_id=group_id,
        name=topic_name[:128],
    )

    if result:
        topic_id = result["message_thread_id"]
        logger.info(f"Создана тема: {topic_name} (topic_id={topic_id})")

        # Приветственное сообщение в тему
        send_to_topic(
            topic_id,
            f"<b>Новое объявление взято в работу</b>\n\n"
            f"<a href=\"{offer_url}\">Открыть на Циан</a>\n\n"
            f"Первое сообщение будет отправлено завтра в 12:00 МСК.\n"
            f"Далее — каждые 3 дня, до 20 сообщений.",
        )
        return topic_id

    return None


def send_to_topic(topic_id: int, text: str):
    """Отправляет сообщение в конкретную тему."""
    _, group_id = _get_config()
    if not group_id:
        return

    _api(
        "sendMessage",
        chat_id=group_id,
        message_thread_id=topic_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


def send_to_general(text: str):
    """Отправляет сообщение в General тему (без thread_id)."""
    _, group_id = _get_config()
    if not group_id:
        return

    _api(
        "sendMessage",
        chat_id=group_id,
        text=text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )


def notify_initialized(topic_id: int, offer_url: str):
    """Уведомление: ссылка взята в работу."""
    send_to_topic(
        topic_id,
        f"<b>Ссылка инициализирована</b>\n"
        f"Статус: active\n"
        f"Первая отправка: завтра в 12:00 МСК",
    )


def notify_send_result(topic_id: int, url: str, msg_num: int, success: bool, error: str = None):
    """Уведомление об отправке сообщения."""
    if success:
        text = f"<b>Сообщение #{msg_num} отправлено</b>"
    else:
        text = f"<b>Ошибка отправки #{msg_num}</b>\n{error}"
    send_to_topic(topic_id, text)


def notify_reply(topic_id: int, offer_url: str, reply_text: str):
    """Уведомление о новом ответе от собственника."""
    send_to_topic(
        topic_id,
        f"<b>Собственник ответил!</b>\n\n"
        f"{reply_text}\n\n"
        f"Цепочка поставлена на паузу.",
    )


def notify_done(topic_id: int):
    """Уведомление: все 20 сообщений отправлены."""
    send_to_topic(
        topic_id,
        f"<b>Все 20 сообщений отправлены</b>\n"
        f"Статус: done. Собственник не ответил.",
    )


def notify_daily_stats(stats: dict):
    """Ежедневная сводка в General."""
    text = (
        f"<b>Ежедневная сводка</b>\n\n"
        f"Всего: {stats.get('total', 0)}\n"
        f"Активных: {stats.get('active', 0)}\n"
        f"Ответили: {stats.get('replied', 0)}\n"
        f"Завершено: {stats.get('done', 0)}\n"
        f"На паузе: {stats.get('paused', 0)}"
    )
    send_to_general(text)
