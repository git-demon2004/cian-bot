"""
Модуль работы с Google Sheets.
Читает ссылки, обновляет статусы, записывает результаты.
"""
import logging
import os
from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# Столбцы таблицы (0-indexed)
COL_URL = 0        # A — ссылка на объявление
COL_ADDED = 1      # B — дата добавления
COL_SENT = 2       # C — кол-во отправленных сообщений
COL_NEXT = 3       # D — дата следующей отправки
COL_STATUS = 4     # E — статус (active / replied / paused / done)
COL_REPLY = 5      # F — текст ответа
COL_TOPIC = 6      # G — Telegram topic_id

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _get_client() -> gspread.Client:
    """Создаёт авторизованный gspread клиент."""
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    credentials = Credentials.from_service_account_file(creds_file, scopes=SCOPES)
    return gspread.authorize(credentials)


def _get_sheet() -> gspread.Worksheet:
    """Возвращает первый лист таблицы."""
    client = _get_client()
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    spreadsheet = client.open_by_key(sheet_id)
    return spreadsheet.sheet1


def get_pending_sends(days_between: int = 3) -> list[dict]:
    """
    Получает список объявлений, которым пора отправить сообщение.

    Возвращает:
        [{"row": int, "url": str, "sent_count": int}, ...]
    """
    sheet = _get_sheet()
    all_rows = sheet.get_all_values()
    today = datetime.now().strftime("%Y-%m-%d")
    pending = []

    for i, row in enumerate(all_rows):
        if i == 0:  # Пропускаем заголовок
            continue

        # Пропускаем пустые строки
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if not url or not url.startswith("http"):
            continue

        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        sent_count = int(row[COL_SENT]) if len(row) > COL_SENT and row[COL_SENT].isdigit() else 0
        next_send = row[COL_NEXT].strip() if len(row) > COL_NEXT else ""

        # Пропускаем неактивные
        if status in ("replied", "paused", "done"):
            continue

        topic_id = row[COL_TOPIC].strip() if len(row) > COL_TOPIC else ""

        # Если статус пустой — новая ссылка, инициализируем
        if not status:
            added_date = datetime.now().strftime("%Y-%m-%d")
            next_date = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            row_num = i + 1  # gspread 1-indexed
            sheet.update_cell(row_num, COL_ADDED + 1, added_date)
            sheet.update_cell(row_num, COL_SENT + 1, "0")
            sheet.update_cell(row_num, COL_NEXT + 1, next_date)
            sheet.update_cell(row_num, COL_STATUS + 1, "active")

            # Создаём тему в Telegram
            import telegram_notify
            new_topic_id = telegram_notify.create_topic(url)
            if new_topic_id:
                sheet.update_cell(row_num, COL_TOPIC + 1, str(new_topic_id))

            logger.info(f"Инициализирована новая ссылка: {url}")
            continue  # Первая отправка — завтра

        # Проверяем, пора ли отправлять
        if next_send and next_send <= today and sent_count < 20:
            pending.append({
                "row": i + 1,
                "url": url,
                "sent_count": sent_count,
                "topic_id": int(topic_id) if topic_id.isdigit() else None,
            })

    logger.info(f"Найдено {len(pending)} объявлений для отправки")
    return pending


def mark_sent(row: int, new_sent_count: int, days_between: int = 3):
    """Отмечает успешную отправку сообщения."""
    sheet = _get_sheet()
    next_date = (datetime.now() + timedelta(days=days_between)).strftime("%Y-%m-%d")

    sheet.update_cell(row, COL_SENT + 1, str(new_sent_count))
    sheet.update_cell(row, COL_NEXT + 1, next_date)

    # Если отправили 20 сообщений — помечаем done
    if new_sent_count >= 20:
        sheet.update_cell(row, COL_STATUS + 1, "done")
        sheet.update_cell(row, COL_NEXT + 1, "—")
        logger.info(f"Строка {row}: все 20 сообщений отправлены, статус → done")


def mark_replied(offer_url: str, reply_text: str):
    """Ставит статус 'replied' для объявления."""
    sheet = _get_sheet()
    all_rows = sheet.get_all_values()

    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""

        # Сравниваем URL (может быть с/без www, с/без слеша)
        if _urls_match(url, offer_url):
            row_num = i + 1
            sheet.update_cell(row_num, COL_STATUS + 1, "replied")
            sheet.update_cell(row_num, COL_NEXT + 1, "—")
            sheet.update_cell(row_num, COL_REPLY + 1, reply_text[:100])
            logger.info(f"Строка {row_num}: собственник ответил, статус → replied")
            return True

    logger.warning(f"URL не найден в таблице: {offer_url}")
    return False


def _urls_match(url1: str, url2: str) -> bool:
    """Нечёткое сравнение URL Циана."""
    def normalize(u):
        return u.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")
    return normalize(url1) == normalize(url2)


def get_offer_url_by_topic(topic_id: int) -> str | None:
    """Возвращает URL объявления по topic_id."""
    sheet = _get_sheet()
    all_rows = sheet.get_all_values()

    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        stored_topic = row[COL_TOPIC].strip() if len(row) > COL_TOPIC else ""
        if stored_topic.isdigit() and int(stored_topic) == topic_id:
            url = row[COL_URL].strip() if len(row) > COL_URL else ""
            return url if url else None
    return None


def get_topic_id(offer_url: str) -> int | None:
    """Возвращает topic_id для объявления."""
    sheet = _get_sheet()
    all_rows = sheet.get_all_values()

    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if _urls_match(url, offer_url):
            topic_id = row[COL_TOPIC].strip() if len(row) > COL_TOPIC else ""
            return int(topic_id) if topic_id.isdigit() else None
    return None


def get_stats() -> dict:
    """Статистика по таблице."""
    sheet = _get_sheet()
    all_rows = sheet.get_all_values()

    stats = {"total": 0, "active": 0, "replied": 0, "done": 0, "paused": 0}

    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if not url:
            continue

        stats["total"] += 1
        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        if status in stats:
            stats[status] += 1

    return stats
