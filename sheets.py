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


def _get_spreadsheet() -> gspread.Spreadsheet:
    client = _get_client()
    return client.open_by_key(os.getenv("GOOGLE_SHEET_ID"))


def get_stop_urls() -> set:
    """Возвращает множество нормализованных URL из листа Стоп."""
    try:
        sp = _get_spreadsheet()
        stop_sheet = sp.worksheet("Стоп")
        rows = stop_sheet.get_all_values()
        result = set()
        for row in rows[1:]:
            url = row[0].strip() if row else ""
            if url.startswith("http"):
                result.add(_normalize_url(url))
        return result
    except Exception as e:
        logger.warning(f"Не удалось прочитать лист Стоп: {e}")
        return set()


def apply_stop_list():
    """
    Читает лист Стоп и останавливает рассылку по найденным URL:
    ставит статус 'paused' и красит строку серым в листе Рассылка.
    """
    stop_urls = get_stop_urls()
    if not stop_urls:
        return

    sheet = _get_sheet()
    all_rows = sheet.get_all_values()
    stopped = 0

    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if not url.startswith("http"):
            continue
        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        if status in ("paused", "done", "replied", "дубль"):
            continue
        if _normalize_url(url) in stop_urls:
            row_num = i + 1
            sheet.update(values=[["paused", "—"]], range_name=f"E{row_num}:F{row_num}")
            sheet.format(f"A{row_num}:G{row_num}", {
                "backgroundColor": {"red": 0.8, "green": 0.8, "blue": 0.8}
            })
            logger.info(f"Остановлена рассылка (лист Стоп): {url}")
            stopped += 1

    if stopped:
        logger.info(f"Остановлено из листа Стоп: {stopped} объявлений")


def _mark_duplicate(sheet, row_num: int):
    """Красит строку оранжевым и ставит статус 'дубль' — один batch-запрос."""
    sheet.update(values=[["дубль"]], range_name=f"E{row_num}")
    sheet.format(f"A{row_num}:G{row_num}", {
        "backgroundColor": {"red": 1.0, "green": 0.6, "blue": 0.0}
    })


def get_pending_sends(days_between: int = 3) -> list[dict]:
    """
    Получает список объявлений, которым пора отправить сообщение.

    Возвращает:
        [{"row": int, "url": str, "sent_count": int}, ...]
    """
    import time as _time
    import telegram_notify

    sheet = _get_sheet()
    all_rows = sheet.get_all_values()
    today = datetime.now().strftime("%Y-%m-%d")
    added_date = datetime.now().strftime("%Y-%m-%d")
    next_date = datetime.now().strftime("%Y-%m-%d")
    pending = []

    # Собираем все URL у которых уже есть статус (не пустой и не дубль)
    seen_urls = set()
    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        if url.startswith("http") and status and status != "дубль":
            seen_urls.add(_normalize_url(url))

    # Собираем новые ссылки для batch-инициализации
    new_rows = []
    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if not url or not url.startswith("http"):
            continue
        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        if not status:
            norm_url = _normalize_url(url)
            if norm_url in seen_urls:
                _mark_duplicate(sheet, i + 1)
            else:
                new_rows.append((i + 1, url, norm_url))
                seen_urls.add(norm_url)

    # Batch-инициализация: одним запросом обновляем B:E для всех новых строк
    if new_rows:
        batch_data = []
        for row_num, url, _ in new_rows:
            batch_data.append({
                "range": f"B{row_num}:E{row_num}",
                "values": [[added_date, "0", next_date, "active"]],
            })
        sheet.spreadsheet.values_batch_update({"valueInputOption": "RAW", "data": batch_data})
        logger.info(f"Инициализировано {len(new_rows)} новых ссылок (batch)")

        # Создаём темы в Telegram — по одной, с паузой чтоб не словить 429
        for row_num, url, _ in new_rows:
            _time.sleep(1)
            new_topic_id = telegram_notify.create_topic(url)
            if new_topic_id:
                sheet.update(values=[[str(new_topic_id)]], range_name=f"G{row_num}")
            logger.info(f"Инициализирована: {url}")

    # Собираем pending — только активные с наступившей датой
    all_rows = sheet.get_all_values()  # перечитываем после обновлений
    for i, row in enumerate(all_rows):
        if i == 0:
            continue
        url = row[COL_URL].strip() if len(row) > COL_URL else ""
        if not url or not url.startswith("http"):
            continue
        status = row[COL_STATUS].strip().lower() if len(row) > COL_STATUS else ""
        if status != "active":
            continue
        sent_count = int(row[COL_SENT]) if len(row) > COL_SENT and row[COL_SENT].isdigit() else 0
        next_send = row[COL_NEXT].strip() if len(row) > COL_NEXT else ""
        topic_id = row[COL_TOPIC].strip() if len(row) > COL_TOPIC else ""

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
    """Отмечает успешную отправку — один batch-запрос."""
    sheet = _get_sheet()
    next_date = (datetime.now() + timedelta(days=days_between)).strftime("%Y-%m-%d")

    if new_sent_count >= 20:
        sheet.update(values=[[str(new_sent_count), "—", "done"]], range_name=f"C{row}:E{row}")
        logger.info(f"Строка {row}: все 20 сообщений отправлены, статус → done")
    else:
        sheet.update(values=[[str(new_sent_count), next_date]], range_name=f"C{row}:D{row}")


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
            sheet.update(values=[["replied", "—", reply_text[:100]]], range_name=f"E{row_num}:G{row_num}")
            logger.info(f"Строка {row_num}: собственник ответил, статус → replied")
            return True

    logger.warning(f"URL не найден в таблице: {offer_url}")
    return False


def _normalize_url(url: str) -> str:
    """Нормализует URL для сравнения."""
    return url.replace("https://", "").replace("http://", "").replace("www.", "").rstrip("/")


def _urls_match(url1: str, url2: str) -> bool:
    return _normalize_url(url1) == _normalize_url(url2)


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


def _parse_collection_offer_ids(url: str) -> list[str]:
    """Парсит подборку Циан и возвращает список offerIds через браузер."""
    import json
    import re
    import time
    from pathlib import Path
    from playwright.sync_api import sync_playwright
    from cian_api import BROWSER_LOCK

    offer_ids = []
    session_file = os.getenv("CIAN_SESSION_FILE", "cian_session.json")

    with BROWSER_LOCK:
        with sync_playwright() as p:
            context = p.chromium.launch_persistent_context(
                user_data_dir="cian_storage",
                headless=True,
                viewport={"width": 1280, "height": 900},
                locale="ru-RU",
                timezone_id="Europe/Moscow",
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                args=["--no-sandbox", "--disable-gpu", "--disable-blink-features=AutomationControlled"],
                ignore_default_args=["--enable-automation"],
            )

            if Path(session_file).exists():
                with open(session_file) as f:
                    cookies = json.load(f)
                valid = []
                for c in cookies:
                    if c.get("sameSite") not in ("Strict", "Lax", "None"):
                        c["sameSite"] = "Lax"
                    valid.append(c)
                context.add_cookies(valid)

            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            for _ in range(15):
                page.evaluate("window.scrollBy(0, 800)")
                time.sleep(0.5)

            html = page.content()
            offer_ids = list(dict.fromkeys(re.findall(r'"offerId":(\d+)', html)))
            context.close()

    return offer_ids


def process_collections():
    """
    Читает лист Подборки, парсит новые подборки Циан,
    добавляет ссылки в лист База (без дублей).
    """
    sp = _get_spreadsheet()

    try:
        col_sheet = sp.worksheet("Подборки")
    except Exception:
        logger.warning("Лист Подборки не найден")
        return

    try:
        base_sheet = sp.worksheet("База")
    except Exception:
        base_sheet = sp.add_worksheet(title="База", rows=2000, cols=2)
        base_sheet.update_cell(1, 1, "Ссылка")

    rows = col_sheet.get_all_values()

    # Существующие URL в листе База (для дедупликации)
    base_rows = base_sheet.get_all_values()
    existing_urls = set()
    for r in base_rows[1:]:
        if r and r[0].startswith("http"):
            existing_urls.add(_normalize_url(r[0].strip()))

    for i, row in enumerate(rows[1:], 2):
        col_url = row[0].strip() if row else ""
        status = row[1].strip().lower() if len(row) > 1 else ""

        if not col_url.startswith("http"):
            continue
        if status in ("обработано", "ошибка"):
            continue

        logger.info(f"Парсю подборку: {col_url}")
        col_sheet.update_cell(i, 2, "парсинг...")
        col_sheet.update_cell(i, 3, datetime.now().strftime("%Y-%m-%d %H:%M"))

        try:
            def _run():
                return _parse_collection_offer_ids(col_url)

            import threading
            result = []
            err = []
            t = threading.Thread(target=lambda: result.extend(_run()) or True)
            t.start()
            t.join(timeout=180)

            offer_ids = result
            if not offer_ids:
                col_sheet.update_cell(i, 2, "ошибка")
                col_sheet.update_cell(i, 4, "0")
                continue

            # Фильтруем дубли
            new_urls = []
            for oid in offer_ids:
                url_norm = _normalize_url(f"www.cian.ru/sale/flat/{oid}/")
                if url_norm not in existing_urls:
                    new_urls.append(f"https://www.cian.ru/sale/flat/{oid}/")
                    existing_urls.add(url_norm)

            # Добавляем в конец листа База
            if new_urls:
                next_row = len(base_rows) + 1
                base_sheet.update(
                    values=[[u] for u in new_urls],
                    range_name=f"A{next_row}:A{next_row + len(new_urls) - 1}",
                )
                base_rows.extend([[u] for u in new_urls])

            col_sheet.update_cell(i, 2, "обработано")
            col_sheet.update_cell(i, 4, str(len(new_urls)))
            logger.info(f"Подборка {col_url}: добавлено {len(new_urls)} новых из {len(offer_ids)}")

        except Exception as e:
            logger.error(f"Ошибка парсинга подборки {col_url}: {e}")
            col_sheet.update_cell(i, 2, "ошибка")
            col_sheet.update_cell(i, 4, str(e)[:50])
