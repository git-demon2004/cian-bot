"""
Тест-скрипт: шлёт ОДНО сообщение или батч из очереди pending.

Не использует APScheduler. Не запускает Telegram polling. Только: берёт
pending items, шлёт через настроенный транспорт (SEND_VIA), обновляет
таблицу при успехе. Останавливается по первой ошибке (кроме единичных
`error` типа не-нашёл-textarea, которые логируются и идут дальше — как
в main.task_send_messages). При `auth_expired` — прерывается СРАЗУ.

Использование:
    python test_send_one.py                 # 1 сообщение (первое pending)
    python test_send_one.py --count 5       # 5 сообщений с production-паузами
    python test_send_one.py --dry-run       # только показать что бы отправил
    python test_send_one.py --index 3       # начать с 3-го по порядку
"""
from __future__ import annotations

import logging
import os
import random
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import sheets
import cian_api
import cian_browser
import session_health
import telegram_notify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("test_send_one")


def _pick_send_impl():
    send_via = os.getenv("SEND_VIA", "browser").strip().lower()
    if send_via == "browser":
        return cian_browser, "browser"
    return cian_api, "api"


def _load_messages() -> list[str]:
    text = Path("messages.txt").read_text(encoding="utf-8")
    return [m.strip() for m in text.split("---") if m.strip()]


def _parse_args() -> tuple[bool, int, int]:
    dry_run = "--dry-run" in sys.argv
    index = 0
    count = 1
    if "--index" in sys.argv:
        index = int(sys.argv[sys.argv.index("--index") + 1])
    if "--count" in sys.argv:
        count = int(sys.argv[sys.argv.index("--count") + 1])
    return dry_run, index, count


def main() -> int:
    dry_run, index, count = _parse_args()

    # Санити-чек сессии заранее
    status = session_health.read_session_status(
        os.getenv("CIAN_SESSION_FILE", "cian_session.json")
    )
    if not status.cookie_found:
        logger.error("Auth-cookie не найдена — пропущу отправку")
        return 2
    if status.is_expired:
        logger.error(f"Сессия Циана истекла ({status.expires_at}) — пропущу")
        return 2
    logger.info(
        f"Сессия ок: осталось ~{status.days_left:.1f} дн. до {status.expires_at}"
    )

    days_between = int(os.getenv("DAYS_BETWEEN_MESSAGES", 3))
    pending = sheets.get_pending_sends(days_between)
    logger.info(f"Всего в очереди: {len(pending)}")

    if not pending:
        logger.info("Очередь пуста")
        return 0
    if index >= len(pending):
        logger.error(f"index={index} вне диапазона (pending={len(pending)})")
        return 1

    send_impl, send_via = _pick_send_impl()
    logger.info(f"Транспорт: {send_via}")
    batch = pending[index : index + count]
    logger.info(f"Отправлю {len(batch)} шт. (index={index}, count={count})")
    messages = _load_messages()

    sent = 0
    failed = 0

    for i, item in enumerate(batch):
        url = item["url"]
        msg_index = item["sent_count"]
        msg_text = messages[msg_index % len(messages)]

        logger.info("-" * 60)
        logger.info(f"[{i+1}/{len(batch)}] row={item['row']} → #{msg_index+1}")
        logger.info(f"URL: {url}")

        if dry_run:
            logger.info(f"--dry-run — шаблон #{msg_index+1}: {msg_text[:80]}...")
            continue

        logger.info("⏳ Отправляю...")
        result = send_impl.send_message(url, msg_text)
        logger.info(f"Result: success={result.get('success')} error={result.get('error')}")

        if result.get("auth_expired"):
            logger.error("🛑 Auth expired — прерываю батч, нужен релогин Циана")
            topic_id = item.get("topic_id")
            if topic_id:
                telegram_notify.notify_send_result(
                    topic_id, url, msg_index + 1, False, result.get("error", "")
                )
            return 3

        topic_id = item.get("topic_id")
        if result.get("success"):
            sheets.mark_sent(item["row"], msg_index + 1, days_between)
            sent += 1
            logger.info(f"✅ row={item['row']} sent_count={msg_index+1}")
            if topic_id:
                telegram_notify.notify_send_result(
                    topic_id, url, msg_index + 1, True
                )
        else:
            failed += 1
            logger.error(f"❌ Пропускаю и иду дальше: {result.get('error')}")
            if topic_id:
                telegram_notify.notify_send_result(
                    topic_id, url, msg_index + 1, False, result.get("error", "")
                )

        # Пауза между отправками — production-паттерн из main.py
        if i < len(batch) - 1 and not dry_run:
            delay = random.randint(30, 90)
            logger.info(f"Пауза {delay} сек...")
            time.sleep(delay)

    logger.info("=" * 60)
    logger.info(f"Итог: отправлено {sent}, ошибок {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
