"""
Ops-script: проверка соответствия Sheets sent_count vs реальное число
наших исходящих сообщений в Cian-чате.

USAGE (требует остановленного сервиса и DISPLAY=:99):
    systemctl stop cian-bot
    pgrep -a Xvfb || (Xvfb :99 -screen 0 1280x800x24 >/dev/null 2>&1 &)
    cd /root/cian-bot && DISPLAY=:99 ./venv/bin/python ops_chat_scan.py --rows 81,11,22 --dry-run
    # При --apply: откатывает sent_count в Sheets для row, где our_bubbles < sc
    systemctl start cian-bot

Опции:
    --rows  N1,N2,...  только эти строки Sheets (1-based)
    --all              сканировать все active-row
    --max  N           ограничить N строк (default 10)
    --apply            обновить Sheets если найден mismatch (иначе только print)

⚠️  Перед использованием обязательно проверить BUBBLE_SELECTORS на реальном
DOM Cian-чата — селекторы меняются после Cian-апдейтов. См. строку
BUBBLE_SELECTORS ниже.
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, "/root/cian-bot")
from dotenv import load_dotenv

load_dotenv("/root/cian-bot/.env")

import sheets

# TODO 2026-04-30: селекторы НЕ ПРОВЕРЕНЫ на реальном DOM Cian-чата.
# Перед запуском зайти headed-mode (Xvfb + screenshot) на одну отправку,
# смотреть DOM message bubbles и определить отличие нашего vs собственника.
# Кандидаты: data-author, [class*="outgoing"], [class*="own"], div с
# определённым background-color, наличие avatar справа vs слева.
BUBBLE_SELECTORS = [
    "[data-author-id]",
    "[class*='outgoing']",
    "[class*='--out']",
    "[class*='_own']",
    "[class*='--own']",
    "[class*='my-message']",
]


def count_our_bubbles(page, our_user_id: str = None) -> int:
    """Подсчёт наших исходящих пузырей. Возвращает -1 если селектор не сработал."""
    for selector in BUBBLE_SELECTORS:
        try:
            els = page.query_selector_all(selector)
            if els:
                if our_user_id and selector == "[data-author-id]":
                    count = sum(
                        1 for el in els if el.get_attribute("data-author-id") == our_user_id
                    )
                else:
                    count = len(els)
                return count
        except Exception:
            continue
    return -1


def scan_one_row(page, row: int, offer_id: str, expected_sc: int, our_user_id: str = None) -> dict:
    url = f"https://www.cian.ru/dialogs/?offerId={offer_id}"
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(5)

    from cian_browser import _handle_captcha, _has_captcha

    if _has_captcha(page):
        if not _handle_captcha(page):
            return {
                "row": row,
                "offer_id": offer_id,
                "expected": expected_sc,
                "actual": None,
                "error": "captcha_failed",
            }
        time.sleep(3)

    actual = count_our_bubbles(page, our_user_id)
    drift = expected_sc - actual if actual >= 0 else None
    return {
        "row": row,
        "offer_id": offer_id,
        "expected": expected_sc,
        "actual": actual,
        "drift": drift,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=str, default="")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--max", type=int, default=10)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    ws = sheets._get_sheet()
    rows_data = ws.get_all_values()

    target_rows = set(int(x) for x in args.rows.split(",") if x.strip()) if args.rows else None

    candidates = []
    for i, r in enumerate(rows_data[1:], start=2):
        if len(r) < 5:
            continue
        url, _, sent_count, _, status = (r + [""] * 5)[:5]
        if status.strip().lower() != "active":
            continue
        if target_rows and i not in target_rows:
            continue
        m = re.search(r"/(\d+)/?$", url.rstrip("/"))
        if not m:
            continue
        oid = m.group(1)
        sc = int(sent_count) if sent_count.strip().isdigit() else 0
        candidates.append((i, oid, sc, url))
        if args.all and len(candidates) >= args.max:
            break

    print(f"Будет просканировано: {len(candidates)} row")
    if not candidates:
        return

    from playwright.sync_api import sync_playwright
    from cian_browser import _get_browser_context

    results = []
    with sync_playwright() as p:
        ctx = _get_browser_context(p)
        page = ctx.new_page()
        try:
            for row, oid, sc, url in candidates:
                try:
                    res = scan_one_row(page, row, oid, sc)
                    results.append(res)
                    actual = res.get("actual")
                    drift = res.get("drift")
                    err = res.get("error", "")
                    print(
                        f"row={row:3} offer={oid} sc={sc} actual={actual} drift={drift} err={err}"
                    )
                    time.sleep(15)
                except Exception as e:
                    print(f"row={row} ERROR: {e}")
        finally:
            ctx.close()

    print()
    print("=== ROLLBACK CANDIDATES (sent_count > actual) ===")
    rollback = [r for r in results if r.get("drift") and r["drift"] > 0]
    for r in rollback:
        row = r["row"]
        print(f"  row={row} offer={r['offer_id']} sc={r['expected']} -> actual={r['actual']} (drift={r['drift']})")

    if args.apply and rollback:
        print()
        print("Применяю rollback в Sheets...")
        today = time.strftime("%Y-%m-%d")
        for r in rollback:
            row = r["row"]
            ws.update_cell(row, 3, r["actual"])
            ws.update_cell(row, 4, today)
            print(f"  row={row}: sc -> {r['actual']}, next_send -> {today}")
            time.sleep(1)


if __name__ == "__main__":
    main()
