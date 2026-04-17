"""
Извлекает cookies cian.ru из Chrome (macOS) и сохраняет в cian_session.json
"""
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
from pathlib import Path

import hashlib
import hmac

CHROME_BASE = Path.home() / "Library/Application Support/Google/Chrome"
PROFILE = "Profile 1"
OUTPUT_FILE = Path("cian_session.json")
CIAN_DOMAINS = ("cian.ru",)


def get_chrome_key() -> bytes:
    """Получает ключ шифрования Chrome из macOS Keychain."""
    result = subprocess.run(
        [
            "security", "find-generic-password",
            "-w",
            "-a", "Chrome",
            "-s", "Chrome Safe Storage",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Не удалось получить ключ из Keychain: {result.stderr}")

    password = result.stdout.strip().encode("utf-8")

    # Derive AES key: PBKDF2 с солью "saltysalt", 1003 итерации, 16 байт
    import hashlib
    key = hashlib.pbkdf2_hmac(
        "sha1",
        password,
        b"saltysalt",
        1003,
        dklen=16,
    )
    return key


def decrypt_cookie(encrypted_value: bytes, key: bytes) -> str:
    """Расшифровывает значение cookie Chrome (v10/v11, AES-128-CBC)."""
    from Crypto.Cipher import AES

    if not encrypted_value or len(encrypted_value) < 3:
        return ""

    # Версия v10/v11 — первые 3 байта это префикс
    if encrypted_value[:3] in (b"v10", b"v11"):
        encrypted_value = encrypted_value[3:]
    else:
        return ""

    # IV = 16 пробелов (Chrome macOS)
    iv = b" " * 16

    try:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        decrypted = cipher.decrypt(encrypted_value)
        # Убираем PKCS7 padding
        pad_len = decrypted[-1]
        if 1 <= pad_len <= 16:
            decrypted = decrypted[:-pad_len]
        # Chrome prepends 32 bytes nonce перед фактическим значением
        decrypted = decrypted[32:]
        return decrypted.decode("utf-8", errors="ignore")
    except Exception:
        return ""


def extract_cookies(profile: str, key: bytes) -> list[dict]:
    """Извлекает и расшифровывает cookies cian.ru из указанного профиля."""
    cookie_db = CHROME_BASE / profile / "Cookies"
    if not cookie_db.exists():
        raise FileNotFoundError(f"Файл Cookies не найден: {cookie_db}")

    # Копируем базу — Chrome может держать её заблокированной
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = tmp.name
    shutil.copy2(str(cookie_db), tmp_path)

    cookies = []
    try:
        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("""
            SELECT host_key, name, encrypted_value, value, path,
                   expires_utc, is_secure, is_httponly, samesite
            FROM cookies
            WHERE host_key LIKE '%cian.ru%'
        """)

        rows = cur.fetchall()
        conn.close()

        for row in rows:
            enc = row["encrypted_value"]
            val = row["value"]

            if enc and len(enc) > 3:
                val = decrypt_cookie(enc, key)

            if not val:
                continue

            # Приводим expires_utc из Chrome (микросекунды с 1601) к Unix timestamp
            expires_utc = row["expires_utc"]
            if expires_utc:
                expires_unix = (expires_utc / 1_000_000) - 11644473600
            else:
                expires_unix = 0

            cookies.append({
                "name": row["name"],
                "value": val,
                "domain": row["host_key"],
                "path": row["path"] or "/",
                "expires": expires_unix,
                "httpOnly": bool(row["is_httponly"]),
                "secure": bool(row["is_secure"]),
                "sameSite": ["Unspecified", "Strict", "Lax", "None"][row["samesite"] or 0],
            })

    finally:
        os.unlink(tmp_path)

    return cookies


def main():
    print(f"Извлекаю cookies из профиля: {PROFILE}")

    print("Получаю ключ из Keychain...")
    key = get_chrome_key()
    print(f"Ключ получен: {key.hex()}")

    print("Читаю cookies из базы Chrome...")
    cookies = extract_cookies(PROFILE, key)
    print(f"Найдено cookies cian.ru: {len(cookies)}")

    if not cookies:
        print("ОШИБКА: Cookies не найдены! Убедись что залогинен на cian.ru в Chrome (Profile 1)")
        return

    # Выводим список для проверки
    for c in cookies:
        print(f"  {c['domain']} | {c['name']} = {c['value'][:30]}...")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)

    print(f"\nГотово! Сохранено в {OUTPUT_FILE} ({len(cookies)} cookies)")


if __name__ == "__main__":
    main()
