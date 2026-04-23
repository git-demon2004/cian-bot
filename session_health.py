"""
Мониторинг здоровья сессии Циан.

Отвечает за:
1. Чтение срока жизни auth-cookie (DMIR_AUTH) из cian_session.json
2. Детект auth-ошибок в ответах API
3. Дедупликацию алертов в Telegram (не чаще одного раза в сутки)
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Имя куки, от которой зависит авторизация на api.cian.ru
AUTH_COOKIE_NAME = "DMIR_AUTH"

# Порог предупреждения — за сколько дней до истечения начинать алертить
WARN_DAYS_THRESHOLD = 5

# Сигнатуры auth-ошибок в теле ответа API Циана
_AUTH_ERROR_SIGNATURES = (
    "X-Real-UserId",
    "Ошибка авторизации",
    "Unauthorized",
)

# Файл состояния алертов — чтобы не спамить в Telegram каждый час
_ALERT_STATE_FILE = Path(os.getenv("SESSION_ALERT_STATE_FILE", "session_alert_state.json"))


@dataclass(frozen=True)
class SessionStatus:
    """Снимок состояния сессии."""

    cookie_found: bool
    expires_at: Optional[datetime]
    days_left: Optional[float]

    @property
    def is_expired(self) -> bool:
        return self.days_left is not None and self.days_left <= 0

    @property
    def needs_warning(self) -> bool:
        return (
            self.days_left is not None
            and 0 < self.days_left <= WARN_DAYS_THRESHOLD
        )


def read_session_status(
    session_file: Path | str,
    now: Optional[datetime] = None,
) -> SessionStatus:
    """
    Читает cian_session.json и возвращает статус auth-cookie.

    Не падает на отсутствующем/битом файле — возвращает cookie_found=False.
    """
    now = now or datetime.now(timezone.utc)
    path = Path(session_file)

    if not path.exists():
        return SessionStatus(cookie_found=False, expires_at=None, days_left=None)

    try:
        cookies = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"Не удалось прочитать {path}: {e}")
        return SessionStatus(cookie_found=False, expires_at=None, days_left=None)

    if not isinstance(cookies, list):
        return SessionStatus(cookie_found=False, expires_at=None, days_left=None)

    for c in cookies:
        if not isinstance(c, dict):
            continue
        if c.get("name") != AUTH_COOKIE_NAME:
            continue

        expires_raw = c.get("expires", -1)
        # -1 или 0 — session cookie, без фиксированной даты истечения
        if expires_raw is None or expires_raw <= 0:
            return SessionStatus(cookie_found=True, expires_at=None, days_left=None)

        try:
            expires_at = datetime.fromtimestamp(float(expires_raw), tz=timezone.utc)
        except (ValueError, OSError, OverflowError):
            return SessionStatus(cookie_found=True, expires_at=None, days_left=None)

        days_left = (expires_at - now).total_seconds() / 86400.0
        return SessionStatus(
            cookie_found=True,
            expires_at=expires_at,
            days_left=days_left,
        )

    return SessionStatus(cookie_found=False, expires_at=None, days_left=None)


def is_auth_error(status_code: int, response_body: str) -> bool:
    """
    Определяет, является ли ответ API признаком протухшей сессии.

    Триггеры:
    - 401
    - 400 с текстом "X-Real-UserId" / "Ошибка авторизации"
    - 403 с текстом "Unauthorized"
    """
    if status_code == 401:
        return True

    body = response_body or ""
    if status_code in (400, 403):
        return any(sig in body for sig in _AUTH_ERROR_SIGNATURES)

    return False


def _load_alert_state() -> dict:
    if not _ALERT_STATE_FILE.exists():
        return {}
    try:
        return json.loads(_ALERT_STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def _save_alert_state(state: dict) -> None:
    try:
        _ALERT_STATE_FILE.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except OSError as e:
        logger.warning(f"Не удалось сохранить состояние алертов: {e}")


def should_send_alert(
    alert_key: str,
    now: Optional[datetime] = None,
    min_interval_hours: float = 24.0,
) -> bool:
    """
    Возвращает True, если с момента последнего алерта с тем же ключом
    прошло не меньше min_interval_hours. Если True — записывает факт алерта.

    Используется для дедупликации, чтобы не спамить Telegram.
    """
    now = now or datetime.now(timezone.utc)
    state = _load_alert_state()

    last_raw = state.get(alert_key)
    if last_raw:
        try:
            last = datetime.fromisoformat(last_raw)
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            elapsed_hours = (now - last).total_seconds() / 3600.0
            if elapsed_hours < min_interval_hours:
                return False
        except ValueError:
            pass

    new_state = {**state, alert_key: now.isoformat()}
    _save_alert_state(new_state)
    return True


def reset_alert(alert_key: str) -> None:
    """Сбрасывает дедуп по ключу — чтобы после релогина снова можно было алертить."""
    state = _load_alert_state()
    if alert_key in state:
        new_state = {k: v for k, v in state.items() if k != alert_key}
        _save_alert_state(new_state)
