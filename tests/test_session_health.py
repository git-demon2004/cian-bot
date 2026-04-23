"""Тесты модуля session_health."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import session_health


# ---------- read_session_status ----------


def _write_cookies(path: Path, cookies: list[dict]) -> None:
    path.write_text(json.dumps(cookies), encoding="utf-8")


def test_read_session_status_file_missing(tmp_path):
    status = session_health.read_session_status(tmp_path / "nope.json")
    assert status.cookie_found is False
    assert status.expires_at is None
    assert status.days_left is None


def test_read_session_status_invalid_json(tmp_path):
    p = tmp_path / "s.json"
    p.write_text("{not json", encoding="utf-8")
    status = session_health.read_session_status(p)
    assert status.cookie_found is False


def test_read_session_status_no_auth_cookie(tmp_path):
    p = tmp_path / "s.json"
    _write_cookies(p, [{"name": "_ga", "value": "x", "expires": 9999999999}])
    status = session_health.read_session_status(p)
    assert status.cookie_found is False


def test_read_session_status_session_cookie_no_expiry(tmp_path):
    p = tmp_path / "s.json"
    _write_cookies(p, [{"name": session_health.AUTH_COOKIE_NAME, "value": "v", "expires": -1}])
    status = session_health.read_session_status(p)
    assert status.cookie_found is True
    assert status.expires_at is None
    assert status.days_left is None
    assert status.is_expired is False


def test_read_session_status_fresh_cookie(tmp_path):
    p = tmp_path / "s.json"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    expires = (now + timedelta(days=20)).timestamp()
    _write_cookies(p, [{"name": session_health.AUTH_COOKIE_NAME, "value": "v", "expires": expires}])

    status = session_health.read_session_status(p, now=now)
    assert status.cookie_found is True
    assert status.days_left == pytest.approx(20.0, abs=0.001)
    assert status.is_expired is False
    assert status.needs_warning is False


def test_read_session_status_about_to_expire(tmp_path):
    p = tmp_path / "s.json"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    expires = (now + timedelta(days=3)).timestamp()
    _write_cookies(p, [{"name": session_health.AUTH_COOKIE_NAME, "value": "v", "expires": expires}])

    status = session_health.read_session_status(p, now=now)
    assert status.days_left == pytest.approx(3.0, abs=0.001)
    assert status.needs_warning is True
    assert status.is_expired is False


def test_read_session_status_expired(tmp_path):
    p = tmp_path / "s.json"
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    expires = (now - timedelta(days=1)).timestamp()
    _write_cookies(p, [{"name": session_health.AUTH_COOKIE_NAME, "value": "v", "expires": expires}])

    status = session_health.read_session_status(p, now=now)
    assert status.is_expired is True
    assert status.needs_warning is False


# ---------- is_auth_error ----------


def test_is_auth_error_401():
    assert session_health.is_auth_error(401, "") is True


def test_is_auth_error_x_real_userid():
    assert session_health.is_auth_error(
        400,
        '{"message":"Ошибка авторизации, ожидается заголовок X-Real-UserId"}',
    ) is True


def test_is_auth_error_russian_message():
    assert session_health.is_auth_error(400, '{"message":"Ошибка авторизации"}') is True


def test_is_auth_error_unauthorized_403():
    assert session_health.is_auth_error(403, "Unauthorized") is True


def test_is_auth_error_other_400():
    assert session_health.is_auth_error(400, '{"message":"needCaptcha"}') is False


def test_is_auth_error_200():
    assert session_health.is_auth_error(200, "") is False


def test_is_auth_error_500():
    assert session_health.is_auth_error(500, "Internal") is False


# ---------- should_send_alert ----------


@pytest.fixture
def isolated_alert_state(tmp_path, monkeypatch):
    """Перенаправляет state-файл в tmp, чтобы тесты не зависели друг от друга."""
    state_path = tmp_path / "alerts.json"
    monkeypatch.setattr(session_health, "_ALERT_STATE_FILE", state_path)
    return state_path


def test_should_send_alert_first_time(isolated_alert_state):
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    assert session_health.should_send_alert("k1", now=now) is True


def test_should_send_alert_dedup_within_window(isolated_alert_state):
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    assert session_health.should_send_alert("k1", now=now) is True
    # Через 1 час — должен заглушить
    later = now + timedelta(hours=1)
    assert session_health.should_send_alert("k1", now=later) is False


def test_should_send_alert_after_window(isolated_alert_state):
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    assert session_health.should_send_alert("k1", now=now) is True
    # Через 25 часов — снова можно
    later = now + timedelta(hours=25)
    assert session_health.should_send_alert("k1", now=later) is True


def test_should_send_alert_different_keys_independent(isolated_alert_state):
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    assert session_health.should_send_alert("k1", now=now) is True
    assert session_health.should_send_alert("k2", now=now) is True


def test_reset_alert_allows_resend(isolated_alert_state):
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    session_health.should_send_alert("k1", now=now)
    session_health.reset_alert("k1")
    # Сразу после сброса — снова разрешено
    assert session_health.should_send_alert("k1", now=now + timedelta(minutes=1)) is True


def test_should_send_alert_corrupted_state(isolated_alert_state):
    isolated_alert_state.write_text("not json", encoding="utf-8")
    now = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)
    # Битый state — как если бы его не было, разрешаем
    assert session_health.should_send_alert("k1", now=now) is True
