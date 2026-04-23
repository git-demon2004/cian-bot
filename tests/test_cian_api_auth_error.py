"""Тесты детекта auth-ошибки в send_message."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import cian_api


OFFER_URL = "https://www.cian.ru/rent/flat/327956131/"


def _mock_response(status: int, text: str, json_body: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status
    resp.text = text
    resp.headers = {"content-type": "application/json"}
    resp.json.return_value = json_body or {}
    return resp


@patch("cian_api._get_session")
def test_send_message_auth_expired_sets_flag(mock_get_session):
    session = MagicMock()
    session.post.return_value = _mock_response(
        400,
        '{"message":"Ошибка авторизации, ожидается заголовок X-Real-UserId"}',
        {"message": "Ошибка авторизации, ожидается заголовок X-Real-UserId"},
    )
    mock_get_session.return_value = session

    result = cian_api.send_message(OFFER_URL, "Hi")

    assert result["success"] is False
    assert result.get("auth_expired") is True
    assert "протухла" in (result.get("error") or "").lower() or "session" in (result.get("error") or "").lower()


@patch("cian_api._get_session")
def test_send_message_401_auth_expired(mock_get_session):
    session = MagicMock()
    session.post.return_value = _mock_response(401, "Unauthorized")
    session.post.return_value.headers = {"content-type": "text/plain"}
    mock_get_session.return_value = session

    result = cian_api.send_message(OFFER_URL, "Hi")

    assert result["success"] is False
    assert result.get("auth_expired") is True


@patch("cian_api._get_session")
def test_send_message_success(mock_get_session):
    session = MagicMock()
    session.post.return_value = _mock_response(200, "{}")
    mock_get_session.return_value = session

    result = cian_api.send_message(OFFER_URL, "Hi")

    assert result["success"] is True
    assert result.get("auth_expired") is not True


@patch("cian_api._get_session")
def test_send_message_generic_500_not_auth(mock_get_session):
    session = MagicMock()
    session.post.return_value = _mock_response(500, "server error")
    session.post.return_value.headers = {"content-type": "text/plain"}
    mock_get_session.return_value = session

    result = cian_api.send_message(OFFER_URL, "Hi")

    assert result["success"] is False
    assert result.get("auth_expired") is not True


@patch("cian_api._send_first_via_hint")
@patch("cian_api._get_session")
def test_send_message_captcha_goes_to_hint(mock_get_session, mock_hint):
    session = MagicMock()
    session.post.return_value = _mock_response(
        400,
        '{"message":"needCaptcha"}',
        {"message": "needCaptcha"},
    )
    mock_get_session.return_value = session
    mock_hint.return_value = {"success": True, "error": None}

    result = cian_api.send_message(OFFER_URL, "Hi")

    assert result["success"] is True
    mock_hint.assert_called_once()


def test_send_message_bad_url():
    result = cian_api.send_message("https://example.com/foo", "Hi")
    assert result["success"] is False
    assert "offer_id" in (result.get("error") or "").lower()
