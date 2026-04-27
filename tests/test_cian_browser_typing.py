"""Тесты для _type_message_with_newlines: \\n не должен отправлять, должен идти как Shift+Enter."""
import sys
from unittest.mock import MagicMock, call

# cian_browser импортирует playwright/stealth на уровне модуля.
# Стабим, чтобы тестировать чистую логику печати без браузера.
sys.modules.setdefault("playwright", MagicMock())
sys.modules.setdefault("playwright.sync_api", MagicMock())
sys.modules.setdefault("playwright_stealth", MagicMock())

import cian_browser  # noqa: E402


def _capture_textarea() -> MagicMock:
    """Mock textarea фиксирующий порядок type/press."""
    textarea = MagicMock()
    return textarea


def test_single_line_no_press_calls():
    """Однострочный текст не должен вызывать press."""
    textarea = _capture_textarea()
    cian_browser._type_message_with_newlines(textarea, "Привет!")
    textarea.press.assert_not_called()
    # Каждый символ — отдельный type
    assert textarea.type.call_count == len("Привет!")


def test_newline_becomes_shift_enter_not_submit():
    """\\n между строками → Shift+Enter, без голого Enter."""
    textarea = _capture_textarea()
    cian_browser._type_message_with_newlines(textarea, "Строка 1\nСтрока 2")
    # Один Shift+Enter между двумя строками
    textarea.press.assert_called_once_with("Shift+Enter")
    # Голого Enter тут быть не должно — он отдельно делается в send_message
    enter_calls = [c for c in textarea.press.call_args_list if c == call("Enter")]
    assert enter_calls == []


def test_multiple_paragraphs_with_blank_lines():
    """Реальный шаблон с пустыми строками между абзацами и soft-newlines внутри."""
    text = (
        "Квартиры покупают не за количество метров.\n"
        "\n"
        "Покупают за ощущение, которое она дарит.\n"
        "\n"
        "Задача фото и описания —\n"
        "передать не «что есть»,\n"
        "а «как это ощущается».\n"
    )
    textarea = _capture_textarea()
    cian_browser._type_message_with_newlines(textarea, text)

    # split("\n") на 7 \n даёт 8 элементов → 7 Shift+Enter
    expected_shift_enter_count = text.count("\n")
    actual = textarea.press.call_args_list
    assert all(c == call("Shift+Enter") for c in actual), (
        f"Все press должны быть Shift+Enter, получили: {actual}"
    )
    assert len(actual) == expected_shift_enter_count, (
        f"Ожидали {expected_shift_enter_count} Shift+Enter, получили {len(actual)}"
    )


def test_no_raw_enter_anywhere_in_typing():
    """Регрессия: ни в одном месте не должно прилететь press('Enter')."""
    text = "a\nb\nc\nd\ne"
    textarea = _capture_textarea()
    cian_browser._type_message_with_newlines(textarea, text)
    for c in textarea.press.call_args_list:
        assert c != call("Enter"), f"Найден submit-Enter в позиции {c}"


def test_type_called_only_with_visible_chars():
    """type() не должен получать \\n как символ — иначе вернёмся к старому багу."""
    text = "ab\ncd"
    textarea = _capture_textarea()
    cian_browser._type_message_with_newlines(textarea, text)
    typed_chars = [c.args[0] for c in textarea.type.call_args_list]
    assert "\n" not in typed_chars, f"\\n не должен попадать в type(): {typed_chars}"
    assert "".join(typed_chars) == "abcd"
