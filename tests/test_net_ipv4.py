"""Тесты принудительного IPv4 для urllib3."""
from __future__ import annotations

import socket

import net_ipv4


def test_force_ipv4_sets_urllib3_allowed_family():
    import urllib3.util.connection as u3c
    net_ipv4.force_ipv4()
    assert u3c.allowed_gai_family() == socket.AF_INET


def test_force_ipv4_is_idempotent():
    net_ipv4.force_ipv4()
    net_ipv4.force_ipv4()  # повторный вызов не должен падать
    import urllib3.util.connection as u3c
    assert u3c.allowed_gai_family() == socket.AF_INET


def test_module_import_applies_patch_automatically():
    # Сам факт `import net_ipv4` в тестовом модуле уже должен применить патч —
    # проверяем, что urllib3 уже переключён на AF_INET до ручного вызова.
    import urllib3.util.connection as u3c
    assert u3c.allowed_gai_family() == socket.AF_INET
