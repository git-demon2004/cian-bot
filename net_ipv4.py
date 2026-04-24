"""
Force IPv4 for all outbound HTTP via urllib3/requests.

Повод: на VPS (российский ДЦ) нет default IPv6 route (`ip -6 route` пусто),
но `getaddrinfo` всё равно возвращает AAAA-записи. urllib3 пробует IPv6
и падает с `[Errno 101] Network is unreachable`. Telegram API в итоге
недостижим, хотя прямой IPv4 работает.

Monkey-patch переопределяет urllib3-helper, чтобы он запрашивал только
AF_INET. Импортируется из модулей, которые ходят в сеть через requests
(telegram_notify, telegram_bot). Повторное применение идемпотентно.
"""
from __future__ import annotations

import socket

try:
    import urllib3.util.connection as _u3c
except ImportError:  # pragma: no cover — urllib3 всегда подтянется с requests
    _u3c = None


_APPLIED = False


def force_ipv4() -> None:
    """Переопределяет urllib3.util.connection.allowed_gai_family → AF_INET."""
    global _APPLIED
    if _APPLIED or _u3c is None:
        return
    _u3c.allowed_gai_family = lambda: socket.AF_INET
    _APPLIED = True


# Применяем сразу при импорте — чтобы модули-потребители
# получили IPv4-only поведение просто фактом `import net_ipv4`.
force_ipv4()
