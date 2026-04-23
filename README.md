# 🏠 Cian Auto-Messenger

Автоматическая рассылка сообщений собственникам на Циан с Google Sheets интеграцией.

## Как работает

1. Ты закидываешь ссылки на объявления Циан в Google Sheets
2. На следующий день в 12:00 отправляется первое сообщение
3. Далее каждые 3 дня — следующее сообщение (до 20 штук)
4. Если собственник ответил — цепочка встаёт на паузу
5. Уведомление в Telegram когда кто-то ответил

## Установка на VPS (Ubuntu)

```bash
# 1. Клонируй проект
git clone <repo> && cd cian-bot

# 2. Python 3.10+ и venv
sudo apt update && sudo apt install python3.10 python3.10-venv -y
python3 -m venv venv
source venv/bin/activate

# 3. Зависимости
pip install -r requirements.txt
playwright install chromium
playwright install-deps

# 4. Скопируй и заполни конфиг
cp .env.example .env
nano .env

# 5. Настрой Google Sheets API (см. ниже)

# 6. Первый логин в Циан (интерактивно, нужен дисплей или VNC)
python login_cian.py

# 7. Запуск
python main.py
```

## Настройка Google Sheets

1. Иди на https://console.cloud.google.com/
2. Создай проект → Enable Google Sheets API
3. Создай Service Account → скачай JSON-ключ
4. Назови файл `credentials.json`, положи в корень проекта
5. Открой свою Google таблицу → Поделиться → добавь email сервисного аккаунта
6. ID таблицы из URL: `https://docs.google.com/spreadsheets/d/ЭТОТ_ID/edit`

## Настройка Telegram бота

1. Напиши @BotFather → /newbot → получи токен
2. Напиши своему боту любое сообщение
3. Открой `https://api.telegram.org/bot<TOKEN>/getUpdates` → найди chat_id
4. Впиши оба значения в `.env`

## Структура Google Sheets

| A (Ссылка) | B (Дата добавления) | C (Отправлено) | D (Следующая отправка) | E (Статус) | F (Ответ) |
|---|---|---|---|---|---|
| https://cian.ru/sale/flat/123 | 2025-03-23 | 0 | 2025-03-24 | active | |

- Столбец A заполняешь вручную (ссылки)
- Остальные столбцы заполняются автоматически

## Шаблоны сообщений

Редактируй файл `messages.txt` — каждое сообщение на новой строке, разделитель `---`.
Сообщение №1 отправляется первым, №2 вторым и т.д.

## Сессия Циан и keepalive

Auth-cookie Циан (`DMIR_AUTH`) живёт ~30 дней. Бот сам следит за сессией:

- **Keepalive** раз в `KEEPALIVE_INTERVAL_DAYS` (по умолчанию 7) — фоновый заход
  в `/dialogs/` через Playwright, чтобы Циан продлил `DMIR_AUTH`.
- **Проверка здоровья** при старте и каждый день в 11:55 МСК — если до истечения
  осталось ≤5 дней, в Telegram прилетает предупреждение.
- **Детект auth-ошибки** при отправке — если API Циана отвечает `401` или
  `"X-Real-UserId"`, бот останавливает рассылку и шлёт алерт «нужен релогин».

Алерты дедуплицируются (не чаще раза в сутки на один ключ), состояние в
`session_alert_state.json`.

Ручная проверка:

```bash
python main.py --health-check   # показать остаток дней и при необходимости алерт
python main.py --keepalive      # прогнать keepalive вручную
```

Если keepalive не помог (Циан принудительно разлогинил) — нужен повторный
`python login_cian.py`. После релогина бот продолжит работу, перезапуск не нужен.
