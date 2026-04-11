#!/bin/bash
# Скрипт установки на VPS (Ubuntu 22/24)
# Запуск: chmod +x setup_vps.sh && ./setup_vps.sh

set -e

echo "========================================="
echo "  🏠 Cian Bot - Установка на VPS"
echo "========================================="

# 1. Системные зависимости
echo "[1/5] Устанавливаю системные пакеты..."
sudo apt update
sudo apt install -y python3 python3-venv python3-pip xvfb

# 2. Python окружение
echo "[2/5] Создаю Python окружение..."
python3 -m venv venv
source venv/bin/activate

# 3. Python зависимости
echo "[3/5] Устанавливаю Python зависимости..."
pip install -r requirements.txt

# 4. Playwright browsers
echo "[4/5] Устанавливаю Chromium для Playwright..."
playwright install chromium
playwright install-deps

# 5. Systemd сервис
echo "[5/5] Настраиваю systemd сервис..."
CURRENT_USER=$(whoami)
CURRENT_DIR=$(pwd)

# Подставляем текущего пользователя и путь
sed -i "s|User=ubuntu|User=$CURRENT_USER|g" cian-bot.service
sed -i "s|/home/ubuntu/cian-bot|$CURRENT_DIR|g" cian-bot.service

sudo cp cian-bot.service /etc/systemd/system/
sudo systemctl daemon-reload

echo ""
echo "========================================="
echo "  ✅ Установка завершена!"
echo "========================================="
echo ""
echo "Следующие шаги:"
echo ""
echo "1. Заполни .env:"
echo "   cp .env.example .env && nano .env"
echo ""
echo "2. Положи credentials.json (Google Sheets API)"
echo ""
echo "3. Залогинься в Циан (нужен VNC или X11 forwarding):"
echo "   source venv/bin/activate"
echo "   python login_cian.py"
echo ""
echo "4. Запусти бота:"
echo "   sudo systemctl start cian-bot"
echo "   sudo systemctl enable cian-bot  # автозапуск"
echo ""
echo "5. Проверь логи:"
echo "   sudo journalctl -u cian-bot -f"
echo "   tail -f cian_bot.log"
