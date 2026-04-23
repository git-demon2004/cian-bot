"""Добавляет корень проекта в sys.path, чтобы работали import sheets/cian_api/и т.д."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
