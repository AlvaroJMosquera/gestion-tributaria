# tests/conftest.py
import sys
from pathlib import Path

# Agrega la raíz del proyecto al PYTHONPATH para que 'backend' sea importable
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))