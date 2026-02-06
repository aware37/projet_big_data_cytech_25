"""Pytest configuration: add src/ to sys.path for taxi_ml imports."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
