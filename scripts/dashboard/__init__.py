"""Web Vitals Dashboard package.

Adds scripts/ to sys.path so that config.py / db.py are importable
regardless of whether this package is launched via the entry-point wrapper
(scripts/dashboard.py) or directly with `streamlit run scripts/dashboard/app.py`.
"""
import sys
from pathlib import Path

_scripts_dir = str(Path(__file__).parent.parent)
if _scripts_dir not in sys.path:
    sys.path.insert(0, _scripts_dir)
