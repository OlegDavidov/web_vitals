"""
Entry point for the Web Vitals Streamlit dashboard.

Kept at scripts/dashboard.py so manage.py can reference it unchanged:
    streamlit run scripts/dashboard.py

The actual application lives in scripts/dashboard/ (package).
"""
import sys
from pathlib import Path

# Ensure scripts/ is on sys.path so the dashboard package can import config/db
sys.path.insert(0, str(Path(__file__).parent))

from dashboard.app import main  # noqa: E402

main()
