"""
Configuration: loads settings from the .env file in the project root.
All scripts import from here — never read os.environ directly.
"""
import os
import logging
from pathlib import Path

from dotenv import load_dotenv

# Project root is one level above this file
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")


def _require(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise ValueError(
            f"Required environment variable '{key}' is not set. "
            f"Copy .env.example → .env and fill in the values."
        )
    return value


# ── New Relic ──────────────────────────────────────────────────────────────────
NEW_RELIC_ACCOUNT_ID: int = int(_require("NEW_RELIC_ACCOUNT_ID"))
NEW_RELIC_API_KEY: str = _require("NEW_RELIC_API_KEY")

# Browser event type that contains Web Vitals data
NR_EVENT_TYPE: str = os.getenv("NR_EVENT_TYPE", "PageViewTiming")

# Browser application name to scope queries (optional — empty string = all apps)
NR_APP_NAME: str = os.getenv("NR_APP_NAME", "")

# Country code to restrict data collection (optional — empty string = all countries)
NR_COUNTRY_CODE: str = os.getenv("NR_COUNTRY_CODE", "")

# NerdGraph endpoint (supports NRAK User API keys)
NERDGRAPH_URL: str = "https://api.newrelic.com/graphql"

# ── Paths ──────────────────────────────────────────────────────────────────────
DATA_DIR: Path = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
DB_PATH: Path = Path(os.getenv("DB_PATH", str(DATA_DIR / "web_vitals.db")))
LOG_DIR: Path = Path(os.getenv("LOG_DIR", str(DATA_DIR / "logs")))
EXPORTS_DIR: Path = Path(os.getenv("EXPORTS_DIR", str(DATA_DIR / "exports")))

# Ensure runtime directories exist (safe to call at import time)
LOG_DIR.mkdir(parents=True, exist_ok=True)
EXPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Collection ─────────────────────────────────────────────────────────────────
BACKFILL_DAYS: int = int(os.getenv("BACKFILL_DAYS", "30"))
INTERVAL_HOURS: int = int(os.getenv("INTERVAL_HOURS", "6"))

# ── HTTP / Retry ───────────────────────────────────────────────────────────────
REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "60"))
MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY: float = float(os.getenv("RETRY_DELAY", "2.0"))

# SSL verification: True (default), False (skip), or path to a CA bundle file.
# Set NR_SSL_VERIFY=false on machines behind a corporate SSL-inspection proxy,
# or NR_SSL_VERIFY=/path/to/corporate-ca.pem to trust a custom CA bundle.
_ssl_env = os.getenv("NR_SSL_VERIFY", "true").strip()
if _ssl_env.lower() == "false":
    NR_SSL_VERIFY: bool | str = False
elif _ssl_env.lower() in ("true", "1", "yes"):
    NR_SSL_VERIFY = True
else:
    NR_SSL_VERIFY = _ssl_env  # treat as CA bundle path

# Proxy control for requests to api.newrelic.com.
# Needed when HTTPS_PROXY is set at the OS level (load_dotenv doesn't override it).
#
# NR_BYPASS_PROXY=true  — ignore system proxy entirely (fixes HTTP 413 on
#                         machines where a corporate proxy blocks NR requests)
# NR_HTTPS_PROXY=<url>  — route NR requests through a specific proxy URL
#                         (e.g. http://proxy.corp:8080)
# Neither set           — use system proxy settings (default)
_bypass = os.getenv("NR_BYPASS_PROXY", "").strip().lower() in ("true", "1", "yes")
_proxy_url = os.getenv("NR_HTTPS_PROXY", "").strip()

if _bypass:
    NR_TRUST_ENV: bool = False
    NR_PROXIES: dict | None = None
elif _proxy_url:
    NR_TRUST_ENV = False
    NR_PROXIES = {"http": _proxy_url, "https": _proxy_url}
else:
    NR_TRUST_ENV = True
    NR_PROXIES = None

# Seconds between API requests to respect NR rate limits (~3 req/s)
REQUEST_SLEEP: float = float(os.getenv("REQUEST_SLEEP", "0.4"))

# ── Logging helpers ────────────────────────────────────────────────────────────

def setup_logging(log_name: str, level: int = logging.INFO) -> logging.Logger:
    """Configure root logging to both stdout and a dated log file."""
    from datetime import datetime

    today = datetime.now().strftime("%Y%m%d")
    log_file = LOG_DIR / f"{log_name}_{today}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: list[logging.Handler] = [
        logging.StreamHandler(),
        logging.FileHandler(log_file, encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers, force=True)
    return logging.getLogger(log_name)
