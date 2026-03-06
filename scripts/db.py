"""
SQLite database: schema definition, connection management, and CRUD helpers.

Schema notes:
  - One row per (timestamp, targetGroupedUrl, deviceType, connectionType, navigationType)
  - timestamp = Unix epoch of the 6-hour window START
  - Percentiles (p75/p90/p95) stored for four CWV + TTFB
  - All metric columns are nullable (some fields may not exist in every NR account)
"""
import sqlite3
import logging
from collections.abc import Generator
from contextlib import contextmanager

from config import DB_PATH
from schema import SCHEMA  # noqa: F401 — re-exported for external use

logger = logging.getLogger(__name__)

# ── Connection management ──────────────────────────────────────────────────────

@contextmanager
def get_conn() -> Generator[sqlite3.Connection, None, None]:
    """Yield a WAL-mode connection; commit on exit, rollback on error."""
    conn = sqlite3.connect(DB_PATH, timeout=60, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode: allows concurrent reads while writing
    conn.execute("PRAGMA journal_mode=WAL")
    # NORMAL sync: safe enough for this use-case, faster than FULL
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Public API ─────────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create tables and indexes if they don't exist yet."""
    with get_conn() as conn:
        conn.executescript(SCHEMA)
    logger.info("Database ready: %s", DB_PATH)


def get_last_timestamp() -> int | None:
    """Return the most recent window epoch stored, or None if the table is empty."""
    with get_conn() as conn:
        row = conn.execute("SELECT MAX(timestamp) FROM vitals").fetchone()
        return row[0]


def get_row_count() -> int:
    with get_conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM vitals").fetchone()[0]


_UPSERT_SQL = """
INSERT INTO vitals (
    timestamp, targetGroupedUrl, deviceType, connectionType, navigationType,
    largestContentfulPaint, lcp_p75, lcp_p90, lcp_p95,
    cumulativeLayoutShift,  cls_p75, cls_p90, cls_p95,
    interactionToNextPaint, inp_p75, inp_p90, inp_p95,
    firstContentfulPaint,   fcp_p75, fcp_p90, fcp_p95,
    timeToFirstByte, ttfb_p75, ttfb_p90, ttfb_p95,
    firstPaint, windowLoad, elementSize,
    sample_count
) VALUES (
    :timestamp, :targetGroupedUrl, :deviceType, :connectionType, :navigationType,
    :largestContentfulPaint, :lcp_p75, :lcp_p90, :lcp_p95,
    :cumulativeLayoutShift,  :cls_p75, :cls_p90, :cls_p95,
    :interactionToNextPaint, :inp_p75, :inp_p90, :inp_p95,
    :firstContentfulPaint,   :fcp_p75, :fcp_p90, :fcp_p95,
    :timeToFirstByte, :ttfb_p75, :ttfb_p90, :ttfb_p95,
    :firstPaint, :windowLoad, :elementSize,
    :sample_count
)
ON CONFLICT (timestamp, targetGroupedUrl, deviceType, connectionType, navigationType)
DO UPDATE SET
    largestContentfulPaint  = excluded.largestContentfulPaint,
    lcp_p75                 = excluded.lcp_p75,
    lcp_p90                 = excluded.lcp_p90,
    lcp_p95                 = excluded.lcp_p95,
    cumulativeLayoutShift   = excluded.cumulativeLayoutShift,
    cls_p75                 = excluded.cls_p75,
    cls_p90                 = excluded.cls_p90,
    cls_p95                 = excluded.cls_p95,
    interactionToNextPaint  = excluded.interactionToNextPaint,
    inp_p75                 = excluded.inp_p75,
    inp_p90                 = excluded.inp_p90,
    inp_p95                 = excluded.inp_p95,
    firstContentfulPaint    = excluded.firstContentfulPaint,
    fcp_p75                 = excluded.fcp_p75,
    fcp_p90                 = excluded.fcp_p90,
    fcp_p95                 = excluded.fcp_p95,
    timeToFirstByte         = excluded.timeToFirstByte,
    ttfb_p75                = excluded.ttfb_p75,
    ttfb_p90                = excluded.ttfb_p90,
    ttfb_p95                = excluded.ttfb_p95,
    firstPaint              = excluded.firstPaint,
    windowLoad              = excluded.windowLoad,
    elementSize             = excluded.elementSize,
    sample_count            = excluded.sample_count
"""


def upsert_vitals(records: list[dict]) -> int:
    """
    Insert or update a batch of vitals records.
    Duplicate (timestamp + dimensions) rows are updated in-place.
    Returns the number of records processed.
    """
    if not records:
        return 0
    with get_conn() as conn:
        conn.executemany(_UPSERT_SQL, records)
    return len(records)


# ── Browser-level data ────────────────────────────────────────────────────────

_UPSERT_BROWSER_SQL = """
INSERT INTO vitals_browser (
    timestamp, targetGroupedUrl, userAgentName, userAgentVersion,
    largestContentfulPaint, lcp_p75,
    cumulativeLayoutShift,  cls_p75,
    interactionToNextPaint, inp_p75,
    firstContentfulPaint,   fcp_p75,
    timeToFirstByte, ttfb_p75,
    sample_count
) VALUES (
    :timestamp, :targetGroupedUrl, :userAgentName, :userAgentVersion,
    :largestContentfulPaint, :lcp_p75,
    :cumulativeLayoutShift,  :cls_p75,
    :interactionToNextPaint, :inp_p75,
    :firstContentfulPaint,   :fcp_p75,
    :timeToFirstByte, :ttfb_p75,
    :sample_count
)
ON CONFLICT (timestamp, targetGroupedUrl, userAgentName, userAgentVersion)
DO UPDATE SET
    largestContentfulPaint  = excluded.largestContentfulPaint,
    lcp_p75                 = excluded.lcp_p75,
    cumulativeLayoutShift   = excluded.cumulativeLayoutShift,
    cls_p75                 = excluded.cls_p75,
    interactionToNextPaint  = excluded.interactionToNextPaint,
    inp_p75                 = excluded.inp_p75,
    firstContentfulPaint    = excluded.firstContentfulPaint,
    fcp_p75                 = excluded.fcp_p75,
    timeToFirstByte         = excluded.timeToFirstByte,
    ttfb_p75                = excluded.ttfb_p75,
    sample_count            = excluded.sample_count
"""


_UPSERT_URL_SQL = """
INSERT INTO vitals_url (
    timestamp, targetGroupedUrl,
    largestContentfulPaint, lcp_p75, lcp_p90, lcp_p95,
    cumulativeLayoutShift,  cls_p75, cls_p90, cls_p95,
    interactionToNextPaint, inp_p75, inp_p90, inp_p95,
    firstContentfulPaint,   fcp_p75, fcp_p90, fcp_p95,
    timeToFirstByte, ttfb_p75, ttfb_p90, ttfb_p95,
    firstPaint, windowLoad,
    sample_count
) VALUES (
    :timestamp, :targetGroupedUrl,
    :largestContentfulPaint, :lcp_p75, :lcp_p90, :lcp_p95,
    :cumulativeLayoutShift,  :cls_p75, :cls_p90, :cls_p95,
    :interactionToNextPaint, :inp_p75, :inp_p90, :inp_p95,
    :firstContentfulPaint,   :fcp_p75, :fcp_p90, :fcp_p95,
    :timeToFirstByte, :ttfb_p75, :ttfb_p90, :ttfb_p95,
    :firstPaint, :windowLoad,
    :sample_count
)
ON CONFLICT (timestamp, targetGroupedUrl)
DO UPDATE SET
    largestContentfulPaint  = excluded.largestContentfulPaint,
    lcp_p75                 = excluded.lcp_p75,
    lcp_p90                 = excluded.lcp_p90,
    lcp_p95                 = excluded.lcp_p95,
    cumulativeLayoutShift   = excluded.cumulativeLayoutShift,
    cls_p75                 = excluded.cls_p75,
    cls_p90                 = excluded.cls_p90,
    cls_p95                 = excluded.cls_p95,
    interactionToNextPaint  = excluded.interactionToNextPaint,
    inp_p75                 = excluded.inp_p75,
    inp_p90                 = excluded.inp_p90,
    inp_p95                 = excluded.inp_p95,
    firstContentfulPaint    = excluded.firstContentfulPaint,
    fcp_p75                 = excluded.fcp_p75,
    fcp_p90                 = excluded.fcp_p90,
    fcp_p95                 = excluded.fcp_p95,
    timeToFirstByte         = excluded.timeToFirstByte,
    ttfb_p75                = excluded.ttfb_p75,
    ttfb_p90                = excluded.ttfb_p90,
    ttfb_p95                = excluded.ttfb_p95,
    firstPaint              = excluded.firstPaint,
    windowLoad              = excluded.windowLoad,
    sample_count            = excluded.sample_count
"""


def upsert_url_vitals(records: list[dict]) -> int:
    """Insert or update URL-level vitals records (no device/connection split)."""
    if not records:
        return 0
    with get_conn() as conn:
        conn.executemany(_UPSERT_URL_SQL, records)
    return len(records)


def upsert_browser_vitals(records: list[dict]) -> int:
    """Insert or update browser-level vitals records."""
    if not records:
        return 0
    with get_conn() as conn:
        conn.executemany(_UPSERT_BROWSER_SQL, records)
    return len(records)
