"""Data access layer: SQLite queries with Streamlit caching."""
from __future__ import annotations

import logging
import sqlite3

import pandas as pd
import streamlit as st

from config import DB_PATH
from .formatters import normalize_url_series

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-query read-only connections (thread-safe for concurrent users)
# ---------------------------------------------------------------------------

def _get_conn() -> sqlite3.Connection:
    """Open a fresh read-only WAL-mode connection for a single query."""
    conn = sqlite3.connect(
        f"file:{DB_PATH}?mode=ro",
        uri=True,
        timeout=60,
    )
    conn.execute("PRAGMA query_only=ON")
    # busy_timeout: wait up to 10s if writer holds a lock (cron update running)
    # instead of failing immediately — important for concurrent dashboard users.
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


# ---------------------------------------------------------------------------
# Column lists (avoid SELECT *)
# ---------------------------------------------------------------------------

_VITALS_COLS = (
    "timestamp, targetGroupedUrl, deviceType, connectionType,"
    "largestContentfulPaint, lcp_p75, lcp_p90, lcp_p95,"
    "cumulativeLayoutShift, cls_p75, cls_p90, cls_p95,"
    "interactionToNextPaint, inp_p75, inp_p90, inp_p95,"
    "firstContentfulPaint, fcp_p75, fcp_p90, fcp_p95,"
    "timeToFirstByte, ttfb_p75, ttfb_p90, ttfb_p95,"
    "firstPaint, windowLoad, elementSize, sample_count"
)

_URL_COLS = (
    "timestamp, targetGroupedUrl,"
    "largestContentfulPaint, lcp_p75, lcp_p90, lcp_p95,"
    "cumulativeLayoutShift, cls_p75, cls_p90, cls_p95,"
    "interactionToNextPaint, inp_p75, inp_p90, inp_p95,"
    "firstContentfulPaint, fcp_p75, fcp_p90, fcp_p95,"
    "timeToFirstByte, ttfb_p75, ttfb_p90, ttfb_p95,"
    "firstPaint, windowLoad, sample_count"
)

_BROWSER_COLS = (
    "timestamp, targetGroupedUrl, userAgentName, userAgentVersion,"
    "largestContentfulPaint, lcp_p75,"
    "cumulativeLayoutShift, cls_p75,"
    "interactionToNextPaint, inp_p75,"
    "firstContentfulPaint, fcp_p75,"
    "timeToFirstByte, ttfb_p75,"
    "sample_count"
)

# ---------------------------------------------------------------------------
# Unit conversion & cleanup
# ---------------------------------------------------------------------------

# Columns stored in seconds by NR -> converted to ms on load.
# TTFB is already stored in ms by NR and must NOT be multiplied.
_SECONDS_COLS = [
    "largestContentfulPaint", "lcp_p75", "lcp_p90", "lcp_p95",
    "firstContentfulPaint",   "fcp_p75", "fcp_p90", "fcp_p95",
    "interactionToNextPaint", "inp_p75", "inp_p90", "inp_p95",
    "firstPaint",
    "windowLoad",
]

# Mapping from avg column to its percentile columns.
_AVG_TO_PCTS = {
    "interactionToNextPaint": ("inp_p75", "inp_p90", "inp_p95"),
    "firstContentfulPaint":   ("fcp_p75", "fcp_p90", "fcp_p95"),
    "cumulativeLayoutShift":  ("cls_p75", "cls_p90", "cls_p95"),
    "timeToFirstByte":        ("ttfb_p75", "ttfb_p90", "ttfb_p95"),
    "largestContentfulPaint": ("lcp_p75", "lcp_p90", "lcp_p95"),
}

# INP outlier thresholds (in seconds, before ms conversion)
_INP_AVG_OUTLIER = 30
_INP_PCT_OUTLIER = 10


def _clean_vitals_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply common cleanup to any vitals DataFrame (in-place, returns df)."""
    # 1. Convert all relevant columns to numeric once (avoids duplicate to_numeric)
    all_numeric: set[str] = set(_SECONDS_COLS)
    for pcts in _AVG_TO_PCTS.values():
        all_numeric.update(pcts)
    all_numeric.update(("timeToFirstByte", "ttfb_p75", "ttfb_p90", "ttfb_p95"))
    present_numeric = [c for c in all_numeric if c in df.columns]
    if present_numeric:
        df[present_numeric] = df[present_numeric].apply(pd.to_numeric, errors="coerce")

    # 2. Nullify fake-zero percentiles where avg is NaN (NR percentile() bug)
    for avg_col, pct_cols in _AVG_TO_PCTS.items():
        if avg_col in df.columns:
            null_mask = df[avg_col].isna()
            if null_mask.any():
                for pc in pct_cols:
                    if pc in df.columns:
                        df.loc[null_mask, pc] = None

    # 3. INP outlier cleanup (already numeric from step 1)
    if "interactionToNextPaint" in df.columns:
        df.loc[df["interactionToNextPaint"] > _INP_AVG_OUTLIER, "interactionToNextPaint"] = None
    for col in ("inp_p75", "inp_p90", "inp_p95"):
        if col in df.columns:
            df.loc[df[col] > _INP_PCT_OUTLIER, col] = None

    # 4. Convert seconds -> milliseconds for NR timing metrics
    present_secs = [c for c in _SECONDS_COLS if c in df.columns]
    if present_secs:
        df[present_secs] *= 1000

    return df


# ---------------------------------------------------------------------------
# LIKE escaping helper
# ---------------------------------------------------------------------------

def _escape_like(s: str) -> str:
    """Escape SQL LIKE wildcards so user input is treated literally."""
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _add_url_filter(conditions: list[str], params: dict, url_filter: str) -> None:
    """Append a LIKE condition with proper escaping if url_filter is non-empty."""
    if url_filter:
        conditions.append("targetGroupedUrl LIKE :url ESCAPE '\\'")
        params["url"] = f"%{_escape_like(url_filter)}%"


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300, max_entries=32)
def load_vitals(
    start_ts: int,
    end_ts: int,
    device: str = "",
    connection: str = "",
    url_filter: str = "",
) -> pd.DataFrame:
    """Return vitals rows matching the given filters."""
    conditions = ["timestamp BETWEEN :start AND :end"]
    params: dict = {"start": start_ts, "end": end_ts}

    if device:
        conditions.append("deviceType = :device")
        params["device"] = device
    if connection:
        conditions.append("connectionType = :connection")
        params["connection"] = connection
    _add_url_filter(conditions, params, url_filter)

    # Safety limit: cap result set to avoid OOM on very large date ranges.
    # 500K rows ≈ ~40 days at 12K rows/day — more than enough for any dashboard view.
    _ROW_LIMIT = 500_000
    sql = (
        f"SELECT {_VITALS_COLS} FROM vitals"
        f" WHERE {' AND '.join(conditions)}"
        f" ORDER BY timestamp LIMIT {_ROW_LIMIT}"
    )

    try:
        conn = _get_conn()
    except Exception:
        logger.exception("Failed to open DB for vitals query")
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        logger.exception("Failed to load vitals")
        return pd.DataFrame()
    finally:
        conn.close()

    if len(df) >= _ROW_LIMIT:
        logger.warning("load_vitals hit row limit (%d); results truncated", _ROW_LIMIT)

    if not df.empty:
        df["datetime"]  = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["url_group"] = normalize_url_series(df["targetGroupedUrl"])
        _clean_vitals_df(df)

    return df


@st.cache_data(ttl=600, max_entries=4)
def load_filter_options() -> dict:
    """Return distinct filter values and the DB timestamp range (single query)."""
    empty = {"devices": [], "connections": [], "urls": [], "min_ts": None, "max_ts": None}
    try:
        conn = _get_conn()
    except Exception:
        logger.warning("Cannot open DB for filter options (DB may not exist yet)")
        return empty

    try:
        sql = """
            SELECT 'device' AS kind, deviceType AS val FROM vitals
                WHERE deviceType != '' GROUP BY deviceType
            UNION ALL
            SELECT 'conn', connectionType FROM vitals
                WHERE connectionType != '' GROUP BY connectionType
            UNION ALL
            SELECT 'ts_min', CAST(MIN(timestamp) AS TEXT) FROM vitals
            UNION ALL
            SELECT 'ts_max', CAST(MAX(timestamp) AS TEXT) FROM vitals
        """
        rows = conn.execute(sql).fetchall()

        devices: list[str] = []
        connections: list[str] = []
        min_ts = None
        max_ts = None
        for kind, val in rows:
            if kind == "device":
                devices.append(val)
            elif kind == "conn":
                connections.append(val)
            elif kind == "ts_min" and val is not None:
                min_ts = int(val)
            elif kind == "ts_max" and val is not None:
                max_ts = int(val)

        devices.sort()
        connections.sort()

        # Use vitals_url (much smaller table) for the URL list when available;
        # fall back to vitals if vitals_url doesn't exist yet.
        try:
            urls = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT targetGroupedUrl FROM vitals_url "
                    "WHERE targetGroupedUrl != '' ORDER BY 1"
                ).fetchall()
            ]
        except sqlite3.OperationalError:
            urls = [
                r[0] for r in conn.execute(
                    "SELECT DISTINCT targetGroupedUrl FROM vitals "
                    "WHERE targetGroupedUrl != '' ORDER BY 1"
                ).fetchall()
            ]
    except Exception:
        logger.exception("Failed to load filter options")
        return empty
    finally:
        conn.close()

    return {
        "devices":     devices,
        "connections": connections,
        "urls":        urls,
        "min_ts":      min_ts,
        "max_ts":      max_ts,
    }


@st.cache_data(ttl=300, max_entries=16)
def load_browser_vitals(
    start_ts: int,
    end_ts: int,
    url_filter: str = "",
) -> pd.DataFrame:
    """Return browser-level vitals rows matching the given filters."""
    conditions = ["timestamp BETWEEN :start AND :end"]
    params: dict = {"start": start_ts, "end": end_ts}
    _add_url_filter(conditions, params, url_filter)

    _ROW_LIMIT = 500_000
    sql = (
        f"SELECT {_BROWSER_COLS} FROM vitals_browser"
        f" WHERE {' AND '.join(conditions)}"
        f" ORDER BY timestamp LIMIT {_ROW_LIMIT}"
    )

    try:
        conn = _get_conn()
    except Exception:
        logger.exception("Failed to open DB for browser vitals query")
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        logger.exception("Failed to load browser vitals")
        return pd.DataFrame()
    finally:
        conn.close()

    if not df.empty:
        df["url_group"] = normalize_url_series(df["targetGroupedUrl"])
        name = df["userAgentName"].fillna("")
        ver  = df["userAgentVersion"].fillna("")
        df["browser"] = (name + " " + ver).str.strip()
        _clean_vitals_df(df)

    return df


@st.cache_data(ttl=300, max_entries=32)
def load_url_vitals(
    start_ts: int,
    end_ts: int,
    url_filter: str = "",
) -> pd.DataFrame:
    """Return URL-level vitals (accurate overall percentiles, no device split)."""
    conditions = ["timestamp BETWEEN :start AND :end"]
    params: dict = {"start": start_ts, "end": end_ts}
    _add_url_filter(conditions, params, url_filter)

    _ROW_LIMIT = 500_000
    sql = (
        f"SELECT {_URL_COLS} FROM vitals_url"
        f" WHERE {' AND '.join(conditions)}"
        f" ORDER BY timestamp LIMIT {_ROW_LIMIT}"
    )

    try:
        conn = _get_conn()
    except Exception:
        logger.exception("Failed to open DB for URL vitals query")
        return pd.DataFrame()

    try:
        df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        logger.exception("Failed to load URL vitals")
        return pd.DataFrame()
    finally:
        conn.close()

    if not df.empty:
        df["datetime"]  = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["url_group"] = normalize_url_series(df["targetGroupedUrl"])
        _clean_vitals_df(df)

    return df


@st.cache_data(ttl=300, max_entries=4)
def db_has_data() -> bool:
    try:
        conn = _get_conn()
    except Exception:
        logger.warning("Cannot open DB for has_data check (DB may not exist yet)")
        return False

    try:
        return conn.execute("SELECT 1 FROM vitals LIMIT 1").fetchone() is not None
    except Exception:
        logger.exception("db_has_data check failed")
        return False
    finally:
        conn.close()
