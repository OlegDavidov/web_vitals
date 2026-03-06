"""Data access layer: SQLite queries with Streamlit caching."""
from __future__ import annotations

import logging
import sqlite3
from contextlib import contextmanager
from collections.abc import Generator

import pandas as pd
import streamlit as st

from config import DB_PATH
from .formatters import normalize_url

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Read-only connection (no commit overhead, safe for concurrent dashboard use)
# ---------------------------------------------------------------------------

@contextmanager
def _ro_conn() -> Generator[sqlite3.Connection, None, None]:
    """Read-only WAL-mode connection for dashboard queries."""
    conn = sqlite3.connect(
        f"file:{DB_PATH}?mode=ro",
        uri=True,
        timeout=60,
        check_same_thread=False,
    )
    conn.execute("PRAGMA query_only=ON")
    try:
        yield conn
    finally:
        conn.close()


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


def _clean_vitals_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply common cleanup to any vitals DataFrame (in-place, returns df)."""
    # Nullify fake-zero percentiles where avg is NaN (NR percentile() bug)
    for avg_col, pct_cols in _AVG_TO_PCTS.items():
        if avg_col in df.columns:
            null_mask = pd.to_numeric(df[avg_col], errors="coerce").isna()
            if null_mask.any():
                for pc in pct_cols:
                    if pc in df.columns:
                        df.loc[null_mask, pc] = None

    # INP outlier cleanup
    if "interactionToNextPaint" in df.columns:
        df["interactionToNextPaint"] = pd.to_numeric(df["interactionToNextPaint"], errors="coerce")
        df.loc[df["interactionToNextPaint"] > 30, "interactionToNextPaint"] = None
    for col in ("inp_p75", "inp_p90", "inp_p95"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df.loc[df[col] > 10, col] = None

    # Convert seconds -> milliseconds for NR timing metrics
    present = [c for c in _SECONDS_COLS if c in df.columns]
    if present:
        df[present] = df[present].apply(pd.to_numeric, errors="coerce") * 1000

    return df


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=300)
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
    if url_filter:
        conditions.append("targetGroupedUrl LIKE :url")
        params["url"] = f"%{url_filter}%"

    sql = f"SELECT {_VITALS_COLS} FROM vitals WHERE {' AND '.join(conditions)} ORDER BY timestamp"

    with _ro_conn() as conn:
        df = pd.read_sql_query(sql, conn, params=params)

    if not df.empty:
        df["datetime"]  = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["url_group"] = df["targetGroupedUrl"].apply(normalize_url)
        _clean_vitals_df(df)

    return df


@st.cache_data(ttl=600)
def load_filter_options() -> dict:
    """Return distinct filter values and the DB timestamp range."""
    with _ro_conn() as conn:
        def _distinct(col: str, table: str = "vitals") -> list[str]:
            return [
                r[0]
                for r in conn.execute(
                    f"SELECT DISTINCT {col} FROM {table} WHERE {col} != '' ORDER BY 1"
                ).fetchall()
            ]

        devices     = _distinct("deviceType")
        connections = _distinct("connectionType")
        urls        = _distinct("targetGroupedUrl")
        ts_range    = conn.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM vitals"
        ).fetchone()

    return {
        "devices":     devices,
        "connections": connections,
        "urls":        urls,
        "min_ts":      ts_range[0],
        "max_ts":      ts_range[1],
    }


@st.cache_data(ttl=300)
def load_browser_vitals(
    start_ts: int,
    end_ts: int,
    url_filter: str = "",
) -> pd.DataFrame:
    """Return browser-level vitals rows matching the given filters."""
    conditions = ["timestamp BETWEEN :start AND :end"]
    params: dict = {"start": start_ts, "end": end_ts}

    if url_filter:
        conditions.append("targetGroupedUrl LIKE :url")
        params["url"] = f"%{url_filter}%"

    sql = f"SELECT {_BROWSER_COLS} FROM vitals_browser WHERE {' AND '.join(conditions)} ORDER BY timestamp"

    try:
        with _ro_conn() as conn:
            df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        logger.exception("Failed to load browser vitals")
        return pd.DataFrame()

    if not df.empty:
        df["url_group"] = df["targetGroupedUrl"].apply(normalize_url)
        df["browser"] = df["userAgentName"].fillna("") + ", " + df["userAgentVersion"].fillna("")
        df["browser"] = df["browser"].str.strip(", ")
        _clean_vitals_df(df)

    return df


@st.cache_data(ttl=300)
def load_url_vitals(
    start_ts: int,
    end_ts: int,
    url_filter: str = "",
) -> pd.DataFrame:
    """Return URL-level vitals (accurate overall percentiles, no device split)."""
    conditions = ["timestamp BETWEEN :start AND :end"]
    params: dict = {"start": start_ts, "end": end_ts}

    if url_filter:
        conditions.append("targetGroupedUrl LIKE :url")
        params["url"] = f"%{url_filter}%"

    sql = f"SELECT {_URL_COLS} FROM vitals_url WHERE {' AND '.join(conditions)} ORDER BY timestamp"

    try:
        with _ro_conn() as conn:
            df = pd.read_sql_query(sql, conn, params=params)
    except Exception:
        logger.exception("Failed to load URL vitals")
        return pd.DataFrame()

    if not df.empty:
        df["datetime"]  = pd.to_datetime(df["timestamp"], unit="s", utc=True)
        df["url_group"] = df["targetGroupedUrl"].apply(normalize_url)
        _clean_vitals_df(df)

    return df


def db_has_data() -> bool:
    try:
        with _ro_conn() as conn:
            return conn.execute("SELECT 1 FROM vitals LIMIT 1").fetchone() is not None
    except Exception:
        return False
