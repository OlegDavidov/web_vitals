"""Sidebar filter controls."""
from __future__ import annotations

from datetime import datetime, time, timedelta, timezone

import streamlit as st

from .constants import PINNED_URL_PATHS


def render_sidebar(opts: dict) -> dict:
    """
    Render sidebar date-range + filter controls.
    Returns a dict of resolved filter values ready to pass to load_vitals().
    """
    st.sidebar.title("Filters")

    # ── Date range ────────────────────────────────────────────────────────────
    now = datetime.now(timezone.utc).date()
    default_start = now - timedelta(days=7)

    if opts["min_ts"] and opts["max_ts"]:
        min_date = datetime.fromtimestamp(opts["min_ts"], tz=timezone.utc).date()
        max_date = datetime.fromtimestamp(opts["max_ts"], tz=timezone.utc).date()
    else:
        min_date = now - timedelta(days=365)
        max_date = now

    # Clamp defaults to the actual data range so Streamlit doesn't raise
    default_start = max(default_start, min_date)
    default_end   = min(now, max_date)

    col1, col2 = st.sidebar.columns(2)
    start_date = col1.date_input("From", value=default_start,
                                 min_value=min_date, max_value=max_date)
    end_date   = col2.date_input("To",   value=default_end,
                                 min_value=min_date, max_value=max_date)

    # ── Time range ────────────────────────────────────────────────────────────
    col3, col4 = st.sidebar.columns(2)
    start_time = col3.time_input("Time from", value=time(0, 0, 0),  step=3600)
    end_time   = col4.time_input("Time to",   value=time(23, 59, 59), step=3600)

    start_ts = int(datetime(start_date.year, start_date.month, start_date.day,
                            start_time.hour, start_time.minute, start_time.second,
                            tzinfo=timezone.utc).timestamp())
    end_ts   = int(datetime(end_date.year, end_date.month, end_date.day,
                            end_time.hour, end_time.minute, end_time.second,
                            tzinfo=timezone.utc).timestamp())

    # ── Dimension filters ─────────────────────────────────────────────────────
    st.sidebar.markdown("---")
    device     = st.sidebar.selectbox("Device",  ["All"] + opts["devices"])
    connection = st.sidebar.selectbox("Network", ["All"] + opts["connections"])

    # ── URL filter ────────────────────────────────────────────────────────────
    st.sidebar.markdown("---")
    quick_options = ["— All —"] + PINNED_URL_PATHS
    quick_url = st.sidebar.selectbox("Quick URL", quick_options, index=0)
    url_filter = st.sidebar.text_input("URL contains",
                                       placeholder="/product, /checkout …")

    # Quick URL takes precedence over text input when selected
    effective_url = quick_url if quick_url != "— All —" else url_filter.strip()

    return {
        "start_ts":   start_ts,
        "end_ts":     end_ts,
        "device":     "" if device     == "All" else device,
        "connection": "" if connection == "All" else connection,
        "url_filter": effective_url,
        "urls":       opts["urls"],
    }
