"""Main Streamlit application: page config, sidebar, tabs wiring."""
from __future__ import annotations

import logging
import platform
import socket
from datetime import datetime, timezone

import streamlit as st

from .data import db_has_data, load_filter_options, load_url_vitals, load_vitals
from .sidebar import render_sidebar
from .tabs.overview import tab_overview
from .tabs.breakdowns import tab_breakdowns
from .tabs.top_pages import tab_top_pages
from .tabs.page_analysis import tab_page_analysis

logger = logging.getLogger(__name__)


@st.cache_resource
def _host_info() -> str:
    """Return distro name and local LAN IP."""
    # OS / distro
    try:
        info = platform.freedesktop_os_release()
        distro = info.get("PRETTY_NAME", info.get("NAME", platform.system()))
    except OSError:
        distro = platform.system()

    # LAN IP
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            lan_ip = s.getsockname()[0]
        finally:
            s.close()
    except Exception:
        lan_ip = "N/A"

    return f"{distro} | LAN {lan_ip}"


def main() -> None:
    st.set_page_config(
        page_title="Web Vitals Dashboard",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Web Vitals Dashboard")

    opts    = load_filter_options()

    if opts["max_ts"]:
        last_dt = datetime.fromtimestamp(opts["max_ts"], tz=timezone.utc)
        st.caption(f"Last data in DB: **{last_dt.strftime('%Y-%m-%d %H:%M')} UTC** ({_host_info()})")

    if not db_has_data():
        st.warning(
            "No data found in the database. "
            "Run `python manage.py init` to populate it."
        )
        st.stop()

    filters = render_sidebar(opts)

    with st.spinner("Loading data..."):
        df = load_vitals(
            start_ts=filters["start_ts"],
            end_ts=filters["end_ts"],
            device=filters["device"],
            connection=filters["connection"],
            url_filter=filters["url_filter"],
        )

        # ── URL-level vitals (accurate overall percentiles) ──────────────────
        # vitals_url has no device/connection columns, so when those filters are
        # active the url_df cannot reflect them — fall back to faceted df.
        dimension_filter_active = bool(filters["device"] or filters["connection"])
        if dimension_filter_active:
            url_df = None
        else:
            url_df = load_url_vitals(
                start_ts=filters["start_ts"],
                end_ts=filters["end_ts"],
                url_filter=filters["url_filter"],
            )

    if len(df) >= 500_000:
        st.warning(
            "Result set was capped at 500 000 rows. "
            "Narrow the date range or add filters for complete data."
        )

    # ── Summary bar ───────────────────────────────────────────────────────────
    if not df.empty:
        c = st.columns(4)
        c[0].metric("Rows loaded",  f"{len(df):,}")
        c[1].metric("Unique URLs",  df["url_group"].nunique())
        c[2].metric(
            "Date range",
            f"{df['datetime'].dt.date.min()} → {df['datetime'].dt.date.max()}",
        )
        c[3].metric("Total views",  f"{df['sample_count'].sum():,.0f}")

    # ── Tabs ──────────────────────────────────────────────────────────────────
    tabs = st.tabs(["Overview", "Breakdowns", "Page URLs", "Page Analysis"])

    _tab_handlers = [
        (tabs[0], "Overview",      lambda: tab_overview(df, url_df=url_df)),
        (tabs[1], "Breakdowns",    lambda: tab_breakdowns(df)),
        (tabs[2], "Page URLs",     lambda: tab_top_pages(df, url_df=url_df)),
        (tabs[3], "Page Analysis", lambda: tab_page_analysis(df, opts["urls"], filters=filters, url_df=url_df)),
    ]
    for tab, name, handler in _tab_handlers:
        with tab:
            try:
                handler()
            except Exception:
                logger.exception("Error rendering '%s' tab", name)
                st.error(f"An error occurred while rendering the {name} tab. Check the logs for details.")
