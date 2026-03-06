"""Page URLs tab: priority pages pinned to top, then worst by chosen metric."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ..constants import CWV_BG, CWV_COLOR, PINNED_URL_PATHS, THRESHOLDS
from ..formatters import normalize_url_series, weighted_mean_grouped

_PAGE_SIZE_OPTIONS = [50, 100, 200]


@st.fragment
def tab_top_pages(df: pd.DataFrame, url_df: pd.DataFrame | None = None) -> None:
    if df.empty:
        st.info("No data for the selected filters.")
        return

    col_sort, col_n = st.columns([3, 1])
    sort_metric = col_sort.selectbox(
        "Sort by",
        ["LCP", "CLS", "INP", "FCP", "TTFB"],
        key="top_sort",
    )
    n = col_n.selectbox(
        "Show",
        options=_PAGE_SIZE_OPTIONS,
        index=0,
        key="top_n",
    )
    sort_col = f"avg_{sort_metric.lower()}"

    st.subheader("Page URLs")

    # Prefer url_df (accurate overall percentiles) when available.
    src = url_df if (url_df is not None and not url_df.empty) else df

    # Vectorized weighted aggregation — single groupby for all metrics
    metric_cols = [
        "largestContentfulPaint", "lcp_p75",
        "cumulativeLayoutShift", "cls_p75",
        "interactionToNextPaint", "inp_p75",
        "firstContentfulPaint",
        "timeToFirstByte", "ttfb_p75",
    ]

    agg = weighted_mean_grouped(src, "url_group", metric_cols)
    if agg.empty:
        st.info("Not enough data.")
        return

    views = src.groupby("url_group")["sample_count"].sum()
    agg["total_views"] = views

    # Rename to display names
    rename = {
        "largestContentfulPaint": "avg_lcp", "lcp_p75": "p75_lcp",
        "cumulativeLayoutShift": "avg_cls", "cls_p75": "p75_cls",
        "interactionToNextPaint": "avg_inp", "inp_p75": "p75_inp",
        "firstContentfulPaint": "avg_fcp",
        "timeToFirstByte": "avg_ttfb", "ttfb_p75": "p75_ttfb",
    }
    agg = agg.rename(columns=rename).reset_index().rename(columns={"url_group": "URL"})

    # Pin priority URLs to top
    pinned_index = {p: i for i, p in enumerate(PINNED_URL_PATHS)}
    agg["_path"] = normalize_url_series(agg["URL"])
    agg["_pin"]  = agg["_path"].map(pinned_index)

    pinned_df = (
        agg[agg["_pin"].notna()]
        .sort_values("_pin")
        .drop(columns=["_path", "_pin"])
    )
    other_df = (
        agg[agg["_pin"].isna()]
        .drop(columns=["_path", "_pin"])
    )
    if sort_col in other_df.columns:
        other_df = other_df.sort_values(sort_col, ascending=False)

    remaining = max(0, n - len(pinned_df))
    result = pd.concat([pinned_df, other_df.head(remaining)], ignore_index=True)

    # Table with threshold color-coding
    styled = _style_table(_format_table(result))
    st.dataframe(
        styled,
        column_config={
            "URL":         st.column_config.TextColumn("URL", width="large"),
            "avg_lcp":     st.column_config.NumberColumn("LCP avg (ms)"),
            "p75_lcp":     st.column_config.NumberColumn("LCP p75 (ms)"),
            "avg_cls":     st.column_config.NumberColumn("CLS avg"),
            "p75_cls":     st.column_config.NumberColumn("CLS p75"),
            "avg_inp":     st.column_config.NumberColumn("INP avg (ms)"),
            "p75_inp":     st.column_config.NumberColumn("INP p75 (ms)"),
            "avg_fcp":     st.column_config.NumberColumn("FCP avg (ms)"),
            "avg_ttfb":    st.column_config.NumberColumn("TTFB avg (ms)"),
            "p75_ttfb":    st.column_config.NumberColumn("TTFB p75 (ms)"),
            "total_views": st.column_config.NumberColumn("Views"),
        },
        hide_index=True,
        width="stretch",
    )

    # Bar chart
    if sort_col in result.columns:
        fig = px.bar(
            result.sort_values(sort_col),
            x=sort_col,
            y="URL",
            orientation="h",
            title=f"Avg {sort_metric} — Page URLs",
            height=max(300, len(result) * 28 + 80),
            color=sort_col,
            color_continuous_scale="RdYlGn_r",
        )
        fig.update_layout(
            margin=dict(t=40, b=20, l=0, r=0),
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, width="stretch")

    # Quick dive
    st.markdown("---")
    dive = st.selectbox(
        "Deep dive into page:",
        options=["—"] + result["URL"].tolist(),
        key="top_pages_dive",
    )
    if dive != "—":
        st.session_state["dive_url"] = dive
        st.info("Switch to the **Page Analysis** tab to see the full breakdown.")


# Map display columns → threshold key
_COL_THRESHOLD: dict[str, str] = {
    "avg_lcp": "lcp", "p75_lcp": "lcp",
    "avg_cls": "cls", "p75_cls": "cls",
    "avg_inp": "inp", "p75_inp": "inp",
    "avg_fcp": "fcp",
    "avg_ttfb": "ttfb", "p75_ttfb": "ttfb",
}


def _cwv_status(value: float, metric_key: str) -> str:
    """Return 'good', 'needs_improvement', or 'poor' for a value."""
    t = THRESHOLDS.get(metric_key)
    if t is None or pd.isna(value):
        return ""
    if value <= t["good"]:
        return "good"
    if value <= t["poor"]:
        return "needs_improvement"
    return "poor"


def _style_table(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Apply green/yellow/red background to metric cells based on CWV thresholds."""
    def _apply(col: pd.Series) -> list[str]:
        key = _COL_THRESHOLD.get(col.name, "")
        if not key:
            return [""] * len(col)
        styles = []
        for v in col:
            status = _cwv_status(v, key)
            if status:
                styles.append(
                    f"background-color: {CWV_BG[status]}; color: {CWV_COLOR[status]}"
                )
            else:
                styles.append("")
        return styles

    fmt: dict[str, str] = {}
    for c in df.columns:
        if c in ("avg_cls", "p75_cls"):
            fmt[c] = "{:.3f}"
        elif c == "total_views":
            fmt[c] = "{:,.0f}"
        elif c in _COL_THRESHOLD:
            fmt[c] = "{:.0f}"
    return df.style.apply(_apply).format(fmt, na_rep="None")


def _format_table(agg: pd.DataFrame) -> pd.DataFrame:
    display = agg.copy()
    for c in ["avg_lcp", "p75_lcp", "avg_inp", "p75_inp", "avg_fcp",
              "avg_ttfb", "p75_ttfb"]:
        if c in display.columns:
            display[c] = pd.to_numeric(display[c], errors="coerce").round(0)
    for c in ["avg_cls", "p75_cls"]:
        if c in display.columns:
            display[c] = pd.to_numeric(display[c], errors="coerce").round(3)
    if "total_views" in display.columns:
        display["total_views"] = pd.to_numeric(display["total_views"], errors="coerce").fillna(0).astype(int)
    return display
