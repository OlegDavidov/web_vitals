"""Page Analysis tab: deep dive into one or more URLs.

When a single URL is selected, shows NR-style breakdowns:
  - Core Web Vitals gauge cards
  - Device type breakdown (pie chart + table)
  - Browser & version breakdown (bar chart + table)
  - All-metric time series charts
"""
from __future__ import annotations

import html as html_mod

import pandas as pd
import plotly.express as px
import streamlit as st

from ..charts import time_series_chart
from ..components import cwv_gauge_card
from ..constants import CWV_BG, CWV_COLOR, THRESHOLDS
from ..formatters import (
    cwv_distribution,
    cwv_status,
    fmt_cls,
    fmt_ms,
    weighted_mean,
    weighted_mean_grouped,
)


@st.fragment
def tab_page_analysis(df: pd.DataFrame, all_urls: list[str],
                      filters: dict | None = None,
                      url_df: pd.DataFrame | None = None) -> None:
    st.subheader("Page Deep Dive")

    if df.empty:
        st.info("No data for the selected filters.")
        return

    urls_in_data = sorted(df["url_group"].dropna().unique().tolist())
    if not urls_in_data:
        st.info("No URLs in the current data set.")
        return

    # Pre-select URL if coming from Top Pages "Deep dive" selectbox
    dive_url = st.session_state.get("dive_url")
    if dive_url and dive_url in urls_in_data:
        default = [dive_url]
    else:
        default = urls_in_data[:3] if len(urls_in_data) >= 3 else urls_in_data

    selected_urls = st.multiselect(
        "Select pages to analyse",
        options=urls_in_data,
        default=default,
        key="page_urls",
    )

    if not selected_urls:
        st.info("Select at least one page.")
        return

    page_df = df[df["url_group"].isin(selected_urls)].copy()

    # ── LCP trend — all selected pages on one chart ───────────────────────────
    st.markdown("#### LCP trend per page")

    # Vectorized LCP aggregation per (datetime, url_group)
    _lcp_col = "largestContentfulPaint"
    _w_col = "sample_count"
    _sub = page_df[[_lcp_col, _w_col, "datetime", "url_group"]].dropna(subset=[_lcp_col, _w_col])
    _sub = _sub[_sub[_w_col] > 0].copy()
    _sub["_wv"] = _sub[_lcp_col] * _sub[_w_col]
    _g = _sub.groupby(["datetime", "url_group"])
    lcp_agg = (_g["_wv"].sum() / _g[_w_col].sum()).reset_index(name=_lcp_col)
    fig_lcp = px.line(
        lcp_agg,
        x="datetime",
        y="largestContentfulPaint",
        color="url_group",
        title="LCP per page over time",
        height=350,
        markers=True,
    )
    t_lcp = THRESHOLDS["lcp"]
    for y_val, color, label in [
        (t_lcp["good"], CWV_COLOR["good"], "Good threshold"),
        (t_lcp["poor"], CWV_COLOR["poor"], "Poor threshold"),
    ]:
        fig_lcp.add_hline(
            y=y_val, line_dash="dash", line_color=color,
            annotation_text=label, annotation_position="bottom right",
        )
    fig_lcp.update_layout(
        margin=dict(t=40, b=20, l=0, r=0),
        hovermode="x unified",
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig_lcp, width="stretch")

    # ── Summary table (prefer url_df for accurate p75) ──────────────────────
    has_url_data = url_df is not None and not url_df.empty
    summary_src = url_df[url_df["url_group"].isin(selected_urls)].copy() if has_url_data else page_df
    st.markdown("#### Metric summary per page")
    summary = _build_summary(summary_src)
    st.dataframe(_style_summary(summary), hide_index=True, width="stretch")

    # ── Per-URL deep dive (single selection) ──────────────────────────────────
    if len(selected_urls) == 1:
        url = selected_urls[0]
        single_url_df = page_df[page_df["url_group"] == url]
        # Use url_df for CWV cards (accurate percentiles) when available
        cwv_src = url_df[url_df["url_group"] == url] if has_url_data else single_url_df
        if cwv_src.empty:
            cwv_src = single_url_df

        # CWV gauge cards (NR style)
        _render_cwv_cards(cwv_src, url)
        st.markdown("---")

        # Device type breakdown (from faceted df — has device column)
        _render_device_breakdown(single_url_df, url)
        st.markdown("---")

        # Browser breakdown (lazy-loaded only for single-URL deep dive)
        if filters:
            from ..data import load_browser_vitals
            browser_df = load_browser_vitals(
                start_ts=filters["start_ts"],
                end_ts=filters["end_ts"],
                url_filter=filters["url_filter"],
            )
            if not browser_df.empty:
                url_browser = browser_df[browser_df["url_group"] == url]
                if not url_browser.empty:
                    _render_browser_breakdown(url_browser, url)
                    st.markdown("---")

        # All-metric time series (use url_df for accurate percentile traces)
        chart_src = url_df[url_df["url_group"] == url] if has_url_data else single_url_df
        if chart_src.empty:
            chart_src = single_url_df
        _render_single_url_charts(chart_src, url)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_summary(page_df: pd.DataFrame) -> pd.DataFrame:
    metric_cols = [
        "largestContentfulPaint", "lcp_p75", "lcp_p90",
        "cumulativeLayoutShift", "cls_p75",
        "interactionToNextPaint", "inp_p75",
        "firstContentfulPaint",
        "timeToFirstByte", "ttfb_p75",
    ]

    agg = weighted_mean_grouped(page_df, "url_group", metric_cols)
    if agg.empty:
        return pd.DataFrame()

    views = page_df.groupby("url_group")["sample_count"].sum()
    agg["Views"] = views

    rename = {
        "largestContentfulPaint": "LCP_avg", "lcp_p75": "LCP_p75", "lcp_p90": "LCP_p90",
        "cumulativeLayoutShift": "CLS_avg", "cls_p75": "CLS_p75",
        "interactionToNextPaint": "INP_avg", "inp_p75": "INP_p75",
        "firstContentfulPaint": "FCP_avg",
        "timeToFirstByte": "TTFB_avg", "ttfb_p75": "TTFB_p75",
    }
    summary = agg.rename(columns=rename).reset_index().rename(columns={"url_group": "URL"})

    for c in ["LCP_avg", "LCP_p75", "LCP_p90", "FCP_avg",
              "TTFB_avg", "TTFB_p75", "INP_avg", "INP_p75"]:
        if c in summary.columns:
            summary[c] = pd.to_numeric(summary[c], errors="coerce").round(0)
    for c in ["CLS_avg", "CLS_p75"]:
        if c in summary.columns:
            summary[c] = pd.to_numeric(summary[c], errors="coerce").round(3)
    return summary


# Map summary columns → threshold key
_SUMMARY_THRESHOLD: dict[str, str] = {
    "LCP_avg": "lcp", "LCP_p75": "lcp", "LCP_p90": "lcp",
    "CLS_avg": "cls", "CLS_p75": "cls",
    "INP_avg": "inp", "INP_p75": "inp",
    "FCP_avg": "fcp",
    "TTFB_avg": "ttfb", "TTFB_p75": "ttfb",
}


def _style_summary(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Apply green/yellow/red background to summary metric cells."""
    def _apply(col: pd.Series) -> list[str]:
        key = _SUMMARY_THRESHOLD.get(col.name, "")
        if not key:
            return [""] * len(col)
        t = THRESHOLDS[key]
        styles = []
        for v in col:
            if pd.isna(v):
                styles.append("")
            elif v <= t["good"]:
                styles.append(f"background-color: {CWV_BG['good']}; color: {CWV_COLOR['good']}")
            elif v <= t["poor"]:
                styles.append(f"background-color: {CWV_BG['needs_improvement']}; color: {CWV_COLOR['needs_improvement']}")
            else:
                styles.append(f"background-color: {CWV_BG['poor']}; color: {CWV_COLOR['poor']}")
        return styles

    fmt: dict[str, str] = {}
    for c in df.columns:
        if c in ("CLS_avg", "CLS_p75"):
            fmt[c] = "{:.3f}"
        elif c == "Views":
            fmt[c] = "{:,.0f}"
        elif c in _SUMMARY_THRESHOLD:
            fmt[c] = "{:.0f}"
    return df.style.apply(_apply).format(fmt, na_rep="None")


def _render_cwv_cards(url_df: pd.DataFrame, url: str) -> None:
    """Show NR-style CWV gauge cards for a single URL."""
    st.markdown(f"#### Web Vitals — `{url}`")
    weights = url_df["sample_count"]

    primary = [
        ("Largest Contentful Paint (LCP)", "lcp_p75", "lcp", fmt_ms, "largestContentfulPaint"),
        ("Interaction to Next Paint (INP)", "inp_p75", "inp", fmt_ms, "interactionToNextPaint"),
        ("Cumulative Layout Shift (CLS)",   "cls_p75", "cls", fmt_cls, "cumulativeLayoutShift"),
    ]
    cols = st.columns(3)
    for col, (label, p75_col, key, fmt_fn, avg_col) in zip(cols, primary):
        with col:
            p75 = weighted_mean(url_df[p75_col], weights) if p75_col in url_df.columns else None
            avg = weighted_mean(url_df[avg_col], weights) if avg_col in url_df.columns else None
            pct_good, pct_ni, pct_poor = cwv_distribution(url_df[p75_col], key, weights)
            cwv_gauge_card(label, p75, key, fmt_fn, avg, pct_good, pct_ni, pct_poor)


def _wmean_grouped_series(df: pd.DataFrame, group_col: str, metric_col: str) -> pd.Series:
    """Compute weighted mean of metric_col grouped by group_col (vectorized)."""
    result = weighted_mean_grouped(df, group_col, [metric_col])
    if result.empty or metric_col not in result.columns:
        return pd.Series(dtype=float)
    return result[metric_col]


def _render_device_breakdown(url_df: pd.DataFrame, url: str) -> None:
    """Device type pie chart + table (like NR)."""
    st.markdown("#### Device type")

    device_views = url_df.groupby("deviceType")["sample_count"].sum().reset_index()
    device_views.columns = ["Device type", "Page views"]
    device_views = device_views.sort_values("Page views", ascending=False)
    total_views = device_views["Page views"].sum()

    if device_views.empty:
        st.info("No device data.")
        return

    col_pie, col_table = st.columns(2)

    with col_pie:
        fig = px.pie(
            device_views,
            values="Page views",
            names="Device type",
            title=f"Page views ({total_views:,})",
            height=300,
            color_discrete_sequence=["#e84040", "#1ec773", "#f5a623", "#4c9be8"],
        )
        fig.update_layout(
            margin=dict(t=40, b=20, l=0, r=0),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, width="stretch")

    with col_table:
        # Build table with LCP, INP, CLS per device
        metrics = [
            ("LCP", "lcp_p75"),
            ("INP", "inp_p75"),
            ("CLS", "cls_p75"),
        ]
        table_data = device_views.copy()
        for label, col_name in metrics:
            if col_name in url_df.columns:
                vals = _wmean_grouped_series(url_df, "deviceType", col_name)
                table_data[label] = table_data["Device type"].map(vals)

        _render_metric_table(table_data, "Device type", metrics)


def _render_browser_breakdown(browser_df: pd.DataFrame, url: str) -> None:
    """Browser & version breakdown (bar chart + table, like NR)."""
    st.markdown("#### Browser and version")

    bv = browser_df.groupby("browser")["sample_count"].sum().reset_index()
    bv.columns = ["Browser and version", "Page views"]
    bv = bv.sort_values("Page views", ascending=False).head(10)

    if bv.empty:
        st.info("No browser data available. Run `manage.py update` to collect browser data.")
        return

    col_bar, col_table = st.columns(2)

    with col_bar:
        fig = px.bar(
            bv.sort_values("Page views"),
            x="Page views",
            y="Browser and version",
            orientation="h",
            height=max(250, len(bv) * 35 + 80),
            color="Page views",
            color_continuous_scale="Plasma",
        )
        fig.update_layout(
            margin=dict(t=20, b=20, l=0, r=0),
            coloraxis_showscale=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, width="stretch")

    with col_table:
        metrics = [("LCP", "lcp_p75")]
        table_data = bv.copy()
        if "lcp_p75" in browser_df.columns:
            vals = _wmean_grouped_series(browser_df, "browser", "lcp_p75")
            table_data["LCP"] = table_data["Browser and version"].map(vals)

        _render_metric_table(table_data, "Browser and version", metrics)


def _render_metric_table(
    table_data: pd.DataFrame,
    group_col: str,
    metrics: list[tuple[str, str]],
) -> None:
    """Render an HTML table with colored metric cells (NR style)."""
    # Map metric label to threshold key
    metric_key_map = {"LCP": "lcp", "INP": "inp", "CLS": "cls"}

    header = f'<th style="text-align:left">{group_col}</th>'
    for label, _ in metrics:
        if label in table_data.columns:
            header += f'<th>{label}</th>'
    header += '<th style="text-align:right">Page views</th>'

    rows_html = ""
    for _, row in table_data.iterrows():
        safe_name = html_mod.escape(str(row[group_col]))
        rows_html += f'<tr><td>{safe_name}</td>'

        for label, _ in metrics:
            if label not in table_data.columns:
                continue
            val = row.get(label)
            if val is None or pd.isna(val):
                rows_html += '<td style="text-align:center">—</td>'
                continue

            key = metric_key_map.get(label, "")
            status = cwv_status(val, key)
            bg = CWV_COLOR.get(status, "#555")

            if label == "CLS":
                formatted = f"{val:.2f}"
            elif val >= 1000:
                formatted = f"{val / 1000:.1f} s"
            else:
                formatted = f"{val:.0f} ms"

            rows_html += (
                f'<td style="background:{bg}33;color:#fff;text-align:center;'
                f'padding:4px 8px;border-radius:4px;font-weight:600">'
                f'{formatted}</td>'
            )

        views = row.get("Page views", 0)
        rows_html += f'<td style="text-align:right">{int(views):,}</td></tr>'

    st.markdown(
        f"""<table style="width:100%;font-size:0.82rem;border-collapse:collapse;
             border-spacing:0 4px">
        <thead><tr style="border-bottom:1px solid #333">{header}</tr></thead>
        <tbody>{rows_html}</tbody>
        </table>""",
        unsafe_allow_html=True,
    )


def _render_single_url_charts(chart_df: pd.DataFrame, url: str) -> None:
    st.markdown(f"#### All metrics — `{url}`")

    chart_pairs = [
        ("largestContentfulPaint", "LCP",  "lcp",  ["lcp_p75", "lcp_p90", "lcp_p95"]),
        ("cumulativeLayoutShift",  "CLS",  "cls",  ["cls_p75", "cls_p90", "cls_p95"]),
        ("interactionToNextPaint", "INP",  "inp",  ["inp_p75", "inp_p90", "inp_p95"]),
        ("firstContentfulPaint",   "FCP",  "fcp",  ["fcp_p75", "fcp_p90", "fcp_p95"]),
        ("timeToFirstByte",        "TTFB", "ttfb", ["ttfb_p75", "ttfb_p90", "ttfb_p95"]),
        ("firstPaint",             "FP",   "",     []),
        ("windowLoad",             "Load", "",     []),
    ]

    col1, col2 = st.columns(2)
    for i, (col, label, key, pcts) in enumerate(chart_pairs):
        target = col1 if i % 2 == 0 else col2
        with target:
            st.plotly_chart(
                time_series_chart(chart_df, col, label, key, pcts),
                width="stretch",
            )
