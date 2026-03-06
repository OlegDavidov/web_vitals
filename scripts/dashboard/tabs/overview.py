"""Overview tab: Core Web Vitals gauge cards + trend charts + worst pages."""
from __future__ import annotations

import html as html_mod

import pandas as pd
import streamlit as st

from ..charts import time_series_chart, volume_bar_chart
from ..components import cwv_gauge_card, kpi_card
from ..constants import CWV_COLOR, THRESHOLDS
from ..formatters import (
    cwv_distribution,
    cwv_status,
    fmt_cls,
    fmt_delta,
    fmt_ms,
    weighted_mean,
    weighted_mean_grouped,
)


def _split_periods(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Split df into two equal halves by time for period-over-period comparison."""
    if df.empty:
        return df, df
    mid_ts = df["timestamp"].min() + (df["timestamp"].max() - df["timestamp"].min()) / 2
    return df[df["timestamp"] > mid_ts], df[df["timestamp"] <= mid_ts]


def tab_overview(df: pd.DataFrame, url_df: pd.DataFrame | None = None) -> None:
    st.subheader("Core Web Vitals — Overview")

    if df.empty:
        st.info("No data for the selected filters.")
        return

    # Use url_df (accurate overall percentiles) for headline cards when available;
    # fall back to faceted df otherwise.
    has_url = url_df is not None and not url_df.empty
    pctl_df = url_df if has_url else df

    weights = df["sample_count"]
    pctl_w  = pctl_df["sample_count"]
    curr_df, prev_df = _split_periods(df)
    curr_w = curr_df["sample_count"]
    prev_w = prev_df["sample_count"]
    pctl_curr, pctl_prev = _split_periods(pctl_df)
    pctl_curr_w = pctl_curr["sample_count"]
    pctl_prev_w = pctl_prev["sample_count"]

    # ── Primary CWV gauge cards (LCP / INP / CLS) ────────────────────────────
    primary = [
        ("Largest Contentful Paint (LCP)", "largestContentfulPaint", "lcp",  fmt_ms,  "lcp_p75"),
        ("Interaction to Next Paint (INP)", "interactionToNextPaint", "inp",  fmt_ms,  "inp_p75"),
        ("Cumulative Layout Shift (CLS)",   "cumulativeLayoutShift",  "cls",  fmt_cls, "cls_p75"),
    ]

    cols = st.columns(3)
    for col, (label, db_col, key, fmt_fn, p75_col) in zip(cols, primary):
        with col:
            p75 = weighted_mean(pctl_df[p75_col], pctl_w) if p75_col in pctl_df.columns else None
            avg = weighted_mean(pctl_df[db_col], pctl_w) if db_col in pctl_df.columns else weighted_mean(df[db_col], weights)

            # Period comparison for p75
            curr_p75 = weighted_mean(pctl_curr[p75_col], pctl_curr_w) if p75_col in pctl_curr.columns else None
            prev_p75 = weighted_mean(pctl_prev[p75_col], pctl_prev_w) if p75_col in pctl_prev.columns else None
            delta_html = fmt_delta(curr_p75, prev_p75, fmt_fn)

            pct_good, pct_ni, pct_poor = cwv_distribution(pctl_df[p75_col], key, pctl_w)
            cwv_gauge_card(label, p75, key, fmt_fn, avg, pct_good, pct_ni, pct_poor,
                           delta_html=delta_html)

    st.markdown("---")

    # ── Secondary metrics (FCP / TTFB / FP / Load) ──────────────────────────
    secondary = [
        ("FCP",  "firstContentfulPaint", "fcp",  fmt_ms),
        ("TTFB", "timeToFirstByte",      "ttfb", fmt_ms),
        ("FP",   "firstPaint",           "",     fmt_ms),
        ("Load", "windowLoad",           "",     fmt_ms),
    ]

    sec_cols = st.columns(len(secondary))
    for col, (label, db_col, key, fmt_fn) in zip(sec_cols, secondary):
        with col:
            # Use url_df for FCP/TTFB if available
            src = pctl_df if has_url and db_col in pctl_df.columns else df
            src_w = src["sample_count"]
            if db_col not in src.columns:
                kpi_card(label, "—", "unknown")
                continue
            avg = weighted_mean(src[db_col], src_w)
            p75_col = f"{key}_p75"
            p75 = weighted_mean(src[p75_col], src_w) if key and p75_col in src.columns else None
            # CWV status is defined by p75 (Google standard); fall back to avg
            # only for metrics without a p75 column (FP, Load).
            status_val = p75 if p75 is not None else avg
            status = cwv_status(status_val, key) if status_val is not None and key else "unknown"

            # Delta (always from faceted df for consistency)
            curr_avg = weighted_mean(curr_df[db_col], curr_w) if db_col in curr_df.columns else None
            prev_avg = weighted_mean(prev_df[db_col], prev_w) if db_col in prev_df.columns else None
            delta = fmt_delta(curr_avg, prev_avg, fmt_fn)

            sub_parts = []
            if p75 is not None:
                sub_parts.append(f"p75: {fmt_fn(p75)}")
            if delta:
                sub_parts.append(delta)
            kpi_card(label, fmt_fn(avg), status, sub=" &nbsp;|&nbsp; ".join(sub_parts))

    st.markdown("---")

    # ── Worst pages (top 5 exceeding thresholds) ─────────────────────────────
    _render_worst_pages(pctl_df, pctl_w)

    st.markdown("---")

    # ── Trend charts + volume (isolated in a fragment to avoid full reruns) ──
    chart_df = pctl_df if has_url else df
    _render_trend_charts(chart_df, df)


@st.fragment
def _render_trend_charts(chart_df: pd.DataFrame, faceted_df: pd.DataFrame) -> None:
    """Trend charts + volume bar — wrapped in @st.fragment to avoid full-page reruns."""
    col_left, col_right = st.columns(2)
    with col_left:
        st.plotly_chart(
            time_series_chart(chart_df, "largestContentfulPaint", "LCP over time",
                              "lcp", ["lcp_p75", "lcp_p90", "lcp_p95"]),
            use_container_width=True,
        )
        st.plotly_chart(
            time_series_chart(chart_df, "interactionToNextPaint", "INP over time",
                              "inp", ["inp_p75", "inp_p90", "inp_p95"]),
            use_container_width=True,
        )
        st.plotly_chart(
            time_series_chart(chart_df, "timeToFirstByte", "TTFB over time",
                              "ttfb", ["ttfb_p75", "ttfb_p90", "ttfb_p95"]),
            use_container_width=True,
        )

    with col_right:
        st.plotly_chart(
            time_series_chart(chart_df, "cumulativeLayoutShift", "CLS over time",
                              "cls", ["cls_p75", "cls_p90", "cls_p95"]),
            use_container_width=True,
        )
        st.plotly_chart(
            time_series_chart(chart_df, "firstContentfulPaint", "FCP over time",
                              "fcp", ["fcp_p75", "fcp_p90", "fcp_p95"]),
            use_container_width=True,
        )

    # Page view volume (from faceted df — has device/connection granularity)
    st.plotly_chart(volume_bar_chart(faceted_df), use_container_width=True)


def _render_worst_pages(df: pd.DataFrame, weights: pd.Series) -> None:
    """Show top 5 worst-performing pages that exceed CWV thresholds."""
    if "url_group" not in df.columns:
        return

    metrics = [
        ("LCP", "lcp_p75", "lcp", fmt_ms),
        ("CLS", "cls_p75", "cls", fmt_cls),
        ("INP", "inp_p75", "inp", fmt_ms),
    ]

    p75_cols = [m[1] for m in metrics if m[1] in df.columns]
    if not p75_cols:
        return

    # Single vectorized groupby for all metrics + views
    agg = weighted_mean_grouped(df, "url_group", p75_cols)
    if agg.empty:
        return
    views = df.groupby("url_group")["sample_count"].sum()
    agg["views"] = views

    worst_rows: list[dict] = []
    for label, p75_col, key, fmt_fn in metrics:
        if p75_col not in agg.columns:
            continue
        t = THRESHOLDS[key]
        col_data = agg[p75_col].dropna()
        poor = col_data[col_data > t["poor"]].nlargest(5)
        for url, val in poor.items():
            status = cwv_status(val, key)
            worst_rows.append({
                "Page": url,
                "Metric": label,
                "p75": fmt_fn(val),
                "Views": int(agg.loc[url, "views"]),
                "_status": status,
                "_raw": val,
            })

    if not worst_rows:
        st.success("All pages within CWV thresholds (p75).")
        return

    st.markdown("#### Pages exceeding thresholds")
    worst_df = pd.DataFrame(worst_rows).sort_values("_raw", ascending=False).head(10)

    rows_html = ""
    for _, r in worst_df.iterrows():
        color = CWV_COLOR.get(r["_status"], CWV_COLOR["poor"])
        safe_page = html_mod.escape(str(r["Page"]))
        rows_html += (
            f'<tr>'
            f'<td style="max-width:300px;overflow:hidden;text-overflow:ellipsis">{safe_page}</td>'
            f'<td>{r["Metric"]}</td>'
            f'<td style="color:{color};font-weight:700">{r["p75"]}</td>'
            f'<td style="text-align:right">{r["Views"]:,}</td>'
            f'</tr>'
        )

    st.markdown(
        f"""<table style="width:100%;font-size:0.82rem;border-collapse:collapse">
        <thead><tr style="border-bottom:1px solid #333">
            <th style="text-align:left">Page</th>
            <th>Metric</th>
            <th>p75</th>
            <th style="text-align:right">Views</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
        </table>""",
        unsafe_allow_html=True,
    )
