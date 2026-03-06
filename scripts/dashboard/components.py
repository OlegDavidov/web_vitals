"""Reusable Streamlit UI components."""
from __future__ import annotations

from typing import Callable

import streamlit as st

from .constants import CWV_COLOR, THRESHOLD_LABELS
from .formatters import cwv_status


def kpi_card(label: str, value: str, status: str, sub: str = "") -> None:
    """Simple KPI card with a coloured left border."""
    color = CWV_COLOR.get(status, CWV_COLOR["unknown"])
    sub_html = f'<div style="font-size:0.75rem;color:#888">{sub}</div>' if sub else ""
    st.markdown(
        f"""
        <div style="
            border-left: 4px solid {color};
            padding: 12px 16px;
            border-radius: 6px;
            background: rgba(255,255,255,0.04);
            margin-bottom: 4px;
        ">
            <div style="font-size:0.78rem;color:#aaa;margin-bottom:2px">{label}</div>
            <div style="font-size:1.6rem;font-weight:700;color:{color}">{value}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def cwv_gauge_card(
    label: str,
    value: float | None,
    metric_key: str,
    fmt_fn: Callable,
    avg: float | None = None,
    pct_good: float = 0.0,
    pct_ni: float = 0.0,
    pct_poor: float = 0.0,
    delta_html: str = "",
) -> None:
    """
    CWV metric card showing p75 value (headline), status label,
    % Good/NI/Poor breakdown, and avg — styled after the New Relic
    Web Vitals widget.

    *value* is the weighted p75 (headline number).
    *avg* is the weighted average (shown at the bottom).
    Distribution % are approximate: based on per-window avg values,
    not individual page loads (NR has event-level access we lack).
    """
    status = cwv_status(value, metric_key) if value is not None else "unknown"
    color  = CWV_COLOR.get(status, CWV_COLOR["unknown"])
    status_label = {
        "good":             "GOOD",
        "needs_improvement": "NEEDS IMPROVEMENT",
        "poor":             "POOR",
        "unknown":          "N/A",
    }[status]

    tl = THRESHOLD_LABELS.get(metric_key, {})
    good_lbl = tl.get("good", "")
    ni_lbl   = tl.get("ni",   "")
    poor_lbl = tl.get("poor", "")

    val_str = fmt_fn(value)
    avg_str = fmt_fn(avg) if avg is not None else "—"

    st.markdown(
        f"""
        <div style="
            padding: 16px 18px;
            border-radius: 8px;
            background: rgba(255,255,255,0.04);
            margin-bottom: 8px;
        ">
            <div style="font-size:0.78rem;color:#aaa;margin-bottom:4px">{label}</div>
            <div style="font-size:2rem;font-weight:700;color:#fff;line-height:1.1">{val_str}</div>
            <div style="font-size:0.72rem;font-weight:700;color:{color};
                        letter-spacing:0.04em;margin:4px 0 12px">{status_label}</div>
            <div style="font-size:0.7rem;color:#888;margin-bottom:3px">
                <span style="color:{CWV_COLOR['good']}">&#9612;</span>
                &nbsp;Good ({good_lbl})&nbsp;&nbsp;<b>{pct_good:.0f}%</b>
            </div>
            <div style="font-size:0.7rem;color:#888;margin-bottom:3px">
                <span style="color:{CWV_COLOR['needs_improvement']}">&#9612;</span>
                &nbsp;Needs improvement ({ni_lbl})&nbsp;&nbsp;<b>{pct_ni:.0f}%</b>
            </div>
            <div style="font-size:0.7rem;color:#888;margin-bottom:10px">
                <span style="color:{CWV_COLOR['poor']}">&#9612;</span>
                &nbsp;Poor ({poor_lbl})&nbsp;&nbsp;<b>{pct_poor:.0f}%</b>
            </div>
            <div style="font-size:0.7rem;color:#555">
                avg &middot; {avg_str} {delta_html}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
