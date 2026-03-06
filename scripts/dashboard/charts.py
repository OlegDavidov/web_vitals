"""Reusable Plotly chart builders."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from .constants import CWV_COLOR, THRESHOLDS
from .formatters import weighted_mean_grouped

_LAYOUT = dict(
    margin=dict(t=40, b=20, l=0, r=0),
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
)


def cwv_threshold_shapes(metric: str) -> list[dict]:
    """Plotly layout shapes for Good / Poor threshold lines."""
    t = THRESHOLDS.get(metric)
    if not t:
        return []
    return [
        dict(
            type="line", y0=t["good"], y1=t["good"], x0=0, x1=1,
            xref="paper",
            line=dict(color=CWV_COLOR["good"], dash="dash", width=1),
        ),
        dict(
            type="line", y0=t["poor"], y1=t["poor"], x0=0, x1=1,
            xref="paper",
            line=dict(color=CWV_COLOR["poor"], dash="dash", width=1),
        ),
    ]


def time_series_chart(
    df: pd.DataFrame,
    col: str,
    title: str,
    metric_key: str = "",
    percentile_cols: list[str] | None = None,
) -> go.Figure:
    """Line chart of a metric over time, with optional p75/p90/p95 traces."""
    pct_cols = [p for p in (percentile_cols or []) if p in df.columns]
    all_cols = [col] + pct_cols

    # Vectorized weighted aggregation — single groupby for all columns
    agg = weighted_mean_grouped(df, "datetime", all_cols)
    if agg.empty:
        return go.Figure().update_layout(title=title, **_LAYOUT)

    agg = agg.sort_index().reset_index()

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=agg["datetime"], y=agg[col],
        name="avg",
        line=dict(color="#4c9be8", width=2),
        visible="legendonly",
    ))

    pct_colors = ["#f5a623", "#e86040", "#9b59b6"]
    for i, p in enumerate(pct_cols):
        if p in agg.columns:
            label = p.split("_")[-1].upper()
            fig.add_trace(go.Scatter(
                x=agg["datetime"], y=agg[p],
                name=label,
                line=dict(color=pct_colors[i % len(pct_colors)], width=1.5, dash="dot"),
                visible=True if label == "P75" else "legendonly",
            ))

    for shape in cwv_threshold_shapes(metric_key):
        fig.add_shape(**shape)

    fig.update_layout(
        title=title,
        height=320,
        legend=dict(orientation="h", y=-0.15),
        xaxis_title=None,
        yaxis_title=None,
        hovermode="x unified",
        **_LAYOUT,
    )
    return fig


def bar_breakdown_chart(
    df: pd.DataFrame,
    group_col: str,
    metric_col: str,
    title: str,
) -> go.Figure:
    """Horizontal bar chart of weighted avg *metric_col* grouped by *group_col*."""
    agg = weighted_mean_grouped(df, group_col, [metric_col])
    if agg.empty:
        return go.Figure().update_layout(title=title, **_LAYOUT)

    agg = agg.dropna().reset_index().sort_values(metric_col)

    fig = px.bar(
        agg,
        x=metric_col,
        y=group_col,
        orientation="h",
        title=title,
        height=max(200, len(agg) * 40 + 80),
        color=metric_col,
        color_continuous_scale="RdYlGn_r",
    )
    fig.update_layout(coloraxis_showscale=False, **_LAYOUT)
    return fig


def volume_bar_chart(df: pd.DataFrame) -> go.Figure:
    """Bar chart of page-view volume over time."""
    vol = df.groupby("datetime")["sample_count"].sum().reset_index()
    fig = px.bar(
        vol,
        x="datetime",
        y="sample_count",
        title="Page view volume",
        height=220,
        color_discrete_sequence=["#4c9be8"],
    )
    fig.update_layout(**_LAYOUT)
    return fig
