"""Breakdowns tab: performance sliced by device and network type."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from ..charts import bar_breakdown_chart
from ..constants import METRIC_COLS


def tab_breakdowns(df: pd.DataFrame) -> None:
    st.subheader("Performance by Device & Network")

    if df.empty:
        st.info("No data for the selected filters.")
        return

    metric_choice = st.selectbox(
        "Metric",
        options=list(METRIC_COLS.keys()),
        index=0,
        key="breakdown_metric",
    )
    col = METRIC_COLS[metric_choice]

    col_left, col_right = st.columns(2)

    with col_left:
        st.plotly_chart(
            bar_breakdown_chart(df, "deviceType", col,
                                f"Avg {metric_choice} by Device"),
            use_container_width=True,
        )

    with col_right:
        st.plotly_chart(
            bar_breakdown_chart(df, "connectionType", col,
                                f"Avg {metric_choice} by Network"),
            use_container_width=True,
        )

        # Vectorized heatmap: weighted mean per (device, network)
        sub = df[["deviceType", "connectionType", col, "sample_count"]].dropna(subset=[col, "sample_count"])
        sub = sub[sub["sample_count"] > 0]
        if not sub.empty:
            sub["_wv"] = sub[col] * sub["sample_count"]
            g = sub.groupby(["deviceType", "connectionType"])
            pivot = (g["_wv"].sum() / g["sample_count"].sum()).unstack(fill_value=None)

            if not pivot.empty:
                fig_heat = px.imshow(
                    pivot,
                    title=f"{metric_choice} heatmap: Device x Network",
                    color_continuous_scale="RdYlGn_r",
                    text_auto=".0f",
                    height=280,
                )
                fig_heat.update_layout(margin=dict(t=40, b=20, l=0, r=0))
                st.plotly_chart(fig_heat, use_container_width=True)
