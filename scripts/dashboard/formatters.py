"""Value formatting, CWV status helpers, and URL normalisation."""
from __future__ import annotations

import re
from urllib.parse import urlparse

import pandas as pd

from .constants import THRESHOLDS


def normalize_url(url: str) -> str:
    """Strip host/scheme from a URL, returning only the path.

    Handles two NR URL formats:
        https://example.com/path  ->  /path
        example.com:443/path      ->  /path  (NR grouped-URL format)
    """
    # Strip scheme + host from https://... URLs
    if url.startswith(("http://", "https://")):
        path = urlparse(url).path or "/"
    # Strip host:port from NR grouped-URL format (e.g. example.com:443/path)
    elif re.match(r'^[^/]+:\d+/', url):
        path = url[url.index('/'):]
    else:
        path = url

    return path


def weighted_mean(values: pd.Series, weights: pd.Series) -> float | None:
    """Return sample_count-weighted mean, or None if no valid data."""
    mask = values.notna() & weights.notna() & (weights > 0)
    if not mask.any():
        return None
    v, w = values[mask], weights[mask]
    return float((v * w).sum() / w.sum())


def weighted_mean_grouped(
    df: pd.DataFrame,
    group_col: str,
    metric_cols: list[str],
    weight_col: str = "sample_count",
) -> pd.DataFrame:
    """Compute weighted means for multiple metrics in a single groupby.

    Returns a DataFrame indexed by *group_col* with one column per metric.
    Much faster than calling weighted_mean() in N separate closures.
    """
    present = [c for c in metric_cols if c in df.columns]
    if not present:
        return pd.DataFrame()

    w = df[weight_col]
    valid_w = w.notna() & (w > 0)
    subset = df.loc[valid_w]

    if subset.empty:
        return pd.DataFrame()

    sw = subset[weight_col]
    weighted = subset[present].multiply(sw, axis=0)
    weighted[weight_col] = sw
    weighted[group_col] = subset[group_col]

    grouped = weighted.groupby(group_col)
    sums = grouped[present].sum()
    w_sums = grouped[weight_col].sum()

    result = sums.div(w_sums, axis=0)

    # Null out metrics where all original values were NaN
    for col in present:
        orig_valid = subset[col].notna()
        if not orig_valid.all():
            groups_with_data = subset.loc[orig_valid, group_col].unique()
            null_groups = result.index.difference(groups_with_data)
            if len(null_groups):
                result.loc[null_groups, col] = None

    return result


def fmt_ms(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{val:,.0f} ms"


def fmt_cls(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    return f"{val:.3f}"


def fmt_delta(current: float | None, previous: float | None, fmt_fn, lower_is_better: bool = True) -> str:
    """Format a delta string with arrow and colour for period comparison."""
    if current is None or previous is None or previous == 0:
        return ""
    delta = current - previous
    pct = delta / previous * 100
    arrow = "▲" if delta > 0 else "▼"
    # For most metrics lower = better; for CLS too
    if lower_is_better:
        color = "#e84040" if delta > 0 else "#1ec773"
    else:
        color = "#1ec773" if delta > 0 else "#e84040"
    return f'<span style="color:{color};font-size:0.75rem">{arrow} {abs(pct):.1f}%</span>'


def cwv_status(value: float | None, metric: str) -> str:
    """Return 'good' | 'needs_improvement' | 'poor' | 'unknown'."""
    t = THRESHOLDS.get(metric)
    if t is None or value is None or pd.isna(value):
        return "unknown"
    if value <= t["good"]:
        return "good"
    if value <= t["poor"]:
        return "needs_improvement"
    return "poor"


def cwv_distribution(
    series: pd.Series,
    metric: str,
    weights: pd.Series | None = None,
) -> tuple[float, float, float]:
    """
    Return (pct_good, pct_ni, pct_poor) for *series* against *metric* thresholds.
    When *weights* (e.g. sample_count) are provided the percentages are weighted.
    """
    t = THRESHOLDS.get(metric)
    if t is None:
        return 0.0, 0.0, 0.0

    s = series.dropna()
    if s.empty:
        return 0.0, 0.0, 0.0

    if weights is not None:
        w = weights.loc[s.index].fillna(0)
        total = w.sum()
        if total == 0:
            return 0.0, 0.0, 0.0
        pct_good = w[s <= t["good"]].sum() / total * 100
        pct_poor = w[s > t["poor"]].sum() / total * 100
    else:
        n = len(s)
        pct_good = (s <= t["good"]).sum() / n * 100
        pct_poor = (s > t["poor"]).sum() / n * 100

    pct_ni = max(0.0, 100.0 - pct_good - pct_poor)
    return pct_good, pct_ni, pct_poor
