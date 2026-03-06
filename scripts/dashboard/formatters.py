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


def normalize_url_series(s: pd.Series) -> pd.Series:
    """Vectorized URL normalization — much faster than .apply(normalize_url)."""
    result = s.copy()
    # Handle https:// URLs — extract path
    http_mask = result.str.startswith("http://") | result.str.startswith("https://")
    if http_mask.any():
        result.loc[http_mask] = result.loc[http_mask].str.replace(
            r'^https?://[^/]*', '', regex=True,
        )
        # Empty path → "/"
        empty = http_mask & (result == '')
        if empty.any():
            result.loc[empty] = '/'
    # Handle NR grouped-URL format (host:port/path) — strip up to first /
    nr_mask = (~http_mask) & result.str.match(r'^[^/]+:\d+/', na=False)
    if nr_mask.any():
        result.loc[nr_mask] = result.loc[nr_mask].str.replace(
            r'^[^/]+', '', regex=True,
        )
    return result


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
    Uses per-metric weight masking so that NaN metric values do not
    inflate the denominator (fixes underestimation when some rows
    have NaN after outlier cleanup or NULL from NR).
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
    groups = subset[group_col]

    # Numerator: value * weight (NaN propagates correctly via sum skipna)
    weighted_vals = subset[present].multiply(sw, axis=0)

    # Denominator: per-metric weights (zero where metric is NaN,
    # so NaN rows don't inflate the divisor)
    metric_weights = subset[present].notna().astype(float).multiply(sw, axis=0)

    weighted_vals[group_col] = groups.values
    metric_weights[group_col] = groups.values

    grp_vals = weighted_vals.groupby(group_col)[present].sum()
    grp_weights = metric_weights.groupby(group_col)[present].sum()

    # Avoid division by zero -> NaN for groups with no valid data
    result = grp_vals / grp_weights.replace(0, float("nan"))

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
