"""Dashboard-wide constants: thresholds, metric column names, colours."""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / ".env", override=False)

# ---------------------------------------------------------------------------
# Priority URLs -- always shown first (pinned) in the Page URLs table,
# regardless of the chosen sort metric.  Order here = order in the table.
# Paths are matched against the normalised url_group (path portion only).
#
# Set via PINNED_URL_PATHS in .env (comma-separated), e.g.:
#   PINNED_URL_PATHS=/,/search,/cart,/checkout
# ---------------------------------------------------------------------------
_raw = os.getenv("PINNED_URL_PATHS", "").strip()
PINNED_URL_PATHS: list[str] = [p.strip() for p in _raw.split(",") if p.strip()] if _raw else []

# Google Core Web Vitals thresholds
THRESHOLDS: dict[str, dict] = {
    "lcp":  {"good": 2500,  "poor": 4000,  "unit": "ms", "label": "LCP"},
    "cls":  {"good": 0.1,   "poor": 0.25,  "unit": "",   "label": "CLS"},
    "inp":  {"good": 200,   "poor": 500,   "unit": "ms", "label": "INP"},
    "fcp":  {"good": 1800,  "poor": 3000,  "unit": "ms", "label": "FCP"},
    "ttfb": {"good": 800,   "poor": 1800,  "unit": "ms", "label": "TTFB"},
}

# Human-readable threshold labels shown in gauge cards (matches Google/NR style)
THRESHOLD_LABELS: dict[str, dict[str, str]] = {
    "lcp":  {"good": "<= 2.5 s",   "ni": "2.5 – 4 s",       "poor": "> 4 s"},
    "cls":  {"good": "<= 0.1",     "ni": "0.1 – 0.25",      "poor": "> 0.25"},
    "inp":  {"good": "<= 200 ms",  "ni": "200 – 500 ms",    "poor": "> 500 ms"},
    "fcp":  {"good": "<= 1.8 s",   "ni": "1.8 – 3 s",       "poor": "> 3 s"},
    "ttfb": {"good": "<= 800 ms",  "ni": "800 ms – 1.8 s",  "poor": "> 1.8 s"},
}

# DB column names keyed by short metric label
METRIC_COLS: dict[str, str] = {
    "LCP":  "largestContentfulPaint",
    "CLS":  "cumulativeLayoutShift",
    "INP":  "interactionToNextPaint",
    "FCP":  "firstContentfulPaint",
    "TTFB": "timeToFirstByte",
    "FP":   "firstPaint",
    "Load": "windowLoad",
}

# Status colours (foreground)
CWV_COLOR: dict[str, str] = {
    "good":             "#1ec773",
    "needs_improvement": "#f5a623",
    "poor":             "#e84040",
    "unknown":          "#aaaaaa",
}

# Status colours (background tint for table cells)
CWV_BG: dict[str, str] = {
    "good":             "#1a3d2e",
    "needs_improvement": "#3d3010",
    "poor":             "#3d1010",
}
