"""
New Relic NerdGraph API client.

Uses the NerdGraph GraphQL endpoint with a User API Key (NRAK-…).
Builds a NRQL query per 6-hour window, fetches faceted Web Vitals,
and returns records ready for DB insertion.

NRQL facets: targetGroupedUrl, deviceType, connectionType, navigationType
Metrics stored: averages + p75/p90/p95 for four CWV + TTFB,
                averages for firstPaint, windowLoad, elementSize.
"""
import logging
import time
import warnings
from typing import Any

import requests
import urllib3

from config import (
    NEW_RELIC_ACCOUNT_ID,
    NEW_RELIC_API_KEY,
    NR_EVENT_TYPE,
    NR_APP_NAME,
    NR_COUNTRY_CODE,
    NERDGRAPH_URL,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_DELAY,
    NR_SSL_VERIFY,
    NR_TRUST_ENV,
    NR_PROXIES,
)

# Suppress InsecureRequestWarning when SSL verification is intentionally disabled
# (NR_SSL_VERIFY=false — typically set on machines behind a corporate SSL proxy).
if NR_SSL_VERIFY is False:
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)

# ── Reusable HTTP session ─────────────────────────────────────────────────────

_session = requests.Session()
_session.trust_env = NR_TRUST_ENV
if NR_PROXIES is not None:
    _session.proxies.update(NR_PROXIES)
_session.headers.update({
    "Api-Key": NEW_RELIC_API_KEY,
    "Content-Type": "application/json",
})

# ── GraphQL query template ─────────────────────────────────────────────────────

_GQL = """
query NrqlQuery($accountId: Int!, $nrql: Nrql!) {
  actor {
    account(id: $accountId) {
      nrql(query: $nrql, timeout: 120) {
        results
        metadata {
          messages
          timeWindow { begin end }
        }
      }
    }
  }
}
"""

# ── NRQL query template ────────────────────────────────────────────────────────
# percentile(field, 75, 90, 95) produces keys like:
#   "percentile.largestContentfulPaint[75]"  etc.

_NRQL_TEMPLATE = """
SELECT
  average(largestContentfulPaint)             AS lcp_avg,
  percentile(largestContentfulPaint, 75, 90, 95),
  average(cumulativeLayoutShift)              AS cls_avg,
  percentile(cumulativeLayoutShift, 75, 90, 95),
  average(interactionToNextPaint)             AS inp_avg,
  percentile(interactionToNextPaint, 75, 90, 95),
  average(firstContentfulPaint)               AS fcp_avg,
  percentile(firstContentfulPaint, 75, 90, 95),
  average(timeToFirstByte)                    AS ttfb_avg,
  percentile(timeToFirstByte, 75, 90, 95),
  average(firstPaint)                         AS fp_avg,
  average(windowLoad)                         AS wl_avg,
  average(elementSize)                        AS elsize_avg,
  count(*)                                    AS sample_count
FROM {event_type}
{where_clause}
FACET targetGroupedUrl, deviceType, networkEffectiveType
SINCE {since} UNTIL {until}
LIMIT MAX
""".strip()


_NRQL_BROWSER_TEMPLATE = """
SELECT
  average(largestContentfulPaint)             AS lcp_avg,
  percentile(largestContentfulPaint, 75),
  average(cumulativeLayoutShift)              AS cls_avg,
  percentile(cumulativeLayoutShift, 75),
  average(interactionToNextPaint)             AS inp_avg,
  percentile(interactionToNextPaint, 75),
  average(firstContentfulPaint)               AS fcp_avg,
  percentile(firstContentfulPaint, 75),
  average(timeToFirstByte)                    AS ttfb_avg,
  percentile(timeToFirstByte, 75),
  count(*)                                    AS sample_count
FROM {event_type}
{where_clause}
FACET targetGroupedUrl, userAgentName, userAgentVersion
SINCE {since} UNTIL {until}
LIMIT MAX
""".strip()

# URL-only query: FACET only by targetGroupedUrl (no device/connection split).
# Produces accurate overall percentiles matching NR's Web Vitals view.
_NRQL_URL_TEMPLATE = """
SELECT
  average(largestContentfulPaint)             AS lcp_avg,
  percentile(largestContentfulPaint, 75, 90, 95),
  average(cumulativeLayoutShift)              AS cls_avg,
  percentile(cumulativeLayoutShift, 75, 90, 95),
  average(interactionToNextPaint)             AS inp_avg,
  percentile(interactionToNextPaint, 75, 90, 95),
  average(firstContentfulPaint)               AS fcp_avg,
  percentile(firstContentfulPaint, 75, 90, 95),
  average(timeToFirstByte)                    AS ttfb_avg,
  percentile(timeToFirstByte, 75, 90, 95),
  average(firstPaint)                         AS fp_avg,
  average(windowLoad)                         AS wl_avg,
  count(*)                                    AS sample_count
FROM {event_type}
{where_clause}
FACET targetGroupedUrl
SINCE {since} UNTIL {until}
LIMIT MAX
""".strip()


def _sanitize_nrql_string(s: str) -> str:
    """Escape single quotes in a NRQL string literal to prevent injection."""
    return s.replace("'", "\\'")


def _build_nrql_from(template: str, since_epoch: int, until_epoch: int) -> str:
    """Build a NRQL query from *template* with standard WHERE filters."""
    conditions = []
    if NR_APP_NAME:
        conditions.append(f"appName = '{_sanitize_nrql_string(NR_APP_NAME)}'")
    if NR_COUNTRY_CODE:
        conditions.append(f"countryCode = '{_sanitize_nrql_string(NR_COUNTRY_CODE)}'")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return template.format(
        event_type=NR_EVENT_TYPE,
        where_clause=where,
        since=since_epoch,
        until=until_epoch,
    )


def _build_nrql(since_epoch: int, until_epoch: int) -> str:
    return _build_nrql_from(_NRQL_TEMPLATE, since_epoch, until_epoch)


def _build_url_nrql(since_epoch: int, until_epoch: int) -> str:
    return _build_nrql_from(_NRQL_URL_TEMPLATE, since_epoch, until_epoch)


def _build_browser_nrql(since_epoch: int, until_epoch: int) -> str:
    return _build_nrql_from(_NRQL_BROWSER_TEMPLATE, since_epoch, until_epoch)


# ── Result parsing ─────────────────────────────────────────────────────────────

def _g(row: dict, *keys: str) -> float | None:
    """Try each key in order; return first non-None scalar value as float."""
    for k in keys:
        v = row.get(k)
        if v is not None and not isinstance(v, dict):
            return float(v)
    return None


def _pct(row: dict, field: str, p: int) -> float | None:
    """
    Extract a percentile value from a NerdGraph result row.

    NerdGraph returns percentile() as a nested dict:
        "percentile.largestContentfulPaint": {"75": 1500.0, "90": 2000.0, "95": 2500.0}

    Falls back to the bracket notation for forward compatibility:
        "percentile.largestContentfulPaint[75]": 1500.0
    """
    key = f"percentile.{field}"
    nested = row.get(key)
    if isinstance(nested, dict):
        v = nested.get(str(p))
        if v is not None:
            return float(v)
    # Fallback: flat bracket notation (older NR behaviour)
    flat = row.get(f"{key}[{p}]")
    if flat is not None:
        return float(flat)
    return None


def _pct_if(row: dict, field: str, p: int, avg_val: float | None) -> float | None:
    """Extract percentile only when the average is non-NULL.

    NR's percentile() returns 0 (not NULL) when there are no matching events
    for a metric, while average() correctly returns NULL.  Storing those fake
    zeros corrupts weighted-mean and distribution calculations downstream.
    """
    if avg_val is None:
        return None
    return _pct(row, field, p)


def _parse_row(row: dict, window_epoch: int) -> dict:
    """
    Convert one NerdGraph FACET result row into a DB-ready dict.

    NerdGraph returns:
      - facet dimensions as a list under the key "facet" (values can be None)
      - averages as "average.<fieldName>"  (also tried via AS alias)
      - percentiles as "percentile.<fieldName>": {"75": v, "90": v, "95": v}
    """
    facet: list = row.get("facet", [])

    def facet_val(idx: int) -> str:
        if idx >= len(facet):
            return ""
        v = facet[idx]
        return v if v is not None else ""  # NULL dimension → empty string

    lcp_avg  = _g(row, "lcp_avg",  "average.largestContentfulPaint")
    cls_avg  = _g(row, "cls_avg",  "average.cumulativeLayoutShift")
    inp_avg  = _g(row, "inp_avg",  "average.interactionToNextPaint")
    fcp_avg  = _g(row, "fcp_avg",  "average.firstContentfulPaint")
    ttfb_avg = _g(row, "ttfb_avg", "average.timeToFirstByte")

    return {
        "timestamp":            window_epoch,
        "targetGroupedUrl":     facet_val(0),
        "deviceType":           facet_val(1),
        "connectionType":       facet_val(2),  # populated from networkEffectiveType
        "navigationType":       "",            # not available in PageViewTiming

        # LCP
        "largestContentfulPaint": lcp_avg,
        "lcp_p75": _pct_if(row, "largestContentfulPaint", 75, lcp_avg),
        "lcp_p90": _pct_if(row, "largestContentfulPaint", 90, lcp_avg),
        "lcp_p95": _pct_if(row, "largestContentfulPaint", 95, lcp_avg),

        # CLS
        "cumulativeLayoutShift": cls_avg,
        "cls_p75": _pct_if(row, "cumulativeLayoutShift", 75, cls_avg),
        "cls_p90": _pct_if(row, "cumulativeLayoutShift", 90, cls_avg),
        "cls_p95": _pct_if(row, "cumulativeLayoutShift", 95, cls_avg),

        # INP
        "interactionToNextPaint": inp_avg,
        "inp_p75": _pct_if(row, "interactionToNextPaint", 75, inp_avg),
        "inp_p90": _pct_if(row, "interactionToNextPaint", 90, inp_avg),
        "inp_p95": _pct_if(row, "interactionToNextPaint", 95, inp_avg),

        # FCP
        "firstContentfulPaint": fcp_avg,
        "fcp_p75": _pct_if(row, "firstContentfulPaint", 75, fcp_avg),
        "fcp_p90": _pct_if(row, "firstContentfulPaint", 90, fcp_avg),
        "fcp_p95": _pct_if(row, "firstContentfulPaint", 95, fcp_avg),

        # TTFB
        "timeToFirstByte": ttfb_avg,
        "ttfb_p75": _pct_if(row, "timeToFirstByte", 75, ttfb_avg),
        "ttfb_p90": _pct_if(row, "timeToFirstByte", 90, ttfb_avg),
        "ttfb_p95": _pct_if(row, "timeToFirstByte", 95, ttfb_avg),

        # Additional metrics
        "firstPaint":  _g(row, "fp_avg",     "average.firstPaint"),
        "windowLoad":  _g(row, "wl_avg",     "average.windowLoad"),
        "elementSize": _g(row, "elsize_avg", "average.elementSize"),

        "sample_count": row.get("sample_count") or row.get("count"),
    }


# ── HTTP layer with retry ──────────────────────────────────────────────────────

def _post_nerdgraph(nrql: str) -> list[dict]:
    """Execute a NRQL query via NerdGraph and return the raw result rows."""
    payload = {
        "query": _GQL,
        "variables": {
            "accountId": NEW_RELIC_ACCOUNT_ID,
            "nrql": nrql,
        },
    }

    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session.post(
                NERDGRAPH_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT,
                verify=NR_SSL_VERIFY,
            )
            resp.raise_for_status()
            body: dict[str, Any] = resp.json()

            if "errors" in body:
                raise RuntimeError(f"NerdGraph errors: {body['errors']}")

            nrql_data = (
                (body.get("data") or {})
                    .get("actor") or {}
            )
            nrql_data = (nrql_data.get("account") or {}).get("nrql") or {}

            # Surface any advisory messages from NR (e.g. truncated results)
            metadata = nrql_data.get("metadata") or {}
            for msg in metadata.get("messages") or []:
                logger.warning("NR message: %s", msg)

            return nrql_data.get("results") or []

        except requests.exceptions.Timeout:
            logger.warning("Request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
            last_exc = Exception("timeout")

        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else 0
            if status == 429:
                wait = RETRY_DELAY * (2 ** (attempt - 1))  # exponential back-off
                logger.warning("Rate limited — waiting %.1fs (attempt %d/%d)", wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                last_exc = exc
                continue
            if status == 413:
                logger.error(
                    "HTTP 413 Payload Too Large — this is typically caused by a corporate "
                    "proxy blocking POST requests to api.newrelic.com. "
                    "Ask IT to whitelist api.newrelic.com, or set NO_PROXY=api.newrelic.com "
                    "in your .env if the host can reach it directly."
                )
            else:
                logger.error("HTTP %d error: %s", status, exc)
            raise

        except Exception as exc:
            logger.error("Unexpected error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
            last_exc = exc

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY * attempt)

    raise RuntimeError(f"All {MAX_RETRIES} attempts failed") from last_exc


# ── Public API ─────────────────────────────────────────────────────────────────

def _parse_browser_row(row: dict, window_epoch: int) -> dict:
    """Convert one NerdGraph browser-FACET result row into a DB-ready dict."""
    facet: list = row.get("facet", [])

    def facet_val(idx: int) -> str:
        if idx >= len(facet):
            return ""
        v = facet[idx]
        return str(v) if v is not None else ""

    lcp_avg  = _g(row, "lcp_avg",  "average.largestContentfulPaint")
    cls_avg  = _g(row, "cls_avg",  "average.cumulativeLayoutShift")
    inp_avg  = _g(row, "inp_avg",  "average.interactionToNextPaint")
    fcp_avg  = _g(row, "fcp_avg",  "average.firstContentfulPaint")
    ttfb_avg = _g(row, "ttfb_avg", "average.timeToFirstByte")

    return {
        "timestamp":            window_epoch,
        "targetGroupedUrl":     facet_val(0),
        "userAgentName":        facet_val(1),
        "userAgentVersion":     facet_val(2),
        "largestContentfulPaint": lcp_avg,
        "lcp_p75": _pct_if(row, "largestContentfulPaint", 75, lcp_avg),
        "cumulativeLayoutShift": cls_avg,
        "cls_p75": _pct_if(row, "cumulativeLayoutShift", 75, cls_avg),
        "interactionToNextPaint": inp_avg,
        "inp_p75": _pct_if(row, "interactionToNextPaint", 75, inp_avg),
        "firstContentfulPaint": fcp_avg,
        "fcp_p75": _pct_if(row, "firstContentfulPaint", 75, fcp_avg),
        "timeToFirstByte": ttfb_avg,
        "ttfb_p75": _pct_if(row, "timeToFirstByte", 75, ttfb_avg),
        "sample_count": row.get("sample_count") or row.get("count"),
    }


def _parse_url_row(row: dict, window_epoch: int) -> dict:
    """Convert one NerdGraph URL-only FACET result row into a DB-ready dict."""
    facet = row.get("facet", "")
    # Single FACET → NR returns a plain string; multi-FACET → list.
    url = facet if isinstance(facet, str) else (facet[0] if facet else "")
    url = url if url is not None else ""

    lcp_avg  = _g(row, "lcp_avg",  "average.largestContentfulPaint")
    cls_avg  = _g(row, "cls_avg",  "average.cumulativeLayoutShift")
    inp_avg  = _g(row, "inp_avg",  "average.interactionToNextPaint")
    fcp_avg  = _g(row, "fcp_avg",  "average.firstContentfulPaint")
    ttfb_avg = _g(row, "ttfb_avg", "average.timeToFirstByte")

    return {
        "timestamp":            window_epoch,
        "targetGroupedUrl":     url,

        "largestContentfulPaint": lcp_avg,
        "lcp_p75": _pct_if(row, "largestContentfulPaint", 75, lcp_avg),
        "lcp_p90": _pct_if(row, "largestContentfulPaint", 90, lcp_avg),
        "lcp_p95": _pct_if(row, "largestContentfulPaint", 95, lcp_avg),

        "cumulativeLayoutShift": cls_avg,
        "cls_p75": _pct_if(row, "cumulativeLayoutShift", 75, cls_avg),
        "cls_p90": _pct_if(row, "cumulativeLayoutShift", 90, cls_avg),
        "cls_p95": _pct_if(row, "cumulativeLayoutShift", 95, cls_avg),

        "interactionToNextPaint": inp_avg,
        "inp_p75": _pct_if(row, "interactionToNextPaint", 75, inp_avg),
        "inp_p90": _pct_if(row, "interactionToNextPaint", 90, inp_avg),
        "inp_p95": _pct_if(row, "interactionToNextPaint", 95, inp_avg),

        "firstContentfulPaint": fcp_avg,
        "fcp_p75": _pct_if(row, "firstContentfulPaint", 75, fcp_avg),
        "fcp_p90": _pct_if(row, "firstContentfulPaint", 90, fcp_avg),
        "fcp_p95": _pct_if(row, "firstContentfulPaint", 95, fcp_avg),

        "timeToFirstByte": ttfb_avg,
        "ttfb_p75": _pct_if(row, "timeToFirstByte", 75, ttfb_avg),
        "ttfb_p90": _pct_if(row, "timeToFirstByte", 90, ttfb_avg),
        "ttfb_p95": _pct_if(row, "timeToFirstByte", 95, ttfb_avg),

        "firstPaint":  _g(row, "fp_avg",  "average.firstPaint"),
        "windowLoad":  _g(row, "wl_avg",  "average.windowLoad"),

        "sample_count": row.get("sample_count") or row.get("count"),
    }


def fetch_window(since_epoch: int, until_epoch: int) -> list[dict]:
    """
    Fetch Web Vitals for one 6-hour window [since_epoch, until_epoch).

    Returns a list of dicts ready to pass to db.upsert_vitals().
    Returns an empty list if NR has no data for the window.
    """
    nrql = _build_nrql(since_epoch, until_epoch)
    logger.debug("NRQL: %s", nrql)

    rows = _post_nerdgraph(nrql)
    records = [_parse_row(r, since_epoch) for r in rows]

    logger.info(
        "Window %d→%d: %d faceted rows from NR",
        since_epoch, until_epoch, len(records),
    )
    return records


def fetch_browser_window(since_epoch: int, until_epoch: int) -> list[dict]:
    """
    Fetch browser-level Web Vitals for one 6-hour window.

    Returns a list of dicts ready to pass to db.upsert_browser_vitals().
    """
    nrql = _build_browser_nrql(since_epoch, until_epoch)
    logger.debug("Browser NRQL: %s", nrql)

    rows = _post_nerdgraph(nrql)
    records = [_parse_browser_row(r, since_epoch) for r in rows]

    logger.info(
        "Window %d→%d: %d browser rows from NR",
        since_epoch, until_epoch, len(records),
    )
    return records


def fetch_url_window(since_epoch: int, until_epoch: int) -> list[dict]:
    """
    Fetch URL-level Web Vitals for one 6-hour window (no device/connection split).

    Returns a list of dicts ready to pass to db.upsert_url_vitals().
    Produces accurate overall percentiles matching NR's Web Vitals view.
    """
    nrql = _build_url_nrql(since_epoch, until_epoch)
    logger.debug("URL NRQL: %s", nrql)

    rows = _post_nerdgraph(nrql)
    records = [_parse_url_row(r, since_epoch) for r in rows]

    logger.info(
        "Window %d→%d: %d URL rows from NR",
        since_epoch, until_epoch, len(records),
    )
    return records
