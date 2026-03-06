"""
SQLite schema DDL — imported by db.py and manage.py (no other dependencies).
Keeping it separate avoids circular import issues and lets manage.py
use the schema without loading the New Relic config.
"""

# Standalone CREATE statements used by manage.py migration to create
# tables that were added after the initial schema release.
# These are intentionally separate from SCHEMA to avoid fragile string parsing.
CREATE_VITALS_BROWSER = """
CREATE TABLE IF NOT EXISTS vitals_browser (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp               INTEGER NOT NULL,
    targetGroupedUrl        TEXT    NOT NULL DEFAULT '',
    userAgentName           TEXT    NOT NULL DEFAULT '',
    userAgentVersion        TEXT    NOT NULL DEFAULT '',

    largestContentfulPaint  REAL,
    lcp_p75                 REAL,
    cumulativeLayoutShift   REAL,
    cls_p75                 REAL,
    interactionToNextPaint  REAL,
    inp_p75                 REAL,
    firstContentfulPaint    REAL,
    fcp_p75                 REAL,
    timeToFirstByte         REAL,
    ttfb_p75                REAL,
    sample_count            INTEGER,

    UNIQUE (timestamp, targetGroupedUrl, userAgentName, userAgentVersion)
);
CREATE INDEX IF NOT EXISTS idx_vb_ts_url
    ON vitals_browser (timestamp, targetGroupedUrl);
"""

CREATE_VITALS_URL = """
CREATE TABLE IF NOT EXISTS vitals_url (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp               INTEGER NOT NULL,
    targetGroupedUrl        TEXT    NOT NULL DEFAULT '',

    largestContentfulPaint  REAL,
    lcp_p75                 REAL,
    lcp_p90                 REAL,
    lcp_p95                 REAL,

    cumulativeLayoutShift   REAL,
    cls_p75                 REAL,
    cls_p90                 REAL,
    cls_p95                 REAL,

    interactionToNextPaint  REAL,
    inp_p75                 REAL,
    inp_p90                 REAL,
    inp_p95                 REAL,

    firstContentfulPaint    REAL,
    fcp_p75                 REAL,
    fcp_p90                 REAL,
    fcp_p95                 REAL,

    timeToFirstByte         REAL,
    ttfb_p75                REAL,
    ttfb_p90                REAL,
    ttfb_p95                REAL,

    firstPaint              REAL,
    windowLoad              REAL,

    sample_count            INTEGER,

    UNIQUE (timestamp, targetGroupedUrl)
);
CREATE INDEX IF NOT EXISTS idx_vu_ts
    ON vitals_url (timestamp);
CREATE INDEX IF NOT EXISTS idx_vu_url
    ON vitals_url (targetGroupedUrl);
"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS vitals (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,

    -- Window identifier
    timestamp               INTEGER NOT NULL,   -- epoch seconds, window START
    targetGroupedUrl        TEXT    NOT NULL DEFAULT '',
    deviceType              TEXT    NOT NULL DEFAULT '',
    connectionType          TEXT    NOT NULL DEFAULT '',  -- NR: networkEffectiveType (4g/3g/2g/slow-2g)
    navigationType          TEXT    NOT NULL DEFAULT '',  -- unused (kept for UNIQUE constraint compat)

    -- Largest Contentful Paint  (seconds in NR)
    largestContentfulPaint  REAL,
    lcp_p75                 REAL,
    lcp_p90                 REAL,
    lcp_p95                 REAL,

    -- Cumulative Layout Shift  (score, unitless)
    cumulativeLayoutShift   REAL,
    cls_p75                 REAL,
    cls_p90                 REAL,
    cls_p95                 REAL,

    -- Interaction to Next Paint  (seconds in NR)
    interactionToNextPaint  REAL,
    inp_p75                 REAL,
    inp_p90                 REAL,
    inp_p95                 REAL,

    -- First Contentful Paint  (seconds in NR)
    firstContentfulPaint    REAL,
    fcp_p75                 REAL,
    fcp_p90                 REAL,
    fcp_p95                 REAL,

    -- Time to First Byte  (ms in NR)
    timeToFirstByte         REAL,
    ttfb_p75                REAL,
    ttfb_p90                REAL,
    ttfb_p95                REAL,

    -- First Paint  (seconds in NR)
    firstPaint              REAL,

    -- Window Load  (seconds in NR)
    windowLoad              REAL,

    -- LCP element size  (px², unitless)
    elementSize             REAL,

    -- Observation count for this window/dimension combination
    sample_count            INTEGER,

    UNIQUE (timestamp, targetGroupedUrl, deviceType, connectionType, navigationType)
);

-- Composite: covers timestamp-range + URL + device filter (most common pattern).
-- Also serves as a timestamp-only index (SQLite uses leftmost prefix).
CREATE INDEX IF NOT EXISTS idx_vitals_ts_url_device
    ON vitals (timestamp, targetGroupedUrl, deviceType);

-- Composite: covers timestamp-range + network filter
CREATE INDEX IF NOT EXISTS idx_vitals_ts_connection
    ON vitals (timestamp, connectionType);

-- Browser-level breakdown (separate FACET query from NR)
CREATE TABLE IF NOT EXISTS vitals_browser (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp               INTEGER NOT NULL,
    targetGroupedUrl        TEXT    NOT NULL DEFAULT '',
    userAgentName           TEXT    NOT NULL DEFAULT '',
    userAgentVersion        TEXT    NOT NULL DEFAULT '',

    largestContentfulPaint  REAL,
    lcp_p75                 REAL,
    cumulativeLayoutShift   REAL,
    cls_p75                 REAL,
    interactionToNextPaint  REAL,
    inp_p75                 REAL,
    firstContentfulPaint    REAL,
    fcp_p75                 REAL,
    timeToFirstByte         REAL,
    ttfb_p75                REAL,
    sample_count            INTEGER,

    UNIQUE (timestamp, targetGroupedUrl, userAgentName, userAgentVersion)
);

CREATE INDEX IF NOT EXISTS idx_vb_ts_url
    ON vitals_browser (timestamp, targetGroupedUrl);

-- URL-level aggregation (no device/connection faceting).
-- Provides accurate overall percentiles that match NR's Web Vitals view.
CREATE TABLE IF NOT EXISTS vitals_url (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp               INTEGER NOT NULL,
    targetGroupedUrl        TEXT    NOT NULL DEFAULT '',

    largestContentfulPaint  REAL,
    lcp_p75                 REAL,
    lcp_p90                 REAL,
    lcp_p95                 REAL,

    cumulativeLayoutShift   REAL,
    cls_p75                 REAL,
    cls_p90                 REAL,
    cls_p95                 REAL,

    interactionToNextPaint  REAL,
    inp_p75                 REAL,
    inp_p90                 REAL,
    inp_p95                 REAL,

    firstContentfulPaint    REAL,
    fcp_p75                 REAL,
    fcp_p90                 REAL,
    fcp_p95                 REAL,

    timeToFirstByte         REAL,
    ttfb_p75                REAL,
    ttfb_p90                REAL,
    ttfb_p95                REAL,

    firstPaint              REAL,
    windowLoad              REAL,

    sample_count            INTEGER,

    UNIQUE (timestamp, targetGroupedUrl)
);

CREATE INDEX IF NOT EXISTS idx_vu_ts
    ON vitals_url (timestamp);
CREATE INDEX IF NOT EXISTS idx_vu_url
    ON vitals_url (targetGroupedUrl);
"""

# Columns added after the initial schema release.
# manage.py uses this list to ALTER TABLE on existing databases.
MIGRATION_COLUMNS = {
    "ttfb_p75":    "REAL",
    "ttfb_p90":    "REAL",
    "ttfb_p95":    "REAL",
    "firstPaint":  "REAL",
    "windowLoad":  "REAL",
    "elementSize": "REAL",
}
