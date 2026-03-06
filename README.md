# Web Vitals Collector & Dashboard

Collects Core Web Vitals from **New Relic** on a configurable schedule (default: every 6 hours), stores them in a local SQLite database, and visualises them in an interactive **Streamlit** dashboard.

## Quick Start

```bash
# 1. Clone and install dependencies
git clone <repo-url>
cd web_vitals
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/pip install -e .   # optional — enables the short `wv` alias

# 2. Fill in credentials
cp .env.example .env
# Edit .env and set:
#   NEW_RELIC_ACCOUNT_ID=<number from one.newrelic.com/accounts/XXXXXXX>
#   NEW_RELIC_API_KEY=NRAK-...

# 3. One-time data import (last 30 days)
venv/bin/python manage.py init

# 4. Register the cron job (updates every 6 hours)
venv/bin/python manage.py cron install

# 5. Start the dashboard
venv/bin/python manage.py dashboard start

# 6. (Optional) Enable dashboard autostart via systemd
venv/bin/python manage.py dashboard autostart install
systemctl --user status web-vitals-dashboard
```

After step 5 the dashboard is available at **http://localhost:8501**.
After step 6 it starts automatically on login.

## Features

- **Automated collection** — cron job fetches data on a configurable interval (`INTERVAL_HOURS`, default: 6h) via New Relic NerdGraph (GraphQL)
- **30-day backfill** — one-time historical import on first setup
- **Incremental updates** — syncs from the last stored window to now; idempotent upserts
- **Per-dimension storage** — broken down by URL, device type, connection type, and navigation type
- **Percentile tracking** — p75 / p90 / p95 alongside averages for LCP, CLS, INP, FCP
- **Interactive dashboard** — time-series charts, device/connection breakdowns, URL rankings, per-URL deep-dive
- **Self-healing database** — `db check` auto-repairs a corrupted SQLite file

## Requirements

- **Python 3.11+**
- **New Relic** account with a **User API Key** (`NRAK-…`) and the Browser agent installed on your site
- **System packages** (Debian/Ubuntu — usually pre-installed, but may be missing on minimal/container images):
  ```bash
  sudo apt install -y ca-certificates openssl
  sudo update-ca-certificates
  ```

## Setup

```bash
git clone <repo-url>
cd web_vitals

python3 -m venv venv
venv/bin/pip install -r requirements.txt

cp .env.example .env
# Edit .env — fill in NEW_RELIC_ACCOUNT_ID and NEW_RELIC_API_KEY
```

**Find your account ID** in the NR URL: `one.newrelic.com/accounts/XXXXXXX`
**Create a User API Key** at: New Relic → Profile → API Keys

```bash
# One-time: populate last 30 days
venv/bin/python manage.py init

# Register the 6-hour cron job
venv/bin/python manage.py cron install

# Start the dashboard
venv/bin/python manage.py dashboard start
```

> After `venv/bin/pip install -e .` you can use the shorter `wv` alias instead of `venv/bin/python manage.py`.

## CLI Reference

### Data collection

| Command | Description |
|---|---|
| `manage.py init` | One-time backfill (default: 30 days). Refuses to re-run if data exists. |
| `manage.py init --days 7` | Backfill a custom number of days. |
| `manage.py init --force` | Re-run backfill even if data already exists (upserts). |
| `manage.py update` | Incremental update from the last stored window to now. |

### Dashboard

| Command | Description |
|---|---|
| `manage.py dashboard start` | Start dashboard in background (port from `$STREAMLIT_PORT` or `8501`). |
| `manage.py dashboard start --port 9000` | Start on a custom port. |
| `manage.py dashboard stop` | Stop the background dashboard. |
| `manage.py dashboard autostart install` | Register a systemd user service — dashboard starts automatically on login. |
| `manage.py dashboard autostart remove` | Remove the systemd user service. |

### Cron job

| Command | Description |
|---|---|
| `manage.py cron install` | Register the 6-hour cron job. Idempotent — replaces any existing entry. |
| `manage.py cron remove` | Remove the cron job. |
| `manage.py cron status` | Show whether the cron job is installed and recent log output. |

### Database

| Command | Description |
|---|---|
| `manage.py db check` | SQLite integrity check with automatic repair on failure. |

## Architecture

```
New Relic NerdGraph API  (api.newrelic.com/graphql)
  NRQL on PageViewTiming, FACET url × device × networkEffectiveType
        │
        ▼
  nr_client.py  — HTTP + retry + rate-limiting
  Parses avg + p75/p90/p95 for each dimension combination
        │
        ▼
  db.py  — SQLite WAL mode
  INSERT … ON CONFLICT DO UPDATE  (idempotent upsert)
  data/web_vitals.db
        │
        ▼
  dashboard.py  — Streamlit + Plotly
  http://host:8501
```

### Data flow

```
cron (every INTERVAL_HOURS, default 6h)
  └─► updater.py
        ├─ get_last_timestamp()       ← find where we left off
        ├─ fetch_window(since, until) ← query NR for one interval window
        ├─ upsert_vitals(records)     ← write to SQLite
        └─ repeat for each missing window
```

All windows align to UTC boundaries (00:00, 06:00, 12:00, 18:00). The `timestamp` column stores the window **start** as a Unix epoch integer.

## Database Schema

Three tables in `data/web_vitals.db`:

### `vitals` — faceted by device & network

| Column group | Columns |
|---|---|
| **Window key** | `timestamp`, `targetGroupedUrl`, `deviceType`, `connectionType`, `navigationType` |
| **LCP** | `largestContentfulPaint`, `lcp_p75`, `lcp_p90`, `lcp_p95` |
| **CLS** | `cumulativeLayoutShift`, `cls_p75`, `cls_p90`, `cls_p95` |
| **INP** | `interactionToNextPaint`, `inp_p75`, `inp_p90`, `inp_p95` |
| **FCP** | `firstContentfulPaint`, `fcp_p75`, `fcp_p90`, `fcp_p95` |
| **TTFB** | `timeToFirstByte`, `ttfb_p75`, `ttfb_p90`, `ttfb_p95` |
| **Other** | `firstPaint`, `windowLoad`, `elementSize` |
| **Volume** | `sample_count` |

**Unique constraint:** `(timestamp, targetGroupedUrl, deviceType, connectionType, navigationType)`
**Indexes:** composite `(timestamp, targetGroupedUrl, deviceType)`, composite `(timestamp, connectionType)`

### `vitals_url` — URL-level aggregation (no device/network split)

Accurate overall percentiles matching New Relic's Web Vitals view. Same metric columns as `vitals` (without `deviceType`, `connectionType`, `navigationType`, `elementSize`).

**Unique constraint:** `(timestamp, targetGroupedUrl)`

### `vitals_browser` — browser & version breakdown

Stores avg + p75 for core metrics, faceted by `userAgentName` and `userAgentVersion`.

**Unique constraint:** `(timestamp, targetGroupedUrl, userAgentName, userAgentVersion)`

## Dashboard Tabs

| Tab | Contents |
|---|---|
| **Overview** | CWV gauge cards (LCP/INP/CLS) with value, status, Good/NI/Poor %; secondary KPI cards (FCP/TTFB/FP/Load); trend charts with threshold lines; page-view volume |
| **Breakdowns** | Avg metric by device type, connection type, navigation type; device × connection heatmap |
| **Page URLs** | Sortable table of URLs with pinned priority pages; configurable page size (50/100/200); links into Page Analysis |
| **Page Analysis** | Multi-URL trend comparison; per-URL summary table; full metric deep-dive for a single URL |

### CWV thresholds

| Metric | Good | Needs Improvement | Poor |
|---|---|---|---|
| LCP | ≤ 2500 ms | 2500–4000 ms | > 4000 ms |
| INP | ≤ 200 ms | 200–500 ms | > 500 ms |
| CLS | ≤ 0.10 | 0.10–0.25 | > 0.25 |
| FCP | ≤ 1800 ms | 1800–3000 ms | > 3000 ms |
| TTFB | ≤ 800 ms | 800–1800 ms | > 1800 ms |

## Project Structure

```
web_vitals/
├── manage.py               # CLI entry point (all commands)
├── requirements.txt        # Python dependencies
├── pyproject.toml          # Package metadata + ruff config
├── .env                    # Credentials (git-ignored)
├── .env.example            # Credentials template
│
├── scripts/
│   ├── schema.py           # SQL DDL — no external dependencies
│   ├── config.py           # Env loading + logging setup
│   ├── db.py               # SQLite connection, schema init, upsert helpers
│   ├── nr_client.py        # New Relic NerdGraph client (retry, rate-limit)
│   ├── backfill_insights.py  # One-time historical import
│   ├── updater.py          # Incremental updater (run by cron)
│   ├── dashboard.py        # Streamlit entry point (thin wrapper)
│   └── dashboard/          # Dashboard package
│       ├── app.py
│       ├── constants.py    # CWV thresholds, metric names, colours
│       ├── formatters.py
│       ├── data.py         # SQLite queries (cached)
│       ├── charts.py       # Plotly chart builders
│       ├── components.py   # kpi_card, cwv_gauge_card
│       ├── sidebar.py
│       └── tabs/
│           ├── overview.py
│           ├── breakdowns.py
│           ├── top_pages.py
│           └── page_analysis.py
│
└── data/                   # Runtime data — git-ignored
    ├── web_vitals.db       # SQLite database
    ├── dashboard.pid       # PID of running dashboard (when active)
    ├── logs/               # Dated log files + cron.log
    └── exports/            # Optional JSON exports
```

## Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `NEW_RELIC_ACCOUNT_ID` | yes | — | Numeric NR account ID |
| `NEW_RELIC_API_KEY` | yes | — | User API Key (`NRAK-…`) |
| `NR_EVENT_TYPE` | | `PageViewTiming` | NR event type for Web Vitals |
| `NR_APP_NAME` | | _(all apps)_ | Filter to a specific NR Browser application |
| `NR_COUNTRY_CODE` | | _(all countries)_ | Restrict collection to one country (ISO 3166-1 alpha-2) |
| `DATA_DIR` | | `data/` | Root directory for all runtime data |
| `DB_PATH` | | `data/web_vitals.db` | SQLite database path |
| `LOG_DIR` | | `data/logs/` | Log file directory |
| `EXPORTS_DIR` | | `data/exports/` | JSON export directory |
| `BACKFILL_DAYS` | | `30` | Days to backfill on `init` |
| `INTERVAL_HOURS` | | `6` | Collection window size |
| `REQUEST_TIMEOUT` | | `60` | NerdGraph request timeout (seconds) |
| `MAX_RETRIES` | | `3` | HTTP retry attempts |
| `RETRY_DELAY` | | `2.0` | Seconds between retries |
| `REQUEST_SLEEP` | | `0.4` | Delay between API requests (rate-limit guard) |
| `NR_SSL_VERIFY` | | `true` | SSL verification: `true`, `false`, or path to CA bundle |
| `NR_BYPASS_PROXY` | | `false` | Bypass system proxy for NR requests (fixes HTTP 413) |
| `NR_HTTPS_PROXY` | | _(system)_ | Route NR requests through a specific proxy URL |
| `STREAMLIT_PORT` | | `8501` | Dashboard port |
| `PINNED_URL_PATHS` | | _(none)_ | Comma-separated URL paths pinned to top of Page URLs table |

## Cron Job

`cron install` generates the schedule from `INTERVAL_HOURS` (default: `6`):

```
5 */<INTERVAL_HOURS> * * *  /path/to/venv/bin/python /path/to/scripts/updater.py >> data/logs/cron.log 2>&1
```

Fires at **:05** past each aligned hour. The 5-minute offset ensures the previous window is fully indexed in New Relic before querying.

> `INTERVAL_HOURS` must be a divisor of 24 (1, 2, 3, 4, 6, 8, 12, 24). The default is `6`.

## Dashboard Autostart (systemd)

`dashboard autostart install` creates a systemd user service at `~/.config/systemd/user/web-vitals-dashboard.service`. The dashboard starts automatically when the user logs in and restarts on failure.

```bash
# Check service status
systemctl --user status web-vitals-dashboard

# View logs
journalctl --user -u web-vitals-dashboard -f

# Remove autostart
venv/bin/python manage.py dashboard autostart remove
```

## Troubleshooting

**`NEW_RELIC_ACCOUNT_ID is not set`**
Edit `.env` and add your numeric account ID (`one.newrelic.com/accounts/XXXXXXX`).

**`init` exits with "DB already contains N rows"**
Expected — `init` is a one-time command. Use `manage.py update` for ongoing collection, or `manage.py init --force` to re-run.

**No data in the dashboard after `init`**
Check `data/logs/backfill_YYYYMMDD.log` for API errors. Common causes: wrong account ID, missing permissions, or `PageViewTiming` events don't exist (try `NR_EVENT_TYPE=BrowserInteraction`).

**Dashboard shows stale data**
Run `manage.py update` manually, or check cron status with `manage.py cron status`.

**Database integrity error**
Run `manage.py db check` — it attempts automatic repair and creates a timestamped `.bak` before modifying anything.
