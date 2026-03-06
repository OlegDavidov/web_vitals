#!/usr/bin/env python3
"""
One-time backfill: collect Web Vitals for the last N days (default: 30).

Usage:
    python scripts/backfill_insights.py            # backfill last 30 days
    python scripts/backfill_insights.py --days 7   # backfill last 7 days
    python scripts/backfill_insights.py --force     # re-fetch even if data exists

Run once after initial setup, then switch to updater.py for ongoing collection.
"""
import argparse
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

# Allow running directly: python scripts/backfill_insights.py
sys.path.insert(0, str(Path(__file__).parent))

from config import setup_logging, BACKFILL_DAYS, INTERVAL_HOURS, REQUEST_SLEEP
from db import init_db, upsert_vitals, upsert_browser_vitals, upsert_url_vitals, get_row_count
from nr_client import fetch_window, fetch_browser_window, fetch_url_window

logger = setup_logging("backfill")


def _build_windows(days: int) -> list[tuple[int, int]]:
    """Return list of (since, until) epoch tuples for `days` days of INTERVAL_HOURS windows."""
    interval = INTERVAL_HOURS * 3600
    now = datetime.now(timezone.utc)
    end_epoch = int(now.timestamp())

    # Align start to a clean interval boundary (UTC)
    start = now - timedelta(days=days)
    start_epoch = int(start.timestamp())
    start_epoch -= start_epoch % interval

    windows = []
    t = start_epoch
    while t + interval <= end_epoch:
        windows.append((t, t + interval))
        t += interval
    return windows


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill Web Vitals from New Relic")
    parser.add_argument(
        "--days", type=int, default=BACKFILL_DAYS,
        help=f"Number of days to backfill (default: {BACKFILL_DAYS})",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-fetch and overwrite existing data",
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Web Vitals Backfill — last %d days", args.days)
    logger.info("=" * 60)

    init_db()
    existing_rows = get_row_count()
    logger.info("Existing rows in DB: %d", existing_rows)

    windows = _build_windows(args.days)
    total = len(windows)
    if total == 0:
        logger.warning("No complete windows to fetch for %d day(s). Nothing to do.", args.days)
        return
    logger.info(
        "Windows to fetch: %d  (%s → %s UTC)",
        total,
        datetime.fromtimestamp(windows[0][0], tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
        datetime.fromtimestamp(windows[-1][1], tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
    )

    total_records = 0
    failed_windows = 0

    for i, (since, until) in enumerate(windows, 1):
        since_dt = datetime.fromtimestamp(since, tz=timezone.utc)
        logger.info(
            "[%d/%d] %s UTC",
            i, total, since_dt.strftime("%Y-%m-%d %H:%M"),
        )

        try:
            records = fetch_window(since, until)
            if records:
                count = upsert_vitals(records)
                total_records += count
                logger.info("  → %d rows saved", count)
            else:
                logger.info("  → no data")

            time.sleep(REQUEST_SLEEP)
            url_records = fetch_url_window(since, until)
            if url_records:
                ucount = upsert_url_vitals(url_records)
                total_records += ucount
                logger.info("  → %d URL rows saved", ucount)

            time.sleep(REQUEST_SLEEP)
            browser_records = fetch_browser_window(since, until)
            if browser_records:
                bcount = upsert_browser_vitals(browser_records)
                total_records += bcount
                logger.info("  → %d browser rows saved", bcount)
        except Exception as exc:
            failed_windows += 1
            logger.error("  → FAILED: %s", exc)

        # Rate limit: ~0.4s between requests (~2.5 req/s, safely under NR's 3 req/s limit)
        if i < total:
            time.sleep(REQUEST_SLEEP)

    logger.info("=" * 60)
    logger.info(
        "Backfill complete — %d records saved, %d windows failed",
        total_records, failed_windows,
    )
    if failed_windows:
        logger.warning("Re-run with --force to retry failed windows.")
    logger.info("=" * 60)

    sys.exit(1 if failed_windows == total else 0)


if __name__ == "__main__":
    main()
