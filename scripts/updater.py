#!/usr/bin/env python3
"""
Incremental updater — runs periodically via cron (default: every 6 hours).

Logic:
  1. Find the latest window timestamp already stored in the DB.
  2. Fetch every complete window (size = INTERVAL_HOURS) from (last + interval) up to now.
  3. Insert/update records.

If the DB is empty it fetches the last 2 intervals and warns the user
to run backfill_insights.py for full historical data.

Register with: python manage.py cron install
"""
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import setup_logging, INTERVAL_HOURS, REQUEST_SLEEP
from db import init_db, upsert_vitals, upsert_browser_vitals, upsert_url_vitals, get_last_timestamp
from nr_client import fetch_window, fetch_browser_window, fetch_url_window

logger = setup_logging("update")

INTERVAL_SECONDS = INTERVAL_HOURS * 3600


def _align_to_boundary(epoch: int) -> int:
    """Round epoch DOWN to the nearest interval boundary (UTC)."""
    return epoch - (epoch % INTERVAL_SECONDS)


def main() -> None:
    logger.info("=" * 60)
    logger.info("Web Vitals incremental update")
    logger.info("=" * 60)

    init_db()

    now_epoch = int(datetime.now(timezone.utc).timestamp())
    last_ts = get_last_timestamp()

    if last_ts is None:
        logger.warning(
            "DB is empty — fetching last %d hours. "
            "Run backfill_insights.py for full historical data.",
            INTERVAL_HOURS * 2,
        )
        # Start 2 intervals ago so we have at least one complete window
        start = _align_to_boundary(now_epoch - INTERVAL_SECONDS * 2)
    else:
        last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc)
        logger.info("Last stored window: %s UTC", last_dt.strftime("%Y-%m-%d %H:%M"))
        start = last_ts + INTERVAL_SECONDS  # next window after the last one stored

    # Collect all complete windows: [start, start+interval), ... up to now
    windows: list[tuple[int, int]] = []
    t = start
    while t + INTERVAL_SECONDS <= now_epoch:
        windows.append((t, t + INTERVAL_SECONDS))
        t += INTERVAL_SECONDS

    if not windows:
        logger.info("Already up to date — no new complete windows to fetch.")
        return

    logger.info("New windows to fetch: %d", len(windows))

    total_records = 0
    failed_windows = 0

    for i, (since, until) in enumerate(windows, 1):
        since_dt = datetime.fromtimestamp(since, tz=timezone.utc)
        logger.info(
            "[%d/%d] %s UTC",
            i, len(windows), since_dt.strftime("%Y-%m-%d %H:%M"),
        )

        try:
            records = fetch_window(since, until)
            if records:
                count = upsert_vitals(records)
                total_records += count
                logger.info("  → %d rows saved", count)
            else:
                logger.info("  → no data")

            # URL-level data (accurate overall percentiles, no device/connection split)
            time.sleep(REQUEST_SLEEP)
            url_records = fetch_url_window(since, until)
            if url_records:
                ucount = upsert_url_vitals(url_records)
                total_records += ucount
                logger.info("  → %d URL rows saved", ucount)

            # Browser-level data (separate NR query, sleep to respect rate limit)
            time.sleep(REQUEST_SLEEP)
            browser_records = fetch_browser_window(since, until)
            if browser_records:
                bcount = upsert_browser_vitals(browser_records)
                total_records += bcount
                logger.info("  → %d browser rows saved", bcount)
        except Exception as exc:
            failed_windows += 1
            logger.error("  → FAILED: %s", exc)

        if i < len(windows):
            time.sleep(REQUEST_SLEEP)

    logger.info("=" * 60)
    logger.info(
        "Update complete — %d records saved, %d windows failed",
        total_records, failed_windows,
    )
    logger.info("=" * 60)

    sys.exit(1 if failed_windows else 0)


if __name__ == "__main__":
    main()
