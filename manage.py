#!/usr/bin/env python3
"""
Web Vitals — project CLI.

Usage (from project root, after setting up the venv):
    python manage.py init              # one-time 30-day backfill
    python manage.py update            # incremental update to now
    python manage.py dashboard start              # start dashboard in background
    python manage.py dashboard stop               # stop background dashboard
    python manage.py dashboard autostart install  # systemd user service
    python manage.py dashboard autostart remove   # remove systemd user service
    python manage.py cron install      # register periodic cron job (INTERVAL_HOURS)
    python manage.py cron remove       # remove cron job
    python manage.py cron status       # show whether cron job is active
    python manage.py db check          # integrity check + auto-repair

If installed via `pip install -e .`:
    wv init / wv update / wv dashboard start|stop / wv cron install|remove|status / wv db check
"""
import argparse
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).parent.resolve()
SCRIPTS_DIR = BASE_DIR / "scripts"
VENV_DIR = BASE_DIR / "venv"
VENV_PYTHON = VENV_DIR / "bin" / "python"
VENV_STREAMLIT = VENV_DIR / "bin" / "streamlit"
DATA_DIR = BASE_DIR / "data"
DB_DEFAULT = DATA_DIR / "web_vitals.db"
PID_FILE = DATA_DIR / "dashboard.pid"
DASHBOARD_LOG = DATA_DIR / "logs" / "dashboard.log"
SYSTEMD_SERVICE_NAME = "web-vitals-dashboard"
SYSTEMD_SERVICE_DIR = Path.home() / ".config" / "systemd" / "user"

# ── Terminal colours (graceful fallback on no-TTY) ─────────────────────────────

_TTY = sys.stdout.isatty()
_G  = "\033[92m"   if _TTY else ""   # green
_Y  = "\033[93m"   if _TTY else ""   # yellow
_R  = "\033[91m"   if _TTY else ""   # red
_C  = "\033[96m"   if _TTY else ""   # cyan
_B  = "\033[1m"    if _TTY else ""   # bold
_RS = "\033[0m"    if _TTY else ""   # reset


def _ok(msg: str)    -> None: print(f"{_G}✓{_RS} {msg}", flush=True)
def _warn(msg: str)  -> None: print(f"{_Y}⚠{_RS}  {msg}", flush=True)
def _err(msg: str)   -> None: print(f"{_R}✗{_RS} {msg}", flush=True)   # stdout; avoids stderr/stdout interleave
def _info(msg: str)  -> None: print(f"{_C}→{_RS} {msg}", flush=True)
def _head(msg: str)  -> None: print(f"\n{_B}{msg}{_RS}", flush=True)
def _sep()           -> None: print("─" * 56)


# ── Env validation ─────────────────────────────────────────────────────────────

def _interval_hours() -> int:
    """Return INTERVAL_HOURS from env (default 6)."""
    _load_env()
    try:
        return max(1, int(os.getenv("INTERVAL_HOURS", "6")))
    except ValueError:
        return 6


def _cron_expression(interval: int) -> str:
    """
    Convert interval hours to a cron schedule string (minute field = 5).
    Works for any divisor of 24; for 24h uses '5 0 * * *'.
    """
    if interval >= 24:
        return "5 0 * * *"
    return f"5 */{interval} * * *"


def _load_env() -> None:
    """Load .env into os.environ (safe to call multiple times)."""
    try:
        from dotenv import load_dotenv
        load_dotenv(BASE_DIR / ".env", override=False)
    except ImportError:
        pass  # dotenv not installed yet — handled below


def check_env() -> list[str]:
    """
    Return a list of human-readable problems with the current configuration.
    Empty list = all good.
    """
    _load_env()
    issues: list[str] = []

    account_id = os.getenv("NEW_RELIC_ACCOUNT_ID", "").strip()
    if not account_id or account_id in ("YOUR_ACCOUNT_ID", "0", ""):
        issues.append(
            "NEW_RELIC_ACCOUNT_ID is not set.\n"
            "  Find it in your New Relic URL: one.newrelic.com/accounts/XXXXXXX\n"
            f"  Then add it to: {BASE_DIR / '.env'}"
        )

    api_key = os.getenv("NEW_RELIC_API_KEY", "").strip()
    if not api_key or "XXXXX" in api_key or not api_key.startswith("NRAK-"):
        issues.append(
            "NEW_RELIC_API_KEY is missing or looks like a placeholder.\n"
            "  Create a User API Key at: one.newrelic.com → Profile → API Keys\n"
            f"  Then add it to: {BASE_DIR / '.env'}"
        )

    return issues


def _assert_env() -> None:
    """Print config errors and exit if any are found."""
    issues = check_env()
    if not issues:
        return
    _err("Configuration problems found:")
    for issue in issues:
        print(f"\n  {_Y}•{_RS} {issue}")
    print()
    sys.exit(1)


def _assert_venv() -> None:
    """Exit if the virtual environment hasn't been created yet."""
    if not VENV_PYTHON.exists():
        _err("Virtual environment not found.")
        _info("Set it up first:")
        print(f"    python3 -m venv venv")
        print(f"    venv/bin/pip install -r requirements.txt")
        sys.exit(1)


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _db_path() -> Path:
    """Resolve DB path (respects DB_PATH env var if set)."""
    _load_env()
    return Path(os.getenv("DB_PATH", str(DB_DEFAULT)))


def _migrate_db(db: Path) -> None:
    """Add any missing columns/tables to an existing database (idempotent)."""
    sys.path.insert(0, str(SCRIPTS_DIR))
    from schema import MIGRATION_COLUMNS, CREATE_VITALS_BROWSER, CREATE_VITALS_URL  # noqa: PLC0415

    conn = sqlite3.connect(db, timeout=30)
    try:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(vitals)").fetchall()}
        added = []
        for col, col_type in MIGRATION_COLUMNS.items():
            if col not in existing:
                conn.execute(f"ALTER TABLE vitals ADD COLUMN {col} {col_type}")
                added.append(col)

        # Create tables that were added after the initial schema release
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        for table_name, ddl in [("vitals_browser", CREATE_VITALS_BROWSER),
                                 ("vitals_url", CREATE_VITALS_URL)]:
            if table_name not in tables:
                conn.executescript(ddl)
                added.append(f"{table_name} (table)")

        # Add new composite index for network-filter queries
        existing_indexes = {r[1] for r in conn.execute("PRAGMA index_list(vitals)").fetchall()}
        idx_changes: list[str] = []
        if "idx_vitals_ts_connection" not in existing_indexes:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_vitals_ts_connection ON vitals (timestamp, connectionType)")
            idx_changes.append("+idx_vitals_ts_connection")
        # Drop redundant single-column indexes (covered by composites)
        for old_idx in ("idx_vitals_timestamp", "idx_vitals_url", "idx_vitals_device"):
            if old_idx in existing_indexes:
                conn.execute(f"DROP INDEX IF EXISTS {old_idx}")
                idx_changes.append(f"-{old_idx}")

        conn.commit()
    finally:
        conn.close()
    if added or idx_changes:
        parts = []
        if added:
            parts.append(f"columns: {', '.join(added)}")
        if idx_changes:
            parts.append(f"indexes: {', '.join(idx_changes)}")
        _ok(f"Schema migrated — {'; '.join(parts)}")


def _db_stats(conn: sqlite3.Connection) -> dict:
    row = conn.execute(
        "SELECT COUNT(*), MIN(timestamp), MAX(timestamp), SUM(sample_count) FROM vitals"
    ).fetchone()
    return {
        "rows": row[0] or 0,
        "min_ts": row[1],
        "max_ts": row[2],
        "total_views": row[3] or 0,
    }


# ── Commands ───────────────────────────────────────────────────────────────────

def cmd_init(args: argparse.Namespace) -> None:
    """One-time 30-day backfill. Warns if data already exists."""
    _head("Web Vitals — init")
    _sep()

    _assert_venv()
    _assert_env()
    _ok("Environment configured")

    for subdir in ("exports", "logs"):
        (DATA_DIR / subdir).mkdir(parents=True, exist_ok=True)
    _ok(f"Directories ready: data/exports, data/logs")

    db = _db_path()
    if db.exists():
        try:
            conn = sqlite3.connect(db, timeout=30)
            stats = _db_stats(conn)
            conn.close()

            if stats["rows"] > 0:
                last_dt = datetime.fromtimestamp(stats["max_ts"], tz=timezone.utc)
                _warn(f"Database already contains {stats['rows']:,} rows.")
                _warn(f"Last entry : {last_dt.strftime('%Y-%m-%d %H:%M')} UTC")
                _warn("'init' is a one-time setup command.")
                print()

                if not args.force:
                    _info("To fetch only new data run:  python manage.py update")
                    _info("To re-run backfill anyway:   python manage.py init --force")
                    sys.exit(0)
                else:
                    _warn("--force flag set — re-running backfill (existing data will be updated).")
        except sqlite3.DatabaseError:
            # DB exists but is unreadable — let backfill recreate it
            pass

    # Migrate schema on existing DB before running backfill
    if db.exists():
        try:
            _migrate_db(db)
        except sqlite3.DatabaseError:
            pass

    extra: list[str] = []
    if args.days != 30:
        extra += ["--days", str(args.days)]
    if args.force:
        extra += ["--force"]

    _info(f"Backfilling last {args.days} days …")
    result = subprocess.run(
        [str(VENV_PYTHON), str(SCRIPTS_DIR / "backfill_insights.py")] + extra,
        cwd=str(BASE_DIR),
    )
    sys.exit(result.returncode)


def cmd_update(args: argparse.Namespace) -> None:
    """Incremental update: fetch from last stored window up to now."""
    _head("Web Vitals — update")
    _sep()

    _assert_venv()
    _assert_env()

    db = _db_path()
    if db.exists():
        try:
            _migrate_db(db)
            conn = sqlite3.connect(db, timeout=30)
            stats = _db_stats(conn)
            conn.close()

            if stats["rows"] == 0:
                _warn("Database is empty.")
                _info("Run 'python manage.py init' for the initial 30-day backfill.")
            else:
                min_dt = datetime.fromtimestamp(stats["min_ts"], tz=timezone.utc)
                max_dt = datetime.fromtimestamp(stats["max_ts"], tz=timezone.utc)
                _info(f"DB range  : {min_dt.strftime('%Y-%m-%d %H:%M')} → {max_dt.strftime('%Y-%m-%d %H:%M')} UTC")
                _info(f"Rows      : {stats['rows']:,}   |   Views: {stats['total_views']:,.0f}")
        except sqlite3.DatabaseError as e:
            _warn(f"Could not read DB stats: {e}")
    else:
        _warn("Database not found — will be created on first fetch.")

    print()
    result = subprocess.run(
        [str(VENV_PYTHON), str(SCRIPTS_DIR / "updater.py")],
        cwd=str(BASE_DIR),
    )
    sys.exit(result.returncode)


def _dashboard_port(args: argparse.Namespace) -> str:
    _load_env()
    return str(getattr(args, "port", None) or os.getenv("STREAMLIT_PORT", "8501"))


def _streamlit_argv(port: str) -> list[str]:
    return [
        "streamlit", "run",
        str(SCRIPTS_DIR / "dashboard.py"),
        "--server.port", port,
        "--server.address", "0.0.0.0",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
    ]


def _read_pid() -> int | None:
    """Return PID from PID file, or None if missing / process not running."""
    if not PID_FILE.exists():
        return None
    try:
        pid = int(PID_FILE.read_text().strip())
        os.kill(pid, 0)   # signal 0 = check existence only
        return pid
    except (ValueError, ProcessLookupError, PermissionError):
        PID_FILE.unlink(missing_ok=True)
        return None


def cmd_dashboard_start(args: argparse.Namespace) -> None:
    """Start the Streamlit dashboard (background by default, foreground with --foreground)."""
    _head("Web Vitals — dashboard start")
    _sep()

    _assert_venv()

    port = _dashboard_port(args)

    # Foreground mode: exec streamlit directly (used by systemd)
    if getattr(args, "foreground", False):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _ok(f"Starting dashboard in foreground on port {port}")
        os.execv(
            str(VENV_STREAMLIT),
            [str(VENV_STREAMLIT)] + _streamlit_argv(port)[1:],
        )

    pid = _read_pid()
    if pid:
        _warn(f"Dashboard is already running (PID {pid}).")
        _info(f"URL : http://localhost:{port}")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Launch a supervisor process that auto-restarts Streamlit on crash.
    supervisor_code = f"""
import subprocess, signal, sys, time, os
from pathlib import Path

cmd = {[str(VENV_STREAMLIT)] + _streamlit_argv(port)[1:]!r}
log_path = {str(DASHBOARD_LOG)!r}
pid_file = {str(PID_FILE)!r}
stop = False

def _handle_term(signum, frame):
    global stop
    stop = True

signal.signal(signal.SIGTERM, _handle_term)
signal.signal(signal.SIGINT, _handle_term)

while not stop:
    log_fh = open(log_path, "a")
    try:
        proc = subprocess.Popen(cmd, stdout=log_fh, stderr=log_fh)
        log_fh.write(f"[supervisor] started streamlit PID {{proc.pid}}\\n")
        log_fh.flush()
        while not stop:
            try:
                rc = proc.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                continue
        if stop:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            break
        log_fh.write(f"[supervisor] streamlit exited with code {{rc}}, restarting...\\n")
        log_fh.flush()
    finally:
        log_fh.close()
    time.sleep(1)

Path(pid_file).unlink(missing_ok=True)
"""
    proc = subprocess.Popen(
        [str(VENV_PYTHON), "-c", supervisor_code],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    PID_FILE.write_text(str(proc.pid))

    _ok(f"Dashboard started  (supervisor PID {proc.pid})")
    _info(f"URL  : http://localhost:{port}")
    _info(f"Log  : {DASHBOARD_LOG}")
    _info(f"Stop : python manage.py dashboard stop")


def cmd_dashboard_stop(args: argparse.Namespace) -> None:
    """Stop the background Streamlit dashboard."""
    _head("Web Vitals — dashboard stop")
    _sep()

    pid = _read_pid()
    if pid is None:
        _warn("Dashboard is not running (no PID file or process already gone).")
        return

    try:
        # Send SIGTERM to the whole process group (supervisor + streamlit)
        pgid = os.getpgid(pid)
        os.killpg(pgid, signal.SIGTERM)
        # Wait up to 5 s for graceful shutdown
        import time
        for _ in range(10):
            time.sleep(0.5)
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
        else:
            os.killpg(pgid, signal.SIGKILL)
            _warn("Process did not exit cleanly — sent SIGKILL.")
    except (ProcessLookupError, PermissionError):
        pass

    PID_FILE.unlink(missing_ok=True)
    _ok(f"Dashboard stopped  (PID {pid})")


def cmd_dashboard_autostart_install(args: argparse.Namespace) -> None:
    """Install a systemd user service that auto-starts the dashboard on login."""
    _head("Web Vitals — dashboard autostart install")
    _sep()

    _assert_venv()
    _load_env()
    port = os.getenv("STREAMLIT_PORT", "8501")

    SYSTEMD_SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    service_file = SYSTEMD_SERVICE_DIR / f"{SYSTEMD_SERVICE_NAME}.service"

    exec_start = " ".join(
        [str(VENV_STREAMLIT)] + _streamlit_argv(port)[1:]
    )

    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    service_content = f"""\
[Unit]
Description=Web Vitals Dashboard
After=network.target

[Service]
Type=simple
WorkingDirectory={BASE_DIR}
ExecStart={exec_start}
Restart=always
RestartSec=1
StandardOutput=append:{log_dir}/dashboard.log
StandardError=append:{log_dir}/dashboard.log

[Install]
WantedBy=default.target
"""
    service_file.write_text(service_content)
    _ok(f"Service file written: {service_file}")

    # Reload systemd and enable + start
    cmds = [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", SYSTEMD_SERVICE_NAME],
        ["systemctl", "--user", "start",  SYSTEMD_SERVICE_NAME],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            _err(f"Command failed: {' '.join(cmd)}")
            if result.stderr:
                print(f"    {result.stderr.strip()}")
            sys.exit(1)

    _ok("Dashboard enabled and started via systemd")
    _info(f"URL  : http://localhost:{port}")
    _info("Check status : systemctl --user status web-vitals-dashboard")
    _info("Remove       : python manage.py dashboard autostart remove")


def cmd_dashboard_autostart_remove(args: argparse.Namespace) -> None:
    """Remove the systemd user service for the dashboard."""
    _head("Web Vitals — dashboard autostart remove")
    _sep()

    service_file = SYSTEMD_SERVICE_DIR / f"{SYSTEMD_SERVICE_NAME}.service"

    if not service_file.exists():
        _warn("Systemd service file not found — nothing to remove.")
        return

    for cmd in [
        ["systemctl", "--user", "stop",    SYSTEMD_SERVICE_NAME],
        ["systemctl", "--user", "disable", SYSTEMD_SERVICE_NAME],
    ]:
        subprocess.run(cmd, capture_output=True)  # best-effort

    service_file.unlink()
    subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)

    _ok("Autostart removed.")
    _info("Service file deleted and systemd daemon reloaded.")


# ── Cron sub-commands ──────────────────────────────────────────────────────────

def cmd_cron_install(args: argparse.Namespace) -> None:
    """Install or update the periodic cron job (idempotent)."""
    _head("Web Vitals — cron install")
    _sep()

    interval = _interval_hours()
    updater = SCRIPTS_DIR / "updater.py"
    log_dir = DATA_DIR / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "cron.log"

    schedule = _cron_expression(interval)
    new_line = f"{schedule} {VENV_PYTHON} {updater} >> {log_file} 2>&1"

    # Read current crontab (ignore error if empty)
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    current_lines: list[str] = result.stdout.splitlines() if result.returncode == 0 else []

    # Strip any existing entry for this updater script (dedup + update in one step)
    filtered = [line for line in current_lines if str(updater) not in line]
    had_existing = len(filtered) < len(current_lines)

    new_crontab = "\n".join(filtered + [new_line]) + "\n"

    proc = subprocess.run(["crontab", "-"], input=new_crontab, text=True)
    if proc.returncode != 0:
        _err("Failed to write crontab.")
        sys.exit(1)

    if had_existing:
        _ok("Cron job updated  (old entry replaced)")
    else:
        _ok("Cron job installed")

    fire_hours = ", ".join(f"{h:02d}:05" for h in range(0, 24, interval))
    _info(f"Interval : every {interval}h  (INTERVAL_HOURS={interval})")
    _info(f"Schedule : {schedule}  →  fires at {fire_hours} UTC")
    _info(f"Log file : {log_file}")
    print()
    _info("Active crontab:")
    subprocess.run(["crontab", "-l"])


def _next_cron_run() -> str:
    """Return the next wall-clock time for the configured cron schedule (UTC)."""
    interval = _interval_hours()
    now = datetime.now(timezone.utc)
    for day_offset in range(2):
        for hour in range(0, 24, interval):
            candidate = now.replace(
                hour=hour, minute=5, second=0, microsecond=0
            ) + timedelta(days=day_offset)
            if candidate > now:
                return candidate.strftime("%Y-%m-%d %H:%M UTC")
    return "unknown"


def cmd_cron_status(args: argparse.Namespace) -> None:
    """Show whether the Web Vitals cron job is installed, last run info, and recent log."""
    _head("Web Vitals — cron status")
    _sep()

    updater = str(SCRIPTS_DIR / "updater.py")

    # ── 1. Cron entry ─────────────────────────────────────────────────────────
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        _warn("No crontab found for the current user.")
        return

    entries = [line for line in result.stdout.splitlines() if updater in line]

    if not entries:
        _warn("Web Vitals cron job is NOT installed.")
        _info("Run:  python manage.py cron install")
        return

    _ok(f"Cron job   : installed ({len(entries)} entr{'y' if len(entries) == 1 else 'ies'})")
    for entry in entries:
        print(f"    {_C}{entry}{_RS}")

    _info(f"Next run   : {_next_cron_run()}")

    # ── 2. Log file (cron stdout redirect) ───────────────────────────────────
    log_file = DATA_DIR / "logs" / "cron.log"
    print()
    if not log_file.exists():
        _warn(f"Log file   : {log_file} (not found — cron has not run yet)")
        return

    stat = log_file.stat()
    size_kb = stat.st_size / 1024
    last_mod = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc)
    _info(f"Log file   : {log_file}  ({size_kb:.1f} KB)")
    _info(f"Last write : {last_mod.strftime('%Y-%m-%d %H:%M:%S')} UTC")

    # Determine last run outcome from the log
    try:
        text = log_file.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()

        # Find the last "Update complete" line
        summary_line = None
        for line in reversed(lines):
            if "Update complete" in line:
                summary_line = line
                break

        if summary_line:
            # Extract timestamp prefix (format: "2026-03-05 00:05:04 [INFO] ...")
            parts = summary_line.split("]", 1)
            ts_part = parts[0].split("[")[0].strip() if "[" in parts[0] else ""
            msg_part = parts[-1].strip() if len(parts) > 1 else summary_line
            status_str = f"{ts_part}  {msg_part}" if ts_part else msg_part
            if "0 windows failed" in summary_line:
                _ok(f"Last result: {status_str}")
            else:
                _warn(f"Last result: {status_str}")
        else:
            _warn("Last result: no completed run found in log")

        # Print last 10 non-empty lines
        tail = [l for l in lines if l.strip()][-10:]
        if tail:
            print()
            _info(f"Last {len(tail)} log lines:")
            for line in tail:
                print(f"    {line}")
    except OSError as exc:
        _warn(f"Could not read log: {exc}")


def cmd_cron_remove(args: argparse.Namespace) -> None:
    """Remove the Web Vitals cron job."""
    _head("Web Vitals — cron remove")
    _sep()

    updater = str(SCRIPTS_DIR / "updater.py")

    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        _warn("No crontab found for the current user.")
        return

    lines = result.stdout.splitlines()
    filtered = [line for line in lines if updater not in line]

    if len(filtered) == len(lines):
        _warn("No Web Vitals cron entry found — nothing to remove.")
        return

    new_crontab = "\n".join(filtered) + "\n" if filtered else ""
    subprocess.run(["crontab", "-"], input=new_crontab, text=True)
    _ok("Cron job removed.")


# ── DB check / repair ──────────────────────────────────────────────────────────

def cmd_db_check(args: argparse.Namespace) -> None:
    """
    Run SQLite integrity_check.
    Shows DB stats when healthy.
    Automatically attempts repair when damage is detected.
    """
    _head("Web Vitals — db check")
    _sep()

    db = _db_path()

    if not db.exists():
        _warn(f"Database not found: {db}")
        _info("Run 'python manage.py init' to create it.")
        return

    size_mb = db.stat().st_size / 1024 / 1024
    _info(f"Path      : {db}")
    _info(f"Size      : {size_mb:.2f} MB")

    # WAL / SHM files indicate an unclean shutdown
    for ext in (".db-wal", ".db-shm"):
        p = db.with_suffix(ext)
        if p.exists():
            _warn(f"Found {p.name} — WAL checkpoint may be needed")

    print()
    _info("Running integrity check …")

    try:
        conn = sqlite3.connect(db, timeout=10)
        check_rows = conn.execute("PRAGMA integrity_check(20)").fetchall()
        conn.close()
    except sqlite3.DatabaseError as exc:
        _err(f"Cannot open database: {exc}")
        _attempt_repair(db)
        return

    messages = [r[0] for r in check_rows]

    if messages == ["ok"]:
        _ok("Integrity check passed")
        _sep()
        _print_db_stats(db)
    else:
        _err("Integrity check FAILED:")
        for msg in messages:
            print(f"    {_Y}{msg}{_RS}")
        print()
        _attempt_repair(db)


def _print_db_stats(db: Path) -> None:
    try:
        conn = sqlite3.connect(db, timeout=30)
        stats = _db_stats(conn)
        urls = conn.execute(
            "SELECT COUNT(DISTINCT targetGroupedUrl) FROM vitals"
        ).fetchone()[0]
        conn.close()

        _info(f"Rows      : {stats['rows']:,}")
        _info(f"Views     : {stats['total_views']:,.0f}")
        _info(f"URLs      : {urls:,}")

        if stats["min_ts"] and stats["max_ts"]:
            min_dt = datetime.fromtimestamp(stats["min_ts"], tz=timezone.utc)
            max_dt = datetime.fromtimestamp(stats["max_ts"], tz=timezone.utc)
            _info(f"Range     : {min_dt.strftime('%Y-%m-%d %H:%M')} → {max_dt.strftime('%Y-%m-%d %H:%M')} UTC")
    except Exception as exc:
        _warn(f"Could not read stats: {exc}")


def _attempt_repair(db: Path) -> None:
    """
    Multi-strategy SQLite repair.

    Strategy 1 — VACUUM INTO  : compacts + copies all readable pages to a new file.
                                 Works even with minor page corruption.
    Strategy 2 — iterdump()   : generates INSERT statements for every readable row.
                                 More tolerant of structural damage.

    Either way, the original is preserved as a timestamped .bak file.
    """
    _sep()
    _info("Attempting automatic repair …")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db.parent / f"{db.stem}.bak.{ts}{db.suffix}"

    # Always back up first
    shutil.copy2(db, backup)
    _ok(f"Backup created: {backup.name}")
    print()

    # ── Strategy 1: VACUUM INTO ───────────────────────────────────────────────
    repaired = db.parent / f"{db.stem}.repaired.{ts}{db.suffix}"
    try:
        _info("Strategy 1: VACUUM INTO …")
        conn = sqlite3.connect(db, timeout=10)
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")  # flush WAL first
        conn.execute("VACUUM INTO ?", (str(repaired),))
        conn.close()

        # Verify the repaired file
        chk = sqlite3.connect(repaired)
        result = chk.execute("PRAGMA integrity_check(1)").fetchone()[0]
        chk.close()

        if result == "ok":
            shutil.move(str(repaired), str(db))
            _ok("Repair successful via VACUUM INTO")
            _sep()
            _print_db_stats(db)
            return
        else:
            _warn(f"VACUUM INTO produced a still-damaged DB ({result})")
            repaired.unlink(missing_ok=True)

    except Exception as exc:
        _warn(f"Strategy 1 failed: {exc}")
        repaired.unlink(missing_ok=True)

    # ── Strategy 2: iterdump() ────────────────────────────────────────────────
    try:
        _info("Strategy 2: row-by-row dump …")

        sys.path.insert(0, str(SCRIPTS_DIR))
        from schema import SCHEMA  # noqa: PLC0415 — schema.py has no config deps

        recovered = db.parent / f"{db.stem}.recovered.{ts}{db.suffix}"
        dst = sqlite3.connect(recovered)
        dst.executescript(SCHEMA)  # fresh schema

        src = sqlite3.connect(db, timeout=10)
        src.execute("PRAGMA journal_mode=DELETE")  # close WAL

        good_rows = 0
        bad_rows = 0
        for line in src.iterdump():
            if line.upper().startswith("INSERT"):
                try:
                    dst.execute(line)
                    good_rows += 1
                except sqlite3.Error:
                    bad_rows += 1
        dst.commit()
        src.close()
        dst.close()

        if good_rows == 0:
            _err("iterdump recovered 0 rows — database may be completely corrupted.")
            recovered.unlink(missing_ok=True)
            _err(f"Manual intervention needed. Backup at: {backup}")
            sys.exit(1)

        shutil.move(str(recovered), str(db))
        _ok(f"Repair complete: {good_rows:,} rows recovered, {bad_rows} rows lost")
        if bad_rows:
            _warn(f"{bad_rows} corrupted rows could not be recovered and were skipped.")
        _sep()
        _print_db_stats(db)

    except Exception as exc:
        _err(f"Strategy 2 failed: {exc}")
        _err(f"Database may be unrecoverable. Backup is at: {backup}")
        sys.exit(1)


# ── CLI wiring ─────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="manage.py",
        description="Web Vitals project CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Commands
────────
  init                       One-time 30-day backfill from New Relic
  update                     Incremental update: last stored → now
  dashboard start [--port N] Start dashboard in background
  dashboard stop             Stop background dashboard
  dashboard autostart install  Register systemd user service (auto-start on login)
  dashboard autostart remove   Remove systemd user service
  cron install               Register 6-hour cron job (safe to re-run)
  cron remove                Remove the cron job
  cron status                Show whether the cron job is active
  db check                   Integrity check; auto-repair on failure

Examples
────────
  python manage.py init
  python manage.py init --days 7
  python manage.py update
  python manage.py dashboard start
  python manage.py dashboard start --port 9000
  python manage.py dashboard stop
  python manage.py dashboard autostart install
  python manage.py cron install
  python manage.py cron status
  python manage.py db check
""",
    )
    sub = p.add_subparsers(dest="command", metavar="<command>")
    sub.required = True

    # init
    p_init = sub.add_parser("init", help="One-time 30-day backfill")
    p_init.add_argument(
        "--days", type=int, default=30,
        help="How many days to backfill (default: 30)",
    )
    p_init.add_argument(
        "--force", action="store_true",
        help="Re-run even if data already exists",
    )
    p_init.set_defaults(func=cmd_init)

    # update
    p_update = sub.add_parser("update", help="Incremental update to now")
    p_update.set_defaults(func=cmd_update)

    # dashboard
    p_dash = sub.add_parser("dashboard", help="Manage the Streamlit dashboard")
    dash_sub = p_dash.add_subparsers(dest="dashboard_action", metavar="<action>")
    dash_sub.required = True

    p_ds = dash_sub.add_parser("start", help="Start dashboard in background")
    p_ds.add_argument(
        "--port", type=int, default=None,
        help="Port to listen on (default: 8501, or $STREAMLIT_PORT)",
    )
    p_ds.add_argument(
        "--foreground", action="store_true",
        help="Run in foreground (used by systemd)",
    )
    p_ds.set_defaults(func=cmd_dashboard_start)

    p_dstop = dash_sub.add_parser("stop", help="Stop background dashboard")
    p_dstop.set_defaults(func=cmd_dashboard_stop)

    p_dau = dash_sub.add_parser("autostart", help="Manage systemd autostart")
    dau_sub = p_dau.add_subparsers(dest="autostart_action", metavar="<action>")
    dau_sub.required = True

    p_dai = dau_sub.add_parser("install", help="Install systemd user service")
    p_dai.set_defaults(func=cmd_dashboard_autostart_install)

    p_dar = dau_sub.add_parser("remove", help="Remove systemd user service")
    p_dar.set_defaults(func=cmd_dashboard_autostart_remove)

    # cron
    p_cron = sub.add_parser("cron", help="Manage cron job")
    cron_sub = p_cron.add_subparsers(dest="cron_action", metavar="<action>")
    cron_sub.required = True

    p_ci = cron_sub.add_parser("install", help="Install or update cron job")
    p_ci.set_defaults(func=cmd_cron_install)

    p_cr = cron_sub.add_parser("remove", help="Remove cron job")
    p_cr.set_defaults(func=cmd_cron_remove)

    p_cs = cron_sub.add_parser("status", help="Show whether cron job is installed")
    p_cs.set_defaults(func=cmd_cron_status)

    # db
    p_db = sub.add_parser("db", help="Database utilities")
    db_sub = p_db.add_subparsers(dest="db_action", metavar="<action>")
    db_sub.required = True

    p_dc = db_sub.add_parser("check", help="Integrity check + auto-repair")
    p_dc.set_defaults(func=cmd_db_check)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
