#!/usr/bin/env python3
"""
tidb_sync.py
------------
Syncs the freshly-scraped floorsheet CSV(s) from GitHub LFS into TiDB Cloud.

Run AFTER daily_scraper.py has pushed the updated CSV to GitHub.

Strategy:
  1. Query TiDB for the latest trade_date already stored.
  2. Fetch only the month CSV(s) that contain newer data via Git LFS CDN.
  3. Parse each row, clean amounts/quantities (strip commas), skip duplicates.
  4. Bulk-insert in batches of 2 000 rows (avoids packet-size limits).
  5. Exit 0 on success, non-zero on fatal error (fails the GH Actions step).

Environment variables required (set as GitHub Secrets):
  TIDB_HOST, TIDB_USER, TIDB_PASSWORD, TIDB_DB_NAME, TIDB_PORT

CSV format (merolagani floorsheet):
  Date,S.N.,Transact. No.,Symbol,Buyer,Seller,Quantity,Rate,Amount
  MM/DD/YYYY,int,str,str,int,int,int,float,float
"""

import os
import sys
import csv
import io
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

# ── Third-party ──────────────────────────────────────────────────────────────
try:
    import requests
    import mysql.connector
    from mysql.connector import errorcode
except ImportError as e:
    print(f"[FATAL] Missing dependency: {e}")
    print("Run: pip install requests mysql-connector-python")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

GITHUB_OWNER = "RohanPoudel2024"
GITHUB_REPO  = "Raw_Floorsheet"
# Git LFS CDN — the ONLY URL that serves real CSV bytes (not LFS pointer stubs)
LFS_BASE_URL = f"https://media.githubusercontent.com/media/{GITHUB_OWNER}/{GITHUB_REPO}/main/data"

BATCH_SIZE   = 2_000        # rows per INSERT batch
MAX_RETRIES  = 3            # HTTP retries per CSV fetch
RETRY_DELAY  = 5            # seconds between retries

LOG_FILE     = "tidb_sync.log"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def clean_number(s: str) -> str:
    """Strip commas and surrounding quotes from numeric strings like '\"4,515.00\"'."""
    return s.strip().strip('"').replace(",", "")


def mero_date_to_iso(date_str: str) -> Optional[str]:
    """Convert merolagani date 'MM/DD/YYYY' → 'YYYY-MM-DD'. Returns None on failure."""
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


def months_between(start: datetime, end: datetime):
    """Yield (year, month) tuples from start to end inclusive."""
    current = start.replace(day=1)
    while current <= end.replace(day=1):
        yield current.year, current.month
        # advance to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def fetch_csv_from_lfs(year: int, month: int) -> Optional[str]:
    """Download a monthly floorsheet CSV from GitHub LFS CDN. Returns raw text or None."""
    filename = f"{year}_{month:02d}_floorsheet.csv"
    url = f"{LFS_BASE_URL}/{filename}"
    log.info(f"⬇  Fetching {filename} from LFS CDN …")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 404:
                log.warning(f"   CSV not found (404): {filename}")
                return None
            resp.raise_for_status()

            # Sanity-check: LFS pointer stubs start with "version https://git-lfs"
            text = resp.text
            if text.startswith("version https://git-lfs"):
                log.error(
                    f"   Got LFS pointer stub instead of real CSV for {filename}. "
                    "The file may not be publicly accessible via LFS CDN."
                )
                return None

            log.info(f"   ✅ Downloaded {len(text):,} bytes")
            return text

        except requests.RequestException as exc:
            log.warning(f"   Attempt {attempt}/{MAX_RETRIES} failed: {exc}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    log.error(f"   ❌ All {MAX_RETRIES} attempts failed for {filename}")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TiDB connection
# ─────────────────────────────────────────────────────────────────────────────

def get_connection():
    """Create a TiDB Cloud MySQL connection using environment variables."""
    return mysql.connector.connect(
        host     = os.environ["TIDB_HOST"],
        user     = os.environ["TIDB_USER"],
        password = os.environ["TIDB_PASSWORD"],
        database = os.environ.get("TIDB_DB_NAME", "test"),
        port     = int(os.environ.get("TIDB_PORT", 4000)),
        ssl_ca   = None,
        ssl_disabled = False,
        connection_timeout = 30,
        # TiDB Cloud requires TLS
        ssl_verify_cert  = False,  # skip CA cert verification (compatible with TiDB Cloud)
        ssl_verify_identity = False,
    )


def get_last_date_in_tidb(conn) -> Optional[str]:
    """Return the MAX(trade_date) already in floorsheet_raw, or None if table is empty."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT MAX(trade_date) FROM floorsheet_raw")
        row = cursor.fetchone()
        if row and row[0]:
            # MySQL returns a date object
            return row[0].strftime("%Y-%m-%d") if hasattr(row[0], "strftime") else str(row[0])
        return None
    finally:
        cursor.close()


def ensure_table(conn):
    """Create floorsheet_raw table if it doesn't exist yet."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS floorsheet_raw (
                id              BIGINT AUTO_INCREMENT PRIMARY KEY,
                trade_date      DATE         NOT NULL,
                sn              INT          NOT NULL,
                transaction_no  VARCHAR(30)  NOT NULL,
                symbol          VARCHAR(20)  NOT NULL,
                buyer_broker    SMALLINT     NOT NULL,
                seller_broker   SMALLINT     NOT NULL,
                quantity        INT          NOT NULL,
                rate            DECIMAL(12,2) NOT NULL,
                amount          DECIMAL(18,2) NOT NULL,

                -- Unique constraint prevents duplicate scrapes
                UNIQUE KEY uq_transaction (transaction_no),
                KEY idx_trade_date   (trade_date),
                KEY idx_symbol       (symbol),
                KEY idx_buyer_broker (buyer_broker),
                KEY idx_seller_broker(seller_broker),
                KEY idx_sym_date     (symbol, trade_date)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        conn.commit()
        log.info("✅ Table floorsheet_raw is ready")
    finally:
        cursor.close()


# ─────────────────────────────────────────────────────────────────────────────
# Core sync logic
# ─────────────────────────────────────────────────────────────────────────────

def parse_csv_rows(text: str, after_date: Optional[str]) -> list:
    """
    Parse CSV text and return rows newer than after_date.

    Returns list of tuples:
      (trade_date, sn, transaction_no, symbol, buyer, seller, quantity, rate, amount)
    """
    rows = []
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)  # skip header row

    skipped_parse = 0
    skipped_old   = 0

    for line in reader:
        if len(line) < 9:
            skipped_parse += 1
            continue

        trade_date_iso = mero_date_to_iso(line[0])
        if not trade_date_iso:
            skipped_parse += 1
            continue

        # Only import rows strictly newer than what's already in DB
        if after_date and trade_date_iso <= after_date:
            skipped_old += 1
            continue

        try:
            sn             = int(clean_number(line[1])) if line[1].strip() else 0
            transaction_no = line[2].strip()
            symbol         = line[3].strip().upper()
            buyer_broker   = int(clean_number(line[4])) if line[4].strip() else 0
            seller_broker  = int(clean_number(line[5])) if line[5].strip() else 0
            quantity       = int(clean_number(line[6])) if line[6].strip() else 0
            rate           = float(clean_number(line[7])) if line[7].strip() else 0.0
            amount         = float(clean_number(line[8])) if line[8].strip() else 0.0
        except (ValueError, IndexError):
            skipped_parse += 1
            continue

        rows.append((trade_date_iso, sn, transaction_no, symbol,
                     buyer_broker, seller_broker, quantity, rate, amount))

    if skipped_parse:
        log.warning(f"   Skipped {skipped_parse} unparseable rows")
    if skipped_old:
        log.info(f"   Skipped {skipped_old} already-synced rows")

    return rows


def insert_rows(conn, rows: list, dry_run: bool = False) -> int:
    """Bulk-insert rows into floorsheet_raw in batches. Returns count of rows inserted."""
    if not rows:
        return 0

    if dry_run:
        log.info(f"   [DRY RUN] Would insert {len(rows):,} rows (skipping actual write)")
        return len(rows)

    sql = """
        INSERT IGNORE INTO floorsheet_raw
            (trade_date, sn, transaction_no, symbol,
             buyer_broker, seller_broker, quantity, rate, amount)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cursor   = conn.cursor()
    inserted = 0

    try:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i : i + BATCH_SIZE]
            cursor.executemany(sql, batch)
            conn.commit()
            inserted += cursor.rowcount
            log.info(f"   Batch {i // BATCH_SIZE + 1}: inserted {cursor.rowcount} rows")
    finally:
        cursor.close()

    return inserted


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("TiDB Floorsheet Sync — starting")
    log.info("=" * 60)

    # ── Optional overrides from env (set by backfill workflow) ──────────────
    force_from_date = os.environ.get("TIDB_SYNC_FROM_DATE", "").strip() or None
    dry_run = os.environ.get("TIDB_SYNC_DRY_RUN", "false").lower() in ("true", "1", "yes")

    if force_from_date:
        log.info(f"📌 TIDB_SYNC_FROM_DATE override: syncing from {force_from_date}")
    if dry_run:
        log.info("🔍 DRY RUN mode — no data will be written to TiDB")

    # ── Validate env vars ─────────────────────────────────────────────────
    required_env = ["TIDB_HOST", "TIDB_USER", "TIDB_PASSWORD"]
    missing = [k for k in required_env if not os.environ.get(k)]
    if missing:
        log.error(f"Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    # ── Connect to TiDB ───────────────────────────────────────────────────
    try:
        conn = get_connection()
        log.info(f"✅ Connected to TiDB: {os.environ['TIDB_HOST']}")
    except Exception as exc:
        log.error(f"❌ Cannot connect to TiDB: {exc}")
        sys.exit(1)

    try:
        ensure_table(conn)

        # ── Find the latest date already synced ───────────────────────────
        last_date = get_last_date_in_tidb(conn)
        log.info(f"📅 Last trade_date in TiDB: {last_date or 'NONE (empty table)'}")

        # ── Determine which months to fetch ───────────────────────────────
        today = datetime.utcnow()

        # force_from_date overrides the auto-detected last_date
        if force_from_date:
            start_dt = datetime.strptime(force_from_date, "%Y-%m-%d")
            # When forcing a from_date, also override last_date so we re-sync that period
            last_date = (datetime.strptime(force_from_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
            log.info(f"📌 Forced start: {force_from_date} (last_date set to {last_date})")
        elif last_date:
            start_dt = datetime.strptime(last_date, "%Y-%m-%d")
        else:
            # Default: sync from Jan 2025 (adjust as needed)
            start_dt = datetime(2025, 1, 1)
            log.info("⚠️  TiDB is empty — will sync ALL available history from 2025-01")

        # Always include current month + previous month as buffer
        end_dt = today

        months_to_fetch = list(months_between(start_dt, end_dt))
        log.info(f"📆 Months to check: {[f'{y}-{m:02d}' for y, m in months_to_fetch]}")

        # ── Fetch & sync each month ───────────────────────────────────────
        total_inserted = 0
        total_errors   = 0

        for year, month in months_to_fetch:
            csv_text = fetch_csv_from_lfs(year, month)
            if csv_text is None:
                continue

            rows = parse_csv_rows(csv_text, after_date=last_date)
            log.info(f"   New rows to insert for {year}-{month:02d}: {len(rows):,}")

            if rows:
                try:
                    n = insert_rows(conn, rows, dry_run=dry_run)
                    total_inserted += n
                    log.info(f"   ✅ {year}-{month:02d}: {n:,} rows inserted")
                    # After inserting, advance last_date to the max date we just inserted
                    new_max = max(r[0] for r in rows)
                    if not last_date or new_max > last_date:
                        last_date = new_max
                except Exception as exc:
                    log.error(f"   ❌ Insert failed for {year}-{month:02d}: {exc}")
                    total_errors += 1

        # ── Summary ───────────────────────────────────────────────────────
        log.info("=" * 60)
        log.info(f"✅ Sync complete — {total_inserted:,} rows inserted")
        if total_errors:
            log.warning(f"⚠️  {total_errors} month(s) had errors")
        log.info("=" * 60)

        if total_errors and total_inserted == 0:
            sys.exit(1)  # Full failure → fail the GH Actions step

    finally:
        conn.close()


if __name__ == "__main__":
    main()
