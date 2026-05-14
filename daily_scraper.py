#!/usr/bin/env python3
"""
daily_scraper.py
----------------
Automated daily floorsheet scraper for merolagani.com

Designed to run after Nepal Stock Exchange market close (after 3:00 PM NPT).
- Scrapes today's (or yesterday's if weekend/holiday) floorsheet data
- Saves to the correct monthly CSV in the data/ directory
- Commits and pushes to GitHub automatically
- Creates a new monthly file when month changes

Usage:
    python daily_scraper.py                      # Scrape today
    python daily_scraper.py --date 05/14/2026    # Scrape specific date
    python daily_scraper.py --date 05/14/2026 --no-push  # Scrape without git push
    python daily_scraper.py --backfill           # Fill any missing dates up to today
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import re
import gc
import os
import sys
import logging
import argparse
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
NEPAL_TZ = ZoneInfo("Asia/Kathmandu")
MARKET_CLOSE_HOUR = 15          # 3 PM NPT
DATA_DIR = "data"
LOG_FILE = "daily_scraper.log"
SCRAPE_URL = "https://merolagani.com/Floorsheet.aspx"
CSV_HEADER = ['Date', 'S.N.', 'Transact. No.', 'Symbol', 'Buyer', 'Seller', 'Quantity', 'Rate', 'Amount']

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# HTTP Session
# ─────────────────────────────────────────────
def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
    return session


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def get_hidden_fields(soup):
    """Extract ASP.NET ViewState and other hidden fields."""
    return {
        tag["name"]: tag.get("value", "")
        for tag in soup.find_all("input", type="hidden")
        if tag.get("name")
    }


def csv_path_for_date(dt: datetime) -> str:
    """Return the monthly CSV file path for a given date."""
    month_str = dt.strftime("%Y_%m")
    return os.path.join(DATA_DIR, f"{month_str}_floorsheet.csv")


def dates_in_csv(csv_path: str) -> set:
    """Return the set of date strings already in a CSV file."""
    if not os.path.exists(csv_path):
        return set()
    dates = set()
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        for row in reader:
            if row:
                dates.add(row[0])
    return dates


def remove_date_from_csv(csv_path: str, date_str: str):
    """Remove all rows for a specific date from CSV (dedup safety)."""
    if not os.path.exists(csv_path):
        return
    temp = csv_path + ".tmp"
    removed = 0
    with open(csv_path, "r", newline="", encoding="utf-8") as fin, \
         open(temp, "w", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header = next(reader, None)
        if header:
            writer.writerow(header)
        for row in reader:
            if row and row[0] == date_str:
                removed += 1
            else:
                writer.writerow(row)
    os.replace(temp, csv_path)
    if removed:
        log.info(f"Removed {removed} duplicate rows for {date_str}")


def ensure_csv_header(csv_path: str):
    """Create CSV with header if it doesn't exist."""
    if not os.path.exists(csv_path):
        os.makedirs(os.path.dirname(csv_path) or ".", exist_ok=True)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CSV_HEADER)
        log.info(f"Created new monthly CSV: {csv_path}")


# ─────────────────────────────────────────────
# Core Scraper
# ─────────────────────────────────────────────
def scrape_one_date(date_str: str, session: requests.Session, soup: BeautifulSoup):
    """
    Scrape all pages for a single date.
    Returns (rows: list[list], updated_soup: BeautifulSoup, success: bool)
    """
    url = SCRAPE_URL
    max_retries = 3

    # ── Search for the date ──
    search_success = False
    for attempt in range(max_retries):
        try:
            data = get_hidden_fields(soup)
            data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = date_str
            data["__EVENTTARGET"] = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"
            data["__EVENTARGUMENT"] = ""
            response = session.post(url, data=data, timeout=40)
            soup = BeautifulSoup(response.content, "html.parser")
            search_success = True
            break
        except Exception as e:
            log.warning(f"Search attempt {attempt + 1} failed for {date_str}: {e}")
            time.sleep(5)

    if not search_success:
        log.error(f"Could not load search results for {date_str}")
        return [], soup, False

    # ── Check for data ──
    table = soup.find("table", {"class": "table table-bordered table-striped table-hover sortable"})
    if not table:
        log.info(f"No data table found for {date_str} (holiday or no trading)")
        return [], soup, True  # Not a failure — just no data

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
    if not rows or (len(rows) == 1 and "No Record Found" in rows[0].text):
        log.info(f"No records found for {date_str} (holiday or no trading)")
        return [], soup, True

    # ── Determine total pages ──
    total_pages = 1
    last_page_link = soup.find("a", title="Last Page")
    if last_page_link:
        onclick = last_page_link.get("onclick", "")
        match = re.search(r'changePageIndex\("(\d+)"', onclick)
        if match:
            total_pages = int(match.group(1))

    log.info(f"  {date_str}: {total_pages} page(s) to scrape")

    all_rows = []

    # ── Page 1 ──
    for row in rows:
        cols = [td.text.strip() for td in row.find_all("td")]
        if len(cols) == 8:
            all_rows.append([date_str] + cols)

    # ── Pages 2+ ──
    for page in range(2, total_pages + 1):
        page_ok = False
        for attempt in range(max_retries):
            log.info(f"    Page {page}/{total_pages} (attempt {attempt + 1})")
            time.sleep(1)
            try:
                p_data = get_hidden_fields(soup)
                p_data["ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"] = date_str
                p_data["ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"] = str(page)
                p_data["ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"] = ""
                p_data["__EVENTTARGET"] = ""
                p_data["__EVENTARGUMENT"] = ""

                r = session.post(url, data=p_data, timeout=40)
                page_soup = BeautifulSoup(r.content, "html.parser")

                t = page_soup.find("table", {"class": "table table-bordered table-striped table-hover sortable"})
                if t:
                    tb = t.find("tbody")
                    page_rows = tb.find_all("tr") if tb else t.find_all("tr")[1:]
                    for row in page_rows:
                        cols = [td.text.strip() for td in row.find_all("td")]
                        if len(cols) == 8:
                            all_rows.append([date_str] + cols)
                    soup = page_soup
                    page_ok = True
                    break
                else:
                    log.warning(f"    No table on page {page}, attempt {attempt + 1}")
            except Exception as e:
                log.warning(f"    Error on page {page}, attempt {attempt + 1}: {e}")
            time.sleep(2)

        if not page_ok:
            log.error(f"Failed to scrape page {page} for {date_str}")
            return all_rows, soup, False  # Partial — let caller decide

        gc.collect()

    return all_rows, soup, True


# ─────────────────────────────────────────────
# Git Push
# ─────────────────────────────────────────────
def git_push(date_str: str, files_changed: list[str]):
    """Stage changed CSV files and push to GitHub."""
    try:
        # Stage only the data/ directory CSV files
        for f in files_changed:
            subprocess.run(["git", "add", f], check=True)

        # Commit
        msg = f"Auto-update floorsheet: {date_str}"
        result = subprocess.run(
            ["git", "commit", "-m", msg],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            if "nothing to commit" in result.stdout or "nothing to commit" in result.stderr:
                log.info("Nothing new to commit — data already up to date.")
                return True
            log.warning(f"Git commit warning: {result.stderr}")

        # Push
        subprocess.run(["git", "push", "origin", "main"], check=True)
        log.info(f"✅ Pushed to GitHub: {msg}")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"Git push failed: {e}")
        return False


# ─────────────────────────────────────────────
# Date Logic
# ─────────────────────────────────────────────
def get_target_date(args_date: str | None) -> datetime:
    """
    Determine which date to scrape.
    - If --date given, use that.
    - Otherwise use today in Nepal time (script is meant to run after 3 PM NPT).
    """
    if args_date:
        return datetime.strptime(args_date, "%m/%d/%Y")
    now_npt = datetime.now(NEPAL_TZ)
    return now_npt.replace(tzinfo=None)


def get_missing_dates(since_date: datetime) -> list[datetime]:
    """Return all weekdays from since_date to today (Nepal time) not yet in CSV."""
    today = datetime.now(NEPAL_TZ).replace(tzinfo=None)
    missing = []
    current = since_date
    while current <= today:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:
            csv_path = csv_path_for_date(current)
            existing = dates_in_csv(csv_path)
            date_str = current.strftime("%m/%d/%Y")
            if date_str not in existing:
                missing.append(current)
        current += timedelta(days=1)
    return missing


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Daily Floorsheet Scraper")
    parser.add_argument("--date", type=str, default=None,
                        help="Specific date to scrape (MM/DD/YYYY). Defaults to today NPT.")
    parser.add_argument("--no-push", action="store_true",
                        help="Skip git push after scraping.")
    parser.add_argument("--backfill", action="store_true",
                        help="Fill all missing dates from the earliest existing CSV up to today.")
    parser.add_argument("--backfill-from", type=str, default=None,
                        help="Start backfill from this date (MM/DD/YYYY).")
    args = parser.parse_args()

    os.makedirs(DATA_DIR, exist_ok=True)

    log.info("=" * 60)
    log.info("Daily Floorsheet Scraper Started")
    log.info(f"Nepal time: {datetime.now(NEPAL_TZ).strftime('%Y-%m-%d %H:%M:%S NPT')}")

    # ── Determine dates to scrape ──
    if args.backfill or args.backfill_from:
        if args.backfill_from:
            since = datetime.strptime(args.backfill_from, "%m/%d/%Y")
        else:
            # Find earliest existing CSV
            csvs = sorted([f for f in os.listdir(DATA_DIR) if f.endswith("_floorsheet.csv")])
            if csvs:
                earliest_month = csvs[0].replace("_floorsheet.csv", "")
                since = datetime.strptime(earliest_month, "%Y_%m")
            else:
                since = datetime.now(NEPAL_TZ).replace(tzinfo=None) - timedelta(days=30)
        dates_to_scrape = get_missing_dates(since)
        log.info(f"Backfill mode: {len(dates_to_scrape)} missing dates found")
    else:
        target = get_target_date(args.date)
        # Check if already scraped today
        date_str = target.strftime("%m/%d/%Y")
        csv_path = csv_path_for_date(target)
        existing = dates_in_csv(csv_path)
        if date_str in existing and not args.date:
            log.info(f"Date {date_str} already in CSV. Nothing to do.")
            return
        dates_to_scrape = [target]

    if not dates_to_scrape:
        log.info("No dates to scrape. Exiting.")
        return

    # ── Initialize scraper session ──
    log.info("Initializing HTTP session...")
    session = get_session()
    try:
        resp = session.get(SCRAPE_URL, timeout=30)
        soup = BeautifulSoup(resp.content, "html.parser")
    except Exception as e:
        log.error(f"Failed to load initial page: {e}")
        sys.exit(1)

    files_changed = set()
    success_count = 0
    fail_count = 0

    for dt in dates_to_scrape:
        date_str = dt.strftime("%m/%d/%Y")
        csv_path = csv_path_for_date(dt)

        log.info(f"\n── Scraping {date_str} ──")

        # Ensure monthly CSV exists with header
        ensure_csv_header(csv_path)

        # Remove any partial data for this date (avoid duplicates)
        remove_date_from_csv(csv_path, date_str)

        # Scrape
        rows, soup, ok = scrape_one_date(date_str, session, soup)

        if ok:
            if rows:
                with open(csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)
                log.info(f"✅ {date_str}: {len(rows)} rows saved to {csv_path}")
                files_changed.add(csv_path)
            else:
                log.info(f"⏭  {date_str}: No trading data (holiday/weekend)")
            success_count += 1
        else:
            log.warning(f"⚠️  {date_str}: Partial failure — will retry next run")
            # Still save whatever we got
            if rows:
                with open(csv_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(rows)
                files_changed.add(csv_path)
            fail_count += 1

        # Refresh session at month boundary
        if dt.day == 1 or (len(dates_to_scrape) > 1 and dates_to_scrape.index(dt) > 0
                           and dates_to_scrape[dates_to_scrape.index(dt) - 1].month != dt.month):
            log.info("Month boundary: refreshing HTTP session...")
            session.close()
            session = get_session()
            try:
                resp = session.get(SCRAPE_URL, timeout=30)
                soup = BeautifulSoup(resp.content, "html.parser")
            except Exception as e:
                log.warning(f"Session refresh failed: {e}")

        gc.collect()
        time.sleep(2)  # Be polite between dates

    # ── Summary ──
    log.info("\n" + "=" * 60)
    log.info(f"Scraping complete: {success_count} succeeded, {fail_count} failed")

    # ── Git Push ──
    if not args.no_push and files_changed:
        log.info(f"Pushing {len(files_changed)} changed file(s) to GitHub...")
        date_label = dates_to_scrape[-1].strftime("%m/%d/%Y") if dates_to_scrape else "unknown"
        git_push(date_label, list(files_changed))
    elif args.no_push:
        log.info("Skipping git push (--no-push flag set)")
    else:
        log.info("No files changed — skipping git push")

    log.info("Done.")


if __name__ == "__main__":
    main()
