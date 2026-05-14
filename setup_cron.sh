#!/bin/bash
# setup_cron.sh
# ─────────────────────────────────────────────────────────────
# Sets up a local macOS cron job to run the daily floorsheet
# scraper after market close (3:15 PM Nepal time = 09:30 UTC).
#
# Usage:
#   chmod +x setup_cron.sh
#   ./setup_cron.sh
#
# Nepal Time = UTC + 5:45
# So 3:15 PM NPT = 09:30 UTC
# On macOS (your local machine), adjust for YOUR local timezone.
# ─────────────────────────────────────────────────────────────

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/venv/bin/python"
SCRAPER="$SCRIPT_DIR/daily_scraper.py"
LOG="$SCRIPT_DIR/daily_scraper.log"

echo "=== Floorsheet Daily Scraper - Cron Setup ==="
echo "Project directory: $SCRIPT_DIR"

# ── Detect local timezone offset from UTC ──
# Get the current UTC offset in hours (e.g. +5.75 for NPT)
LOCAL_UTC_OFFSET=$(python3 -c "
import time
offset_sec = -time.timezone if time.daylight == 0 else -time.altzone
offset_hr = offset_sec / 3600
print(offset_hr)
")
echo "Your local UTC offset: $LOCAL_UTC_OFFSET hours"

# ── Target time: 09:30 UTC (3:15 PM NPT) ──
# Convert 09:30 UTC to local time
TARGET_UTC_HOUR=9
TARGET_UTC_MIN=30

LOCAL_HOUR=$(python3 -c "
offset = $LOCAL_UTC_OFFSET
utc_total = $TARGET_UTC_HOUR * 60 + $TARGET_UTC_MIN
local_total = (utc_total + int(offset * 60)) % (24 * 60)
print(local_total // 60)
")
LOCAL_MIN=$(python3 -c "
offset = $LOCAL_UTC_OFFSET
utc_total = $TARGET_UTC_HOUR * 60 + $TARGET_UTC_MIN
local_total = (utc_total + int(offset * 60)) % (24 * 60)
print(local_total % 60)
")

echo "Cron will run at: $LOCAL_HOUR:$(printf '%02d' $LOCAL_MIN) local time (= 09:30 UTC = 3:15 PM NPT)"

# ── Build the cron entry ──
CRON_ENTRY="$LOCAL_MIN $LOCAL_HOUR * * 1-5 cd \"$SCRIPT_DIR\" && \"$PYTHON\" \"$SCRAPER\" >> \"$LOG\" 2>&1"

echo ""
echo "Cron entry to add:"
echo "  $CRON_ENTRY"
echo ""

# ── Add to crontab (avoid duplicates) ──
TMPFILE=$(mktemp)
crontab -l 2>/dev/null | grep -v "daily_scraper.py" > "$TMPFILE" || true
echo "$CRON_ENTRY" >> "$TMPFILE"
crontab "$TMPFILE"
rm "$TMPFILE"

echo "✅ Cron job installed!"
echo ""
echo "To verify, run:   crontab -l"
echo "To remove, run:   crontab -l | grep -v 'daily_scraper' | crontab -"
echo ""
echo "=== Setup complete! ==="
