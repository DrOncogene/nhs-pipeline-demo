import calendar
from pathlib import Path

# year runs from prev April to current year March,
# so e.g. "prev jan 26" would be Jan 2026 data, which is in the 2025-26 page
MONTHS = {
    "prev": calendar.month_abbr[4:],
    "curr": calendar.month_abbr[:4],
}
TEMP_DIR = Path("/tmp/rtt_data")
TEMP_DIR.mkdir(exist_ok=True)
