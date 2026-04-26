from datetime import datetime

from scripts import MONTHS


def parse_month(value: str | None) -> str | None:
    """Parse month from various formats. Returns 3-letter month."""
    if value is None:
        return None

    for fmt in ("%m", "%B", "%b"):
        try:
            dt = datetime.strptime(value.strip(), fmt)
            return dt.strftime("%b")
        except ValueError:
            continue

    return None


def parse_year(value: str) -> int | None:
    """Parse year from various formats. Returns 2-digit year."""
    for fmt in ("%Y", "%y"):
        try:
            return datetime.strptime(value.strip(), fmt).year - 2000
        except ValueError:
            continue
    return None


def get_month_year_target(month: str, year: int) -> str:
    """Return the target month-year string for scraping."""

    if year > 2000:
        year -= 2000

    month = month.strip()[:3].title()
    if month in MONTHS["prev"]:
        return f"{month}{year - 1}"
    elif month in MONTHS["curr"]:
        return f"{month}{year}"
    else:
        raise ValueError(f"Invalid month: {month}")
    

def month_label_sorter(label: str) -> tuple[int, int]:
    """Return (year, month) for labels like Jan26, Feb22. Used with sorted()"""
    dt = datetime.strptime(label.strip(), "%b%y")
    return (dt.year, dt.month)
