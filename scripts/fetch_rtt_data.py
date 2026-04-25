from datetime import datetime
import os
from pathlib import Path
import re
import zipfile
import io
import polars as pl
import httpx
from bs4 import BeautifulSoup

BASE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-20{}-{}/"
# year runs from prev April to current year March, 
# so e.g. "prev jan 26" would be Jan 2026 data, which is in the 2025-26 page
MONTHS = {
    "prev": ["apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
    "curr": ["jan", "feb", "mar"],
}


def parse_month(value: str) -> str | None:
    for fmt in ("%m", "%B", "%b"):
        try:
            return value if datetime.strptime(value.strip(), fmt).month is not None else None
        except ValueError:
            continue
    return None

def parse_year(value: str) -> int | None:
    for fmt in ("%Y", "%y"):
        try:
            return datetime.strptime(value.strip(), fmt).year - 2000
        except ValueError:
            continue
    return None


def get_month_year_target(month: str, year: int | str) -> str:
    """"""
    year = int(year)
    if year > 2000:
        year -= 2000

    month = month[:3].lower()
    if month in MONTHS["prev"]:
        return f"{month.capitalize()}{year-1}"
    elif month in MONTHS["curr"]:
        return f"{month.capitalize()}{year}"
    else:
        raise ValueError(f"Invalid month: {month}")


def get_main_page(year: int | str) -> httpx.Response:
    # Scrape page for the full CSV zip link
    year = int(year)
    if year < 12 or year > 27:
        raise ValueError("Only data between 2011 to 2026 are supported for now")

    try:
        return httpx.get(BASE_URL.format(year - 1, year))
    except Exception as e:
        raise e


def fetch_monthly_data(page: httpx.Response, target: str) -> Path:
    soup = BeautifulSoup(page.text, "html.parser")

    link = next(
        a["href"]
        for a in soup.find_all("a", href=True)
        if "full csv" in a.text.lower() and target.lower() in str(a["href"]).lower()
    )

    # Download and extract
    r = httpx.get(str(link))
    z = zipfile.ZipFile(io.BytesIO(r.content))
    csv_name = next(n for n in z.namelist() if n.endswith(".csv"))
    df = pl.read_csv(z.open(csv_name))

    prev_year, curr_year = (int(target[-2:]) - 1) + 2000, int(target[-2:])
    year_dir = Path(f"data/{prev_year}-{curr_year}")
    year_dir.mkdir(parents=True, exist_ok=True)
    data_file = year_dir / f"rtt_{target}.csv.gz"

    df.write_csv(data_file, compression="gzip")
    print(f"Saved {data_file} — {len(df)} rows")

    return data_file


def transform_data(file: Path) -> None:
    df = pl.read_csv(file, null_values=["*"])
    # Clean: handle suppressed values, standardise columns
    df = df.with_columns(pl.col(pl.Utf8).str.strip_chars().replace(" ", "_"))  # Remove whitespace from string columns

    numeric_cols = [
        c for c in df.columns if any(x in c for x in ["total", "within", "gt", "weeks"])
    ]
    # df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
    df = df.with_columns(pl.col(numeric_cols).cast(pl.Int64, strict=False))


    df.write_csv(f"{file.parent}/{file.stem}_clean.csv.gz", compression="gzip")

def main():
    month = os.getenv("MONTH", "").lower()
    year = os.getenv("YEAR", "2026")[-2:]  # e.g. "26"
    month, year = parse_month(month), parse_year(year)
    assert month is not None, "Invalid month format. Enter valid month name"
    assert year is not None, "Invalid year format. Use e.g. '2026' or '26'."

    target = get_month_year_target(month, year)
    saved_file = fetch_monthly_data(get_main_page(year), target)
    transform_data(saved_file)


if __name__ == "__main__":
    main()
