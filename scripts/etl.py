from pathlib import Path
import calendar
import os
import io
import zipfile
import json

from databricks.sdk import WorkspaceClient
import httpx
import polars as pl
from bs4 import BeautifulSoup

from scripts import TEMP_DIR
from scripts.utils import (
    get_month_year_target,
    parse_month,
    parse_year,
    month_label_sorter,
)

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_CLIENT_ID = os.getenv("DATABRICKS_CLIENT_ID")
DATABRICKS_API_KEY = os.getenv("DATABRICKS_API_KEY")
DATABRICKS_DATA_DIR = os.getenv("DATABRICKS_DATA_DIR")
LOCAL_DATA_DIR = Path("data")

if not all(
    [
        DATABRICKS_HOST,
        DATABRICKS_CLIENT_ID,
        DATABRICKS_API_KEY,
        DATABRICKS_DATA_DIR,
    ]
):
    raise ValueError(
        "Missing Databricks credentials. Please set DATABRICKS_HOST,"
        "DATABRICKS_CLIENT_ID, and DATABRICKS_API_KEY environment variables."
    )

BASE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-20{}-{}/"


def extract(month: str, year: int):
    """Scrape page for the full CSV zip link, downloads, extract, and saves it"""

    if year < 12 or year > 27:
        raise ValueError("Only data between 2011 to 2026 are supported for now")

    try:
        page = httpx.get(BASE_URL.format(year - 1, year))
    except Exception as e:
        raise e

    target = get_month_year_target(month, year)
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

    temp_file = TEMP_DIR / f"rtt_{target}.csv"
    df.write_csv(temp_file)

    print(f"Fetched and extracted data for {target} - saved to {temp_file}")


def transform(local_path: Path) -> pl.DataFrame:
    """reads the raw csv and performs the cleaning transformations"""
    df = pl.read_csv(local_path, null_values=["", "*", "x", "X"])
    week_cols = [c for c in df.columns if c.startswith("Gt ") and "Weeks" in c]
    BUCKETS = [
        ("0_to_4w", [c for c in week_cols if int(c.split()[1]) < 4]),
        ("4_to_8w", [c for c in week_cols if 4 <= int(c.split()[1]) < 8]),
        ("8_to_12w", [c for c in week_cols if 8 <= int(c.split()[1]) < 12]),
        ("12_to_18w", [c for c in week_cols if 12 <= int(c.split()[1]) < 18]),
        ("18_to_26w", [c for c in week_cols if 18 <= int(c.split()[1]) < 26]),
        ("26_to_52w", [c for c in week_cols if 26 <= int(c.split()[1]) < 52]),
        ("gt_52w", [c for c in week_cols if int(c.split()[1]) >= 52]),
    ]

    df_clean = (
        df.filter(
            (pl.col("Treatment Function Code") != "C_999")
            & (pl.col("RTT Part Type") == "Part_2")  # incomplete pathways only
        )
        .with_columns([pl.sum_horizontal(cols).alias(name) for name, cols in BUCKETS])
        .with_columns(
            [
                pl.col("Period")
                .str.replace("RTT-", "")
                .str.replace("-", " ")
                .alias("Period")
            ]
        )
        .with_columns(
            [
                (
                    pl.col("0_to_4w")
                    + pl.col("4_to_8w")
                    + pl.col("8_to_12w")
                    + pl.col("12_to_18w")
                ).alias("count_within_18w"),
                (pl.col("18_to_26w") + pl.col("26_to_52w") + pl.col("gt_52w")).alias(
                    "count_beyond_18w"
                ),
                pl.col("gt_52w").alias("count_beyond_52w"),
            ]
        )
        .with_columns(
            [
                (pl.col("count_within_18w") / pl.col("Total All") * 100)
                .round(2)
                .alias("pct_within_18w"),
                (pl.col("count_beyond_18w") / pl.col("Total All") * 100)
                .round(2)
                .alias("pct_beyond_18w"),
                (pl.col("count_beyond_52w") / pl.col("Total All") * 100)
                .round(2)
                .alias("pct_beyond_52w"),
            ]
        )
        .select(
            [
                "Period",
                "Provider Org Code",
                "Provider Org Name",
                "Provider Parent Name",
                "Commissioner Org Code",
                "Commissioner Org Name",
                "Commissioner Parent Name",
                "Treatment Function Code",
                "Treatment Function Name",
                "RTT Part Type",
                "RTT Part Description",
                "Total All",
                "count_within_18w",
                "count_beyond_18w",
                "count_beyond_52w",
                "pct_within_18w",
                "pct_beyond_18w",
                "pct_beyond_52w",
                # the 7 bucket columns
                "0_to_4w",
                "4_to_8w",
                "8_to_12w",
                "12_to_18w",
                "18_to_26w",
                "26_to_52w",
                "gt_52w",
            ]
        )
    )
    return df_clean


def load(df: pl.DataFrame, month: str, year: int):
    """uploads cleaned data to databricks workspace"""
    from databricks.sdk.service.workspace import ImportFormat

    target = get_month_year_target(month, year)

    try:
        w = WorkspaceClient(
            host=DATABRICKS_HOST,
            client_id=DATABRICKS_CLIENT_ID,
            client_secret=DATABRICKS_API_KEY,
        )

        prev_year, curr_year = (year - 1) + 2000, year
        year_dir = f"{prev_year}-{curr_year}"
        data_file = f"{DATABRICKS_DATA_DIR}/{year_dir}/rtt_{target}_clean.csv.gz"

        buffer = io.BytesIO()
        df.write_csv(buffer, compression="gzip")
        buffer.seek(0)
        w.workspace.mkdirs(f"{DATABRICKS_DATA_DIR}/{year_dir}")
        w.workspace.upload(
            path=data_file,
            content=buffer,
            format=ImportFormat.RAW,
            overwrite=True,
        )
        print(f"uploaded rtt_{target}_clean.csv.gz — {len(df)} rows")

        # update manifest.json with the new month if not already present
        with w.workspace.download(f"{DATABRICKS_DATA_DIR}/manifest.json") as f:
            manifest: dict[str, list[str]] = json.load(f)

        if target not in manifest["months"]:
            manifest["months"].append(target)
            manifest["months"] = sorted(manifest["months"], key=month_label_sorter)
            w.workspace.upload(
                path=f"{DATABRICKS_DATA_DIR}/manifest.json",
                content=json.dumps(manifest, indent=2).encode(),
                format=ImportFormat.RAW,
                overwrite=True,
            )
            print(f"Updated manifest.json with month {target}")

    except Exception as e:
        print(f"Error fetching and uploading data for {target}")
        raise e


def main():
    month = os.getenv("MONTH", None)
    year = os.getenv("YEAR", "2026")[-2:]  # e.g. "26"
    month, year = parse_month(month), parse_year(year)

    assert year is not None, "Invalid year format. Use e.g. '2026' or '26'."

    if month is None:
        print("No month specified, defaulting to all months in the year.")
        months = calendar.month_abbr[1:]
    else:
        months = [month]

    for m in months:
        target = get_month_year_target(m, year)

        try:
            print(f"Fetching data for {target}...")
            extract(m, year)
        except Exception as e:
            print(f"Data for {target} not found: {e}")
            continue

        df_clean = transform(TEMP_DIR / f"rtt_{target}.csv")
        load(df_clean, m, year)


if __name__ == "__main__":
    main()
