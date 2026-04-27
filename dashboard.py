# /// script
# dependencies = [
#     "altair==6.0.0",
#     "databricks-sdk==0.105.0",
#     "marimo",
#     "polars==1.39.3",
#     "python-dotenv==1.2.2",
#     "wigglystuff==0.3.5",
# ]
# requires-python = ">=3.13"
# ///

import marimo

__generated_with = "0.23.0"
app = marimo.App(
    width="medium",
    css_file="/usr/local/_marimo/custom.css",
    auto_download=["html"],
)


@app.cell
def _():
    import os
    import calendar
    from pathlib import Path
    import json

    import marimo as mo
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import ObjectType
    from wigglystuff import EnvConfig
    import polars as pl
    import altair as alt

    return EnvConfig, Path, WorkspaceClient, alt, calendar, json, mo, os, pl


@app.cell
def _(mo):
    mo.image(src="nhs_logo.svg")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(rf"""
    # NHS Monthly Referral-To-Treatment (RTT) Data Exploration

    This notebook uses the monthly RTT data available on the NHSE statistics website [here](https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2025-26/).

    The full code, including the backend that runs on Github actions is available on Github. [![GitHub](https://img.shields.io/badge/github-repo-blue?logo=github)](https://github.com/DrOncogene/nhs-pipeline-demo)
    """)
    return


@app.cell
def _(mo):
    get_show_env, set_show_env = mo.state(False)


    def toggle_env_display(v):
        set_show_env(not get_show_env())


    show_env_btn = mo.ui.button(
        label="Show Environment", on_click=toggle_env_display
    )
    hide_env_btn = mo.ui.button(
        label="Hide Environment", on_click=toggle_env_display
    )
    return get_show_env, hide_env_btn, show_env_btn


@app.cell
def _(EnvConfig, WorkspaceClient, mo, os):
    def check_key(api_key: str):
        w = WorkspaceClient(
            host=os.getenv("***REMOVED***"),
            client_id=os.getenv("***REMOVED***"),
            client_secret=api_key,
        )
        me = w.current_user.me()

        return me.user_name


    valid_env = mo.ui.anywidget(
        EnvConfig(
            variables={
                "***REMOVED***": None,
                "***REMOVED***": None,
                "***REMOVED***": check_key,
                "***REMOVED***": None,
            }
        )
    )
    return (valid_env,)


@app.cell
def _(get_show_env, hide_env_btn, mo, show_env_btn, valid_env):
    mo.md(
        f"### Databricks Credentials (Note: Enter only if empty)<br><br>{valid_env}<br>{hide_env_btn}"
    ) if get_show_env() else mo.md(f"{show_env_btn}")
    return


@app.cell
def _(valid_env):
    valid_env.require_valid()
    return


@app.cell
def _(Path, WorkspaceClient, json, valid_env):
    w = WorkspaceClient(
        host=valid_env["***REMOVED***"],
        client_id=valid_env["***REMOVED***"],
        client_secret=valid_env["***REMOVED***"],
    )

    ***REMOVED*** = Path(valid_env["***REMOVED***"])
    with w.workspace.download(***REMOVED*** / "manifest.json") as f:
        manifest: dict = json.load(f)
    return ***REMOVED***, manifest, w


@app.cell
def _(calendar, manifest: dict, mo):
    months = ["All", *calendar.month_name[1:]]
    available = manifest["months"]
    get_start, set_start = mo.state(available[-12])
    get_end, set_end = mo.state(available[-1])
    return available, get_end, get_start, set_end, set_start


@app.cell
def _(available, get_end, get_start, mo, set_end, set_start):
    start_select = mo.ui.dropdown(
        options=available,
        label="**From**",
        value=get_start(),
        allow_select_none=False,
        searchable=True,
        on_change=lambda v: set_start(v),
    )

    end_select = mo.ui.dropdown(
        options=available,
        label="**To**",
        value=get_end(),
        allow_select_none=False,
        searchable=True,
        on_change=lambda v: set_end(v),
    )
    return end_select, start_select


@app.cell
def _(available, end_select, mo, set_end, set_start, start_select):
    def reset_range(value):
        set_start(available[-12])
        set_end(available[-1])


    refresh_btn = mo.ui.button(on_click=reset_range, label="Reset Data")

    mo.vstack(
        [
            mo.md("## Select data range:"),
            mo.hstack(
                [start_select, end_select, refresh_btn], justify="start", gap=1.0
            ),
        ],
        gap=2.0,
    )
    return


@app.cell
def _(***REMOVED***, available, calendar, get_end, get_start, mo, pl, w):
    @mo.cache
    def load_data(start: str, end: str) -> pl.DataFrame:
        """loads data files from databricks workspace based on start and end month,
        concatenates them into a df and adds year, month_num and period_label columns
        for easier filtering and plotting downstream.
        """
        global available, w, ***REMOVED***

        def sorter(path: str) -> int:
            name = path.split("/")[-1]
            return available.index(name[4:9])

        start_idx = available.index(start)
        end_idx = available.index(end)
        if start_idx > end_idx:
            return df
        months_in_range = available[start_idx : end_idx + 1]
        files_in_range = []
        for item in w.workspace.list(***REMOVED***, recursive=True):
            file_name = item.path.split("/")[-1]
            if not file_name[4:9] in months_in_range:
                continue
            files_in_range.append(item.path)

        files_in_range.sort(key=sorter)
        data_in_range = []
        for path in files_in_range:
            with w.workspace.download(path) as f:
                data_in_range.append(pl.read_csv(f))

        full_data = pl.concat(data_in_range).with_columns(
            [
                pl.col("Period")
                .str.strip_chars()
                .str.split(" ")
                .list.get(1)
                .cast(pl.Int32)
                .alias("year"),
                pl.col("Period")
                .str.strip_chars()
                .str.split(" ")
                .list.get(0)
                .map_elements(
                    lambda m: list(calendar.month_name).index(m),
                    return_dtype=pl.Int32,
                )
                .alias("month_num"),
                (
                    pl.col("Period").str.strip_chars().str.slice(0, 3)
                    + pl.lit(" ")
                    + pl.col("Period").str.split(" ").list.get(1)
                ).alias("period_label"),
            ]
        )
        return full_data


    df = load_data(get_start(), get_end())
    df
    return (df,)


@app.cell
def _(alt, df, pl):
    bucket_order = [
        "0_to_4w",
        "4_to_8w",
        "8_to_12w",
        "12_to_18w",
        "18_to_26w",
        "26_to_52w",
        "gt_52w",
    ]

    bucket_labels = {
        "0_to_4w": "0–4w",
        "4_to_8w": "4–8w",
        "8_to_12w": "8–12w",
        "12_to_18w": "12–18w",
        "18_to_26w": "18–26w",
        "26_to_52w": "26–52w",
        "gt_52w": ">52w",
    }

    df_national_rtt_dist = (
        df.select(bucket_order)
        .sum()
        .unpivot(variable_name="bucket", value_name="total_patients")
        .with_columns(
            pl.col("bucket").replace(bucket_labels).alias("label"),
            pl.col("bucket")
            .is_in(["18_to_26w", "26_to_52w", "gt_52w"])
            .alias("breaching"),
        )
    )

    national_rtt_chart = (
        alt.Chart(df_national_rtt_dist)
        .mark_bar()
        .encode(
            x=alt.X(
                "label:N", sort=list(bucket_labels.values()), title="Weeks waiting"
            ),
            y=alt.Y(
                "total_patients:Q", title="Patients", axis=alt.Axis(format="~s")
            ),
            color=alt.Color(
                "breaching:N",
                scale=alt.Scale(
                    domain=[False, True],
                    range=["#005EB8", "#DA291C"],  # NHS blue / NHS red
                ),
                legend=alt.Legend(title="Breaching 18w standard"),
            ),
            tooltip=[
                alt.Tooltip("label:N", title="Band"),
                alt.Tooltip("total_patients:Q", title="Patients", format=","),
            ],
        )
        .configure_axis(labelFontSize=12, titleFontSize=14, tickSize=12)
        .configure_legend(labelFontSize=12, titleFontSize=14)
        .properties(
            width=600,
            height=350,
        )
    )
    return (national_rtt_chart,)


@app.cell
def _(mo, national_rtt_chart):
    mo.vstack(
        [
            mo.md(
                f"### National RTT Waiting Time Distribution — Incomplete Pathways"
            ),
            national_rtt_chart,
        ],
        gap=2.0,
    )
    return


@app.cell
def _(alt, df, pl):
    df_speciality_perf = (
        df.group_by("Treatment Function Name")
        .agg(
            [
                pl.sum("Total All").alias("total_waiting"),
                pl.sum("count_within_18w").alias("total_within_18w"),
                pl.sum("count_beyond_52w").alias("total_beyond_52w"),
            ]
        )
        .with_columns(
            (pl.col("total_within_18w") / pl.col("total_waiting") * 100).alias(
                "pct_within_18w"
            )
        )
        # Sort ascending so worst performers are at the top
        .sort("pct_within_18w", descending=False)
    )

    # Preserve the sort order from polars in altair
    sort_order = df_speciality_perf["Treatment Function Name"].to_list()

    _bars = (
        alt.Chart(df_speciality_perf)
        .mark_bar()
        .encode(
            y=alt.Y(
                "Treatment Function Name:N",
                sort=sort_order,
                title="Speciality",
            ),
            x=alt.X(
                "pct_within_18w:Q",
                title="% patients waiting ≤18 weeks",
                scale=alt.Scale(domain=[0, 100]),
                axis=alt.Axis(format=".0f", tickCount=6),
            ),
            color=alt.condition(
                alt.datum.pct_within_18w >= 92,
                alt.value("#005EB8"),  # blue = meeting standard
                alt.value("#DA291C"),  # red = breaching
            ),
            tooltip=[
                alt.Tooltip("Treatment Function Name:N", title="speciality"),
                alt.Tooltip(
                    "pct_within_18w:Q", title="% within 18w", format=".1f"
                ),
                alt.Tooltip("total_waiting:Q", title="Total waiting", format=","),
                alt.Tooltip(
                    "total_beyond_52w:Q", title=">52w waiters", format=","
                ),
            ],
        )
    )

    # 92% reference line
    _rule = (
        alt.Chart(pl.DataFrame({"threshold": [92]}))
        .mark_rule(
            color="#FFB81C",
            strokeDash=[5, 3],
            strokeWidth=2,
        )
        .encode(x="threshold:Q")
    )

    # Annotation label on the rule
    _rule_label = (
        alt.Chart(pl.DataFrame({"threshold": [92], "label": ["92% standard"]}))
        .mark_text(
            align="left",
            dx=4,
            dy=-8,
            fontSize=11,
            color="#FFB81C",
            fontWeight="bold",
        )
        .encode(x="threshold:Q", text="label:N")
    )

    speciality_perf_chart = (
        (_bars + _rule + _rule_label)
        .properties(width=550, height=480)
        .configure_axis(labelFontSize=12, titleFontSize=14)
        .configure_view(strokeWidth=0)
    )
    return (speciality_perf_chart,)


@app.cell
def _(mo, speciality_perf_chart):
    mo.vstack(
        [
            mo.md(
                "### % of Incomplete Pathway Performance by speciality<br><small style='color: gray;'>Red bars are breaching the NHS constitutional standard. Sorted by performance ascending<small>"
            ),
            speciality_perf_chart,
        ],
        gap=2.0,
    )
    return


@app.cell
def _(df, end_select, mo, start_select):
    # get all specialities in the current data
    all_specialities = (
        df.select("Treatment Function Name")
        .unique()
        .sort("Treatment Function Name")["Treatment Function Name"]
        .to_list()
    )

    # default selected specialities
    DEFAULT_SPECIALITIES = [
        "Trauma and Orthopaedic Service",
        "Ear Nose and Throat Service",
        "Gastroenterology Service",
        "Gynaecology Service",
        "General Surgery Service",
        "Ophthalmology Service",
    ]
    # ensure the defaults
    defaults = [s for s in DEFAULT_SPECIALITIES if s in all_specialities]

    speciality_select = mo.ui.multiselect(
        options=all_specialities,
        value=defaults,
        label="**Specialities**",
    )

    mo.vstack(
        [
            mo.md("## Filter speciality trend plot"),
            mo.hstack(
                [start_select, end_select, speciality_select],
                justify="start",
                gap=5.0,
            ),
        ],
        gap=2.0,
    )
    return (speciality_select,)


@app.cell
def _(df, mo, pl, speciality_select):
    df_speciality_trend = (
        df.filter(pl.col("Treatment Function Name").is_in(speciality_select.value))
        .group_by(
            [
                "Period",
                "period_label",
                "year",
                "month_num",
                "Treatment Function Name",
            ]
        )
        .agg(
            [
                pl.sum("Total All").alias("total_waiting"),
                pl.sum("count_within_18w").alias("total_within_18w"),
                pl.sum("count_beyond_52w").alias("total_beyond_52w"),
            ]
        )
        .with_columns(
            (pl.col("total_within_18w") / pl.col("total_waiting") * 100).alias(
                "pct_within_18w"
            )
        )
        .sort(["year", "month_num", "Treatment Function Name"])
    )

    period_sort_order = (
        df_speciality_trend.unique("period_label")
        .sort(["year", "month_num"])["period_label"]
        .to_list()
    )

    mo.vstack(
        [mo.md("### Per specility trend aggregated data"), df_speciality_trend],
        gap=2.0,
    )
    return df_speciality_trend, period_sort_order


@app.cell
def _(alt, df_speciality_trend, mo, period_sort_order, pl, speciality_select):
    if (
        df_speciality_trend["period_label"].n_unique() < 2
        or len(speciality_select.value) < 1
    ):
        speciality_trend_chart = mo.md(
            "**Select at least two months and one speciality to display a trend.**"
        )
    else:
        lines = (
            alt.Chart(df_speciality_trend)
            .mark_line(point=True, strokeWidth=2)
            .encode(
                x=alt.X(
                    "period_label:N",
                    sort=period_sort_order,
                    title=None,
                    axis=alt.Axis(labelAngle=-35, labelFontSize=10),
                ),
                y=alt.Y(
                    "pct_within_18w:Q",
                    title="% patients waiting ≤18 weeks",
                    scale=alt.Scale(domain=[0, 100]),
                    axis=alt.Axis(format=".0f", tickCount=6),
                ),
                color=alt.Color(
                    "Treatment Function Name:N",
                    legend=alt.Legend(
                        title="Specialty",
                        labelLimit=220,
                        orient="bottom",
                        columns=2,
                    ),
                ),
                tooltip=[
                    alt.Tooltip("period_label:N", title="Period"),
                    alt.Tooltip("Treatment Function Name:N", title="Specialty"),
                    alt.Tooltip(
                        "pct_within_18w:Q",
                        title="% within 18w",
                        format=".1f",
                    ),
                    alt.Tooltip(
                        "total_waiting:Q",
                        title="Total waiting",
                        format=",",
                    ),
                    alt.Tooltip(
                        "total_beyond_52w:Q",
                        title=">52w waiters",
                        format=",",
                    ),
                ],
            )
        )

        _rule = (
            alt.Chart(pl.DataFrame({"threshold": [92]}))
            .mark_rule(color="#FFB81C", strokeDash=[5, 3], strokeWidth=1.5)
            .encode(y="threshold:Q")
        )

        _rule_label = (
            alt.Chart(pl.DataFrame({"threshold": [92], "label": ["92% standard"]}))
            .mark_text(
                align="right",
                dx=-4,
                dy=-8,
                fontSize=10,
                color="#FFB81C",
                fontWeight="bold",
            )
            .encode(y="threshold:Q", text="label:N")
        )

        speciality_trend_chart = (
            (lines + _rule + _rule_label)
            .properties(
                width=680,
                height=380,
            )
            .configure_axis(labelFontSize=12, titleFontSize=14, tickSize=12)
            .configure_view(strokeWidth=0)
            .configure_point(size=50)
            .configure_legend(labelFontSize=12, titleFontSize=14)
        )
    return (speciality_trend_chart,)


@app.cell
def _(mo, speciality_trend_chart):
    mo.vstack(
        [
            mo.md(
                "### 18-Week RTT Performance Trend — Incomplete Pathways<br><small style='color: gray;'>% of patients waiting ≤18 weeks by specialty. Yellow line = 92% constitutional standard.<small>"
            ),
            speciality_trend_chart,
        ],
        gap=2.0,
    )
    return


@app.cell
def _(mo):
    mo.hstack(
        [
            mo.md("### More charts incoming..."),
            mo.status.spinner(remove_on_exit=False),
        ],
        justify="start",
        gap=2.0,
        align="center",
    ).callout(kind="info")
    return


if __name__ == "__main__":
    app.run()
