# NHSE MONTHLY RTT DATA DASHBOARD

This repository contains code and example data for exploring the NHS England (NHSE) monthly Referral to Treatment (RTT) data. The RTT data provides insights into the performance of healthcare services in England, specifically focusing on the time patients wait for treatment after being referred by a healthcare professional. The live marimo dashboard is hosted on Molab  [![Open in molab](https://molab.marimo.io/molab-shield.svg)](https://molab.marimo.io/notebooks/nb_LT39YgqBXBvdt4j1XJ5CQ2/app)

## Data Source
The RTT data is sourced from the NHS England statistics website, which publishes monthly reports and datasets. See [NHSE RTT Data](https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/).

## Code Overview
The ETL pipeline script that scrape the NHSE RTT pages, download the data, process it, and load it into a Databricks workspace live in `scripts/`. The script run on GitHub Actions on a schedule every month. The dashboard code/notebook is in `dashboard.py` and the static notebook (as preview) is in `dashboard.html`.

## More
I will be adding more charts and other analyses over time, stay tuned! [Marimo](https://marimo.io/) is awesome!
