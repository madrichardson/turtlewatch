#!/usr/bin/env python3
"""
Scrapes CPC marine heatwave outlook and updates JSON outputs.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
JSON_DIR = ROOT / "data" / "json"
JSON_DIR.mkdir(parents=True, exist_ok=True)

LAST_FILE = JSON_DIR / "heatwave.json"
HIST_FILE = JSON_DIR / "heatwave_history.json"

URL = "https://www.cpc.ncep.noaa.gov/products/Ocean_Pacific_forecast/marine_heatwave.shtml"

def scrape_heatwave():
    r = requests.get(URL, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Example scraping (adjust selectors if CPC page changes)
    paragraphs = soup.find_all("p")
    text = paragraphs[1].get_text().strip() if len(paragraphs) > 1 else "N/A"

    today = datetime.utcnow()
    date_yrmo = today.strftime("%Y%m")
    heat_date = today.strftime("%B %Y")
    heat_period = f"{heat_date} – {today.strftime('%B %Y')}"

    record = {
        "date_yrmo": date_yrmo,
        "heat_status": text,
        "heat_date": heat_date,
        "heat_period": heat_period
    }
    return record

def update_json(record):
    # Save last
    with open(LAST_FILE, "w") as f:
        json.dump(record, f, indent=4)

    # Update history
    history = {}
    if HIST_FILE.exists():
        with open(HIST_FILE) as f:
            try:
                history = json.load(f)
            except json.JSONDecodeError:
                history = {}

    history[record["date_yrmo"]] = {
        "heat_status": record["heat_status"],
        "heat_date": record["heat_date"],
        "heat_period": record["heat_period"]
    }

    with open(HIST_FILE, "w") as f:
        json.dump(history, f, indent=4)

if __name__ == "__main__":
    rec = scrape_heatwave()
    update_json(rec)