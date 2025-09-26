#!/usr/bin/env python3
"""
Scrapes ENSO advisory and updates JSON outputs.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
from pathlib import Path

__author__ = "Dale Robinson"
__credits__ = ["Dale Robinson"]
__license__ = "GPL"
__version__ = "2.1"
__maintainer__ = "Dale Robinson"
__email__ = "dale.Robinson@noaa.gov"
__status__ = "Production"


ROOT = Path(__file__).resolve().parents[1]
JSON_DIR = ROOT / "data" / "json"
JSON_DIR.mkdir(parents=True, exist_ok=True)

LAST_FILE = JSON_DIR / "elnino_last.json"
HIST_FILE = JSON_DIR / "elnino_history.json"

URL = "https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/enso_advisory/ensodisc.shtml"

def scrape_enso():
    r = requests.get(URL, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    # Example scraping logic (adjust if CPC changes structure)
    paragraphs = soup.find_all("p")
    synopsis = paragraphs[1].get_text().strip() if len(paragraphs) > 1 else "N/A"
    status = "N/A"
    for p in paragraphs:
        txt = p.get_text()
        if "El Niño" in txt or "La Niña" in txt or "ENSO-neutral" in txt:
            status = txt.strip()
            break

    today = datetime.utcnow()
    date_yrmo = today.strftime("%Y%m")
    date_iso = today.strftime("%Y-%m-%dT00:00:00")
    date_print = today.strftime("%d %B, %Y")

    record = {
        "date_yrmo": date_yrmo,
        "date_iso": date_iso,
        "date_print": date_print,
        "synopsis": synopsis,
        "status": status
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
        "status": record["status"],
        "synopsis": record["synopsis"],
        "date_print": record["date_print"]
    }

    with open(HIST_FILE, "w") as f:
        json.dump(history, f, indent=4)

if __name__ == "__main__":
    rec = scrape_enso()
    update_json(rec)