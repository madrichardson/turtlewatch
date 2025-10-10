
"""Update TOTAL heatwave status from the NOAA PSL Marine Heatwave page.

Scrapes the latest Marine Heatwave forecasts from the NOAA PSL “Current Report”
section, capturing both Tropical Pacific and North Pacific narratives, along
with forecast dates and periods.
"""

from bs4 import BeautifulSoup
import requests
import os
import subprocess
from dateutil.parser import parse
from datetime import datetime
import json
import sys
import re
import argparse
from pathlib import Path
from typing import Dict, Any


def send_to_erddap(work_dir: Path, infile: Path, erddap_path: str, ofile: str) -> bool:
    """Sends files from the production server to an ERDDAP server via SCP."""
    cmd = [
        "scp",
        str(work_dir / infile),
        f"cwatch@192.168.31.15:{os.path.join(erddap_path, ofile)}"
    ]
    print(f"Executing: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Successfully sent {ofile} to ERDDAP.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"SCP command failed with exit code {e.returncode}.", file=sys.stderr)
        print("Error details:", e.stderr, file=sys.stderr)
    except FileNotFoundError:
        print("SCP command not found. Is it installed and in your PATH?", file=sys.stderr)
    return False


def get_latest_heatwave_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes Marine Heatwave information from the NOAA PSL “Current Report” section."""
    html = session.get(url)
    html.raise_for_status()
    soup = BeautifulSoup(html.text, "html.parser")

    # Locate the "Current Report" section
    report_div = soup.find("div", id="report")
    if not report_div:
        print("Could not find the '#report' section on the page.", file=sys.stderr)
        sys.exit(1)

    # --- Extract Forecast Dates ---
    forecast_period = None
    forecast_date = None

    # h5 tags with "Forecast period" and "Forecast initial time"
    for h5 in report_div.find_all("h5"):
        txt = h5.text.strip()
        if "Forecast period" in txt:
            strong = h5.find("strong")
            if strong:
                forecast_period = strong.text.strip()
        elif "Forecast initial time" in txt:
            strong = h5.find("strong")
            if strong:
                forecast_date = strong.text.strip()

    # --- Extract Regional Summaries ---
    tropical_text = ""
    north_text = ""
    for h3 in report_div.find_all("h3"):
        title = h3.text.strip()
        if "Tropical Pacific" in title:
            p = h3.find_next("p")
            if p:
                tropical_text = p.text.strip()
        elif "North Pacific" in title:
            p = h3.find_next("p")
            if p:
                north_text = p.text.strip()

    if not tropical_text and not north_text:
        print("No Tropical or North Pacific text found in #report.", file=sys.stderr)

    # --- Combine summaries ---
    if tropical_text and north_text:
        combined_summary = f"Tropical Pacific: {tropical_text}\n\nNorth Pacific: {north_text}"
    elif tropical_text:
        combined_summary = tropical_text
    elif north_text:
        combined_summary = north_text
    else:
        combined_summary = "No current marine heatwave summaries found."

    # --- Build output dictionary ---
    return {
        "heat_status": combined_summary,
        "heat_date": forecast_date or datetime.now().strftime("%B %d, %Y"),
        "heat_period": forecast_period or "Not available"
    }


def main():
    """Controls and coordinates updates to the TOTAL heatwave status."""
    CONFIG = {
        "ROOT_DIR": Path(__file__).resolve().parents[1],
        "WORK_DIR_NAME": "work",
        "DATA_DIR_NAME": "data",
        "JSON_DIR_NAME": "json",
        "OUT_FILE_NAME": "heatwave.json",
        "DATED_OUT_FILE_TEMPLATE": "{}_heatwave.json",
        "SCRAPE_URL": "https://psl.noaa.gov/marine-heatwaves/#report"
    }

    ROOT_DIR = CONFIG["ROOT_DIR"]
    WORK_DIR = ROOT_DIR / CONFIG["WORK_DIR_NAME"]
    JSON_DIR = ROOT_DIR / CONFIG["DATA_DIR_NAME"] / CONFIG["JSON_DIR_NAME"]

    parser = argparse.ArgumentParser(description="Update TOTAL heatwave status. Use -o to force overwrite.")
    parser.add_argument("-o", "--overwrite", action="store_true", help="Force update and overwrite existing data.")
    args = parser.parse_args()

    print(f"Scraping heatwave data from {CONFIG['SCRAPE_URL']}")
    with requests.Session() as session:
        new_data = get_latest_heatwave_data(session, CONFIG["SCRAPE_URL"])

    try:
        new_date_obj = parse(new_data["heat_date"])
    except Exception:
        new_date_obj = datetime.now()

    local_data_path = JSON_DIR / CONFIG["OUT_FILE_NAME"]
    local_date_obj = datetime(1990, 1, 1)

    try:
        with open(local_data_path, "r") as f:
            local_data = json.load(f)
            local_date_obj = parse(local_data.get("heat_date", "1990-01-01"))
    except FileNotFoundError:
        print(f"Local file {local_data_path} not found. A new file will be created.")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error reading local JSON file: {e}", file=sys.stderr)

    print(f"Local data date: {local_date_obj.strftime('%Y-%m-%d')}")
    print(f"Website data date: {new_date_obj.strftime('%Y-%m-%d')}")

    if local_date_obj.date() == new_date_obj.date() and not args.overwrite:
        print("Heatwave info is already up to date.")
    else:
        print("New heatwave data available. Updating files...")
        JSON_DIR.mkdir(parents=True, exist_ok=True)

        with open(local_data_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved new data to {local_data_path}")

        dated_ofile = CONFIG["DATED_OUT_FILE_TEMPLATE"].format(new_date_obj.strftime("%Y%m"))
        dated_path = JSON_DIR / dated_ofile
        with open(dated_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved dated copy to {dated_path}")

    sys.exit(0)


if __name__ == "__main__":
    main()
