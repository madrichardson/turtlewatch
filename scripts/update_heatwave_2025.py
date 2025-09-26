"""Update TOTAL heatwave status from the CPS website.

This script scrapes the latest marine heatwave forecast from the NOAA PSL website,
compares it to the current data, and updates the necessary JSON files if new
information is available.
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


def get_latest_heatwave_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes heatwave narrative and dates from the PSL site."""
    html = session.get(url)
    html.raise_for_status()
    soup = BeautifulSoup(html.text, 'html.parser')

    pattern = re.compile(r'North Pacific|Pacific')

    try:
        heatwave_text = soup.find('strong', string=pattern).parent.text.strip()
        heatwave_date = soup.select_one('h5:-soup-contains("Forecast initial time") strong').text.strip()
        heatwave_period = soup.select_one('h5:-soup-contains("Forecast period") strong').text.strip()
    except AttributeError:
        print("Required HTML elements not found on the page.", file=sys.stderr)
        sys.exit(1)

    return {
        'heat_status': heatwave_text,
        'heat_date': heatwave_date,
        'heat_period': heatwave_period
    }


def save_outputs(new_data: Dict[str, Any], json_dir: Path, out_file_name: str, dated_template: str):
    """Save outputs locally (JSON + dated JSON)."""
    json_dir.mkdir(parents=True, exist_ok=True)

    # Main file
    local_data_path = json_dir / out_file_name
    with open(local_data_path, "w") as outfile:
        json.dump(new_data, outfile, indent=4)
    print(f"Saved new data to {local_data_path}")

    # Dated copy
    new_date_obj = parse(new_data['heat_date'])
    dated_ofile = dated_template.format(new_date_obj.strftime('%Y%m'))
    dated_path = json_dir / dated_ofile
    with open(dated_path, "w") as outfile:
        json.dump(new_data, outfile, indent=4)
    print(f"Saved dated copy to {dated_path}")


def main():
    """Controls and coordinates updates to the TOTAL heatwave status."""
    CONFIG = {
        # Base dir = project root (repo root)
        'ROOT_DIR': Path(__file__).resolve().parents[1],
        'WORK_DIR_NAME': 'work',
        'DATA_DIR_NAME': 'data',
        'JSON_DIR_NAME': 'json',
        'OUT_FILE_NAME': 'heatwave.json',
        'DATED_OUT_FILE_TEMPLATE': '{}_heatwave.json',
        'SCRAPE_URL': 'https://psl.noaa.gov/marine-heatwaves/'
    }

    ROOT_DIR = CONFIG['ROOT_DIR']
    JSON_DIR = ROOT_DIR / CONFIG['DATA_DIR_NAME'] / CONFIG['JSON_DIR_NAME']

    parser = argparse.ArgumentParser(description='Update TOTAL heatwave status. Use -o to force overwrite.')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Force update and overwrite existing data.')
    args = parser.parse_args()

    # Get new data
    print(f"Scraping heatwave data from {CONFIG['SCRAPE_URL']}")
    with requests.Session() as session:
        new_data = get_latest_heatwave_data(session, CONFIG['SCRAPE_URL'])

    new_date_obj = parse(new_data['heat_date'])

    # Load existing data if present
    local_data_path = JSON_DIR / CONFIG['OUT_FILE_NAME']
    local_date_obj = datetime(1990, 1, 1)
    try:
        with open(local_data_path, 'r') as f:
            local_data = json.load(f)
            local_date_obj = parse(local_data.get('heat_date', '1990-01-01'))
    except FileNotFoundError:
        print(f"No local file found, will create new.")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error reading local JSON: {e}", file=sys.stderr)

    # Compare and update
    print(f"Local data date: {local_date_obj.strftime('%Y-%m-%d')}")
    print(f"Website data date: {new_date_obj.strftime('%Y-%m-%d')}")

    if local_date_obj.date() == new_date_obj.date() and not args.overwrite:
        print("Heatwave info is up to date. No action needed.")
    else:
        print("New heatwave data available. Updating files...")
        save_outputs(new_data, JSON_DIR, CONFIG['OUT_FILE_NAME'], CONFIG['DATED_OUT_FILE_TEMPLATE'])

    sys.exit(0)


if __name__ == "__main__":
    main()