#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Scrape CPC website for El Nino data.

This script obtains the latest status and summary for El Nino from the
CLIMATE PREDICTION CENTER website. It stores these data in a JSON file,
archives it locally, and uploads it to ERDDAP.
"""

from bs4 import BeautifulSoup
import requests
import re
from dateutil.parser import parse
import os
import shutil
import json
import sys
from datetime import datetime
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Any

__author__ = "Dale Robinson"
__credits__ = ["Dale Robinson"]
__license__ = "GPL"
__version__ = "2.1"
__maintainer__ = "Dale Robinson"
__email__ = "dale.Robinson@noaa.gov"
__status__ = "Production"


# --- Configuration ---
CONFIG = {
    # Base dir = repo root (no hardcoded server paths)
    'ROOT_DIR': Path(__file__).resolve().parents[1],
    'CPC_URL_BASE': 'https://www.cpc.ncep.noaa.gov/products/analysis_monitoring',
    'JSON_FILE_NAME': 'elnino_last.json',
    'JSON_FILE_ARCHIVE_TEMPLATE': '{}_elnino.json',
}


def get_latest_enso_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes the latest ENSO status and synopsis from the CPC website."""
    print(f"Scraping CPC ENSO page: {url}")
    try:
        html = session.get(url)
        html.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}", file=sys.stderr)
        sys.exit(3)

    soup = BeautifulSoup(html.text, 'html.parser')

    # Scrape Date
    try:
        ft = soup.find_all('font')
        ft_clean = [f.contents[0].strip() for f in ft]
        idx = ft_clean.index('issued by')
        date_text = ft_clean[idx + 2]
        date_obj = parse(date_text.strip().replace("\n", ""))
    except Exception:
        print("Could not parse advisory date.", file=sys.stderr)
        sys.exit(3)

    # Scrape Synopsis
    try:
        synopsis_heading = soup.find('u', string=re.compile('Synopsis'))
        synopsis_text = synopsis_heading.find_next("strong").string
    except Exception:
        synopsis_text = "Not available"

    # Scrape Status
    try:
        status_heading = soup.find('strong', string=re.compile('ENSO Alert'))
        status_text = status_heading.find_next('a').get_text().strip()
    except Exception:
        status_text = "Not available"

    return {
        "date_yrmo": date_obj.strftime('%Y%m'),
        "date_iso": date_obj.isoformat(),
        "date_print": date_obj.strftime('%d %B, %Y'),
        "synopsis": synopsis_text,
        "status": status_text,
    }


def save_outputs(scraped_data: Dict[str, Any], json_dir: Path, main_file: str, archive_template: str, custom_date: bool):
    """Save ENSO data to repo (main JSON + dated archive)."""
    json_dir.mkdir(parents=True, exist_ok=True)

    # Save dated archive
    dated_file_name = archive_template.format(scraped_data['date_yrmo'])
    dated_file_path = json_dir / dated_file_name
    with open(dated_file_path, 'w') as f:
        json.dump(scraped_data, f, indent=4)
    print(f"Archived ENSO advisory: {dated_file_path}")

    # Save main file only if this is the latest advisory
    if not custom_date:
        main_path = json_dir / main_file
        with open(main_path, 'w') as f:
            json.dump(scraped_data, f, indent=4)
        print(f"Updated main file: {main_path}")


def main():
    """Controls the scraping and updating of ENSO data."""
    ROOT_DIR = CONFIG['ROOT_DIR']
    JSON_DIR = ROOT_DIR / 'data' / 'json'

    # Parse arguments
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-d', '--date',
        help='Date for advisory (YYYY-MM). If not given, fetch latest.',
        type=str
    )
    args = parser.parse_args()

    # Determine URL
    if args.date:
        try:
            custom_date = parse(args.date)
            url_part = f"enso_disc_{custom_date.strftime('%b%Y').lower()}"
        except ValueError:
            parser.error("Invalid date format. Use YYYY-MM.")
        custom_mode = True
    else:
        url_part = "enso_advisory"
        custom_mode = False

    url = f"{CONFIG['CPC_URL_BASE']}/{url_part}/ensodisc.shtml"

    # Get last stored advisory date if available
    last_date_obj = datetime.min.date()
    main_path = JSON_DIR / CONFIG['JSON_FILE_NAME']
    if main_path.exists():
        try:
            with open(main_path, 'r') as f:
                last_dict = json.load(f)
                last_date_obj = parse(last_dict['date_iso']).date()
        except Exception:
            print("Local JSON corrupted or unreadable. Proceeding with update.")

    # Scrape CPC
    with requests.Session() as session:
        scraped_data = get_latest_enso_data(session, url)

    # If fetching latest, check if new data is newer
    if not custom_mode:
        new_date_obj = parse(scraped_data['date_iso']).date()
        if new_date_obj <= last_date_obj:
            print(f"No update needed (latest {new_date_obj}, stored {last_date_obj}).")
            sys.exit(0)

    # Save outputs locally
    save_outputs(scraped_data, JSON_DIR, CONFIG['JSON_FILE_NAME'], CONFIG['JSON_FILE_ARCHIVE_TEMPLATE'], custom_mode)
    sys.exit(0)


if __name__ == "__main__":
    main()