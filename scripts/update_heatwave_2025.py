
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
from typing import Dict, Any, Optional


def send_to_erddap(work_dir: Path, infile: Path, erddap_path: str, ofile: str) -> bool:
    """Sends files from the production server to an ERDDAP server via SCP.

    Args:
        work_dir (Path): The local directory of the file.
        infile (Path): The name of the local file.
        erddap_path (str): The remote directory path on the ERDDAP server.
        ofile (str): The name of the remote file.

    Returns:
        bool: True if the file transfer was successful, False otherwise.
    """
    cmd = [
        'scp',
        str(work_dir / infile),
        f'cwatch@192.168.31.15:{os.path.join(erddap_path, ofile)}'
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


def safe_parse_date(s, fallback=datetime(1990, 1, 1)):
    """Parse a date string safely; return fallback on failure (handles 'Unknown', '', None)."""
    if not s or not isinstance(s, str):
        return fallback
    try:
        return parse(s, fuzzy=True)
    except Exception:
        return fallback


def _text(elem) -> str:
    return elem.get_text(" ", strip=True) if elem else ""


def _find_region_paragraph(soup: BeautifulSoup, region_pattern: re.Pattern) -> Optional[str]:
    """
    Find a <strong> tag whose text matches region_pattern (e.g., 'Tropical Pacific' or 'North Pacific'),
    then return the full text of its parent paragraph.
    """
    strong = soup.find('strong', string=region_pattern)
    if strong and strong.parent:
        # If the <strong> lives inside a <p>, take the whole paragraph text
        p = strong.find_parent('p')
        if p:
            return _text(p)
        # Fallback: take the immediate parent's text
        return _text(strong.parent)
    return None


def get_latest_heatwave_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes heatwave narrative and dates from the website.

    Args:
        session (requests.Session): The requests session object to use for the HTTP request.
        url (str): The URL of the website to scrape.

    Returns:
        Dict[str, Any]: Dictionary with date/period + region summaries.
    """
    html = session.get(url, timeout=30)
    html.raise_for_status()
    soup = BeautifulSoup(html.text, 'html.parser')

    # --- Date / period (as in your original approach) ---
    try:
        # These headings appear on the page; collect their <strong> values
        heatwave_date = soup.select_one('h5:-soup-contains("Forecast initial time") strong')
        heatwave_period = soup.select_one('h5:-soup-contains("Forecast period") strong')
        heatwave_date = _text(heatwave_date)
        heatwave_period = _text(heatwave_period)
    except Exception:
        print("Could not find Forecast initial time / Forecast period.", file=sys.stderr)
        heatwave_date = ""
        heatwave_period = ""

    # --- Region paragraphs ---
    tp_pat = re.compile(r'\bTropical Pacific\b', re.I)
    np_pat = re.compile(r'\bNorth Pacific\b', re.I)

    tropical_pacific = _find_region_paragraph(soup, tp_pat) or ""
    north_pacific    = _find_region_paragraph(soup, np_pat) or ""

    if not tropical_pacific and not north_pacific:
        # Fallback to any paragraph containing "Pacific" if region headings moved
        any_pacific = soup.find('p', string=re.compile(r'Pacific', re.I))
        if any_pacific:
            txt = _text(any_pacific)
            # Heuristic split if both mentioned in one paragraph
            if re.search(tp_pat, txt) and re.search(np_pat, txt):
                tropical_pacific = txt
                north_pacific = txt
            elif re.search(tp_pat, txt):
                tropical_pacific = txt
            elif re.search(np_pat, txt):
                north_pacific = txt

    # Backward-compat combined status string
    parts = []
    if north_pacific:
        parts.append(f"North Pacific: {north_pacific}")
    if tropical_pacific:
        parts.append(f"Tropical Pacific: {tropical_pacific}")
    heat_status = " | ".join(parts) if parts else "No regional summaries found."

    return {
        'heat_date': heatwave_date,
        'heat_period': heatwave_period,
        'north_pacific': north_pacific,
        'tropical_pacific': tropical_pacific,
        'heat_status': heat_status,  # backward-compat combined text
        'source_url': url
    }


def main():
    """Controls and coordinates updates to the TOTAL heatwave status."""
    CONFIG = {
        'ROOT_DIR': Path.cwd().resolve(),
        'WORK_DIR_NAME': 'work',
        'DATA_DIR_NAME': 'data',
        'JSON_DIR_NAME': 'json',
        'OUT_FILE_NAME': 'heatwave.json',
        'DATED_OUT_FILE_TEMPLATE': '{}_heatwave.json',
        'SCRAPE_URL': 'https://psl.noaa.gov/marine-heatwaves/'
    }

    ROOT_DIR = CONFIG['ROOT_DIR']
    WORK_DIR = ROOT_DIR / CONFIG['WORK_DIR_NAME']
    JSON_DIR = ROOT_DIR / CONFIG['DATA_DIR_NAME'] / CONFIG['JSON_DIR_NAME']
    JSON_DIR.mkdir(parents=True, exist_ok=True)

    parser = argparse.ArgumentParser(description='Update TOTAL heatwave status. Use -o to force overwrite.')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Force update and overwrite existing data.')
    args = parser.parse_args()

    print(f"Scraping heatwave data from {CONFIG['SCRAPE_URL']}")
    with requests.Session() as session:
        new_data = get_latest_heatwave_data(session, CONFIG['SCRAPE_URL'])

    # Robust date parsing (won’t crash on “Unknown”)
    new_date_obj = safe_parse_date(new_data.get('heat_date'))

    # Get local data date
    local_data_path = JSON_DIR / CONFIG['OUT_FILE_NAME']
    local_date_obj = datetime(1990, 1, 1)

    try:
        with open(local_data_path, 'r') as f:
            local_data = json.load(f)
            local_date_obj = safe_parse_date(local_data.get('heat_date'))
    except FileNotFoundError:
        print(f"Local file {local_data_path} not found. A new file will be created.")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error reading or parsing local JSON file: {e}", file=sys.stderr)

    print(f"Local data date: {local_date_obj.strftime('%Y-%m-%d')}")
    print(f"Website data date: {new_date_obj.strftime('%Y-%m-%d')}")

    if local_date_obj.date() == new_date_obj.date() and not args.overwrite:
        print("Heatwave info is up to date. No action needed.")
    else:
        print("New heatwave data available. Updating files...")

        # Write main JSON
        with open(local_data_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved new data to {local_data_path}")

        # Dated copy
        dated_ofile = CONFIG['DATED_OUT_FILE_TEMPLATE'].format(new_date_obj.strftime('%Y%m'))
        dated_path = JSON_DIR / dated_ofile
        with open(dated_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved dated copy to {dated_path}")

    sys.exit(0)


if __name__ == "__main__":
    main()
