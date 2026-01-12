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
import time
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


def fetch_with_retry_html(
    session: requests.Session,
    url: str,
    retries: int = 5,
    timeout: int = 30,
    backoff_seconds: int = 20,
) -> requests.Response:
    """
    Fetch a URL with simple retry/backoff logic for HTML pages.

    Retries on connection errors, timeouts, and 5xx HTTP responses,
    which are typically transient for the CPC website.
    """
    retriable_statuses = {429, 500, 502, 503, 504}

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            if resp.status_code in retriable_statuses:
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} from {url}",
                    response=resp
                )
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            is_last = (attempt == retries)
            print(
                f"[fetch_with_retry_html] Attempt {attempt}/{retries} failed for {url}: {e}",
                file=sys.stderr,
            )
            if is_last:
                print(
                    f"[fetch_with_retry_html] Giving up after {retries} attempts.",
                    file=sys.stderr,
                )
                raise
            sleep_for = backoff_seconds * attempt  # 20, 40, 60, ...
            print(
                f"[fetch_with_retry_html] Sleeping {sleep_for} seconds before retry...",
                file=sys.stderr,
            )
            time.sleep(sleep_for)


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


# Only treat these as region headers (stop markers)
REGION_LABELS_REGEX = r'(Tropical\s+Pacific|North\s+Pacific|North\s+Atlantic|Southern\s+Ocean)'


def grab_region_paragraph(soup: BeautifulSoup, label_regex: str) -> str:
    """
    Find a <strong> whose text matches label_regex and return the FULL paragraph text
    containing it (raw block that may include *multiple* regions).
    """
    strong = soup.find('strong', string=re.compile(label_regex, re.I))
    if not strong:
        return ""
    p = strong.find_parent('p')
    if p:
        return p.get_text(" ", strip=True)
    return strong.parent.get_text(" ", strip=True) if strong.parent else ""


def slice_region_block(raw_text: str, region_label: str) -> str:
    if not raw_text:
        return ""
    text = " ".join(raw_text.split())

    start = re.search(rf'\b{re.escape(region_label)}\b\s*[-–:]\s*', text, re.I)
    if not start:
        return ""

    sub = text[start.end():]

    # stop at ANY next region header (now includes North Atlantic / Southern Ocean)
    next_region = re.search(rf'\b{REGION_LABELS_REGEX}\b\s*[-–:]\s*', sub, re.I)
    if next_region:
        sub = sub[:next_region.start()]

    return sub.strip()


def get_latest_heatwave_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes heatwave date/period and extracts ONLY the North/Tropical Pacific region text."""
    try:
        resp = fetch_with_retry_html(session, url)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching heatwave URL after retries: {e}", file=sys.stderr)
        sys.exit(3)

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Date / Period (unchanged)
    d_el = soup.select_one('h5:-soup-contains("Forecast initial time") strong')
    p_el = soup.select_one('h5:-soup-contains("Forecast period") strong')
    heatwave_date = d_el.get_text(strip=True) if d_el else ""
    heatwave_period = p_el.get_text(strip=True) if p_el else ""

    # Raw text blocks containing each label
    raw_tp = grab_region_paragraph(soup, r'^Tropical\s+Pacific')
    raw_np = grab_region_paragraph(soup, r'^North\s+Pacific')

    # Slice only the desired regions (no sentence limits)
    tropical_pacific = slice_region_block(raw_tp, "Tropical Pacific") or slice_region_block(raw_np, "Tropical Pacific")
    north_pacific = slice_region_block(raw_np, "North Pacific") or slice_region_block(raw_tp, "North Pacific")

    # Combined (only these two)
    parts = []
    if north_pacific:
        parts.append(f"North Pacific - {north_pacific}")
    if tropical_pacific:
        parts.append(f"Tropical Pacific - {tropical_pacific}")
    heat_status = " | ".join(parts) if parts else "No regional summaries found."

    return {
        "heat_status": heat_status,
        "heat_date": heatwave_date,
        "heat_period": heatwave_period,
        "north_pacific": north_pacific,
        "tropical_pacific": tropical_pacific,
        "source_url": url
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

    # Get local data date + load full local JSON (fallback to empty)
    local_data_path = JSON_DIR / CONFIG['OUT_FILE_NAME']
    local_date_obj = datetime(1990, 1, 1)
    local_data = {}

    try:
        with open(local_data_path, 'r') as f:
            local_data = json.load(f)
            local_date_obj = safe_parse_date(local_data.get('heat_date'))  # use your safe parse if added
    except FileNotFoundError:
        print(f"Local file {local_data_path} not found. A new file will be created.")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error reading or parsing local JSON file: {e}", file=sys.stderr)

    # --- NEW: also compare content, not just date ---
    old_comp = json.dumps(local_data, sort_keys=True, ensure_ascii=False)
    new_comp = json.dumps(new_data,  sort_keys=True, ensure_ascii=False)
    contents_differ = (old_comp != new_comp)

    print(f"Local data date: {local_date_obj.strftime('%Y-%m-%d')}")
    print(f"Website data date: {new_date_obj.strftime('%Y-%m-%d')}")
    print(f"Content changed: {contents_differ}")

    # Only skip if same date AND same content AND not forcing overwrite
    if (not args.overwrite) and (local_date_obj.date() == new_date_obj.date()) and (not contents_differ):
        print("Heatwave info is up to date. No action needed.")
    else:
        print("New heatwave data available (date or content). Updating files...")

        with open(local_data_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved new data to {local_data_path}")

        dated_ofile = CONFIG['DATED_OUT_FILE_TEMPLATE'].format(new_date_obj.strftime('%Y%m'))
        dated_path = JSON_DIR / dated_ofile
        with open(dated_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved dated copy to {dated_path}")

    sys.exit(0)


if __name__ == "__main__":
    main()
