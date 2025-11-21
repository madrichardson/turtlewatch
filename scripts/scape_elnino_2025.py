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
import time
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
    'ROOT_DIR': Path(__file__).resolve().parents[1],
    'CPC_URL_BASE': 'https://www.cpc.ncep.noaa.gov/products/analysis_monitoring',
    'JSON_FILE_NAME': 'elnino_last.json',
    'JSON_FILE_ARCHIVE_TEMPLATE': '{}_elnino.json',
}


def send_to_erddap(local_file: Path, remote_path: Path) -> bool:
    """Sends a local file to a remote ERDDAP server via SCP.

    Args:
        local_file (Path): The path to the local file to send.
        remote_path (Path): The path on the remote server where the file should be saved.

    Returns:
        bool: True if the transfer was successful, False otherwise.
    """
    cmd = ['scp', str(local_file), f'{CONFIG["ERDDAP_USER_HOST"]}:{remote_path.as_posix()}']
    
    print(f"Executing: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(f"Successfully sent {local_file.name} to ERDDAP.")
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


def get_latest_enso_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes the latest ENSO status and synopsis from the CPC website.

    Args:
        session (requests.Session): The requests session object to use for the HTTP request.
        url (str): The URL of the specific ENSO advisory page.

    Returns:
        Dict[str, Any]: A dictionary containing the ENSO status, synopsis, and date.
    
    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
        AttributeError: If key HTML elements (date, status, synopsis) are not found.
    """
    print(f"Attempting to scrape URL: {url}")
    try:
        resp = fetch_with_retry_html(session, url)
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL after retries: {e}", file=sys.stderr)
        sys.exit(3)

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Scrape Date
    try:
        ft = soup.find_all('font')
        ft_clean = [f.contents[0].strip() for f in ft]
        idx = ft_clean.index('issued by')
        date_text = ft_clean[idx+2]
        date_obj = parse(date_text.strip().replace("\n", ""))
    except (AttributeError, TypeError):
        print("Could not find date on the website.", file=sys.stderr)
        sys.exit(3)

    # Scrape Synopsis
    try:
        synopsis_heading = soup.find('u', string=re.compile('Synopsis'))
        synopsis_text = synopsis_heading.find_next("strong").string
    except AttributeError:
        print("Could not parse synopsis info.", file=sys.stderr)
        synopsis_text = "Not available"

    # Scrape Status
    try:
        status_heading = soup.find('strong', string=re.compile('ENSO Alert'))
        status_text = status_heading.find_next('a').get_text().strip()
    except AttributeError:
        print("Could not parse status info.", file=sys.stderr)
        status_text = "Not available"

    return {
        "date_yrmo": date_obj.strftime('%Y%m'),
        "date_iso": date_obj.isoformat(),
        "date_print": date_obj.strftime('%d %B, %Y'),
        "synopsis": synopsis_text,
        "status": status_text,
    }


def main():
    """Controls and coordinates the scraping and updating of ENSO data."""
    # Define directories
    ROOT_DIR = CONFIG['ROOT_DIR']
    WORK_DIR = ROOT_DIR / 'work'
    JSON_DIR = ROOT_DIR / 'data' / 'json'
    
    # Create directories if they don't exist
    WORK_DIR.mkdir(exist_ok=True)
    JSON_DIR.mkdir(exist_ok=True)

    # Argument parsing
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '-d', '--date',
        help='Date for which to get the advisory (YYYY-MM).',
        type=str
    )
    args = parser.parse_args()

    # Determine URL based on args
    if args.date:
        try:
            custom_date = parse(args.date)
            url_part = f"enso_disc_{custom_date.strftime('%b%Y').lower()}"
        except ValueError:
            parser.error("Invalid date format. Use YYYY-MM.")
            
    else:
        url_part = "enso_advisory"
        
    url = f"{CONFIG['CPC_URL_BASE']}/{url_part}/ensodisc.shtml"
    
    # Get last scraped date from local file
    local_file_path = JSON_DIR / CONFIG['JSON_FILE_NAME']
    
    try:
        with open(local_file_path, 'r') as f:
            last_dict = json.load(f)
            last_date_obj = parse(last_dict['date_iso']).date()
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        last_date_obj = datetime.min.date()
        print("Local JSON file not found or corrupted. Proceeding with update.")

    # Scrape data from the website
    with requests.Session() as session:
        scraped_data = get_latest_enso_data(session, url)

    # Check for update if not a custom date
    if args.date is None:
        new_date_obj = parse(scraped_data['date_iso']).date()
        if new_date_obj <= last_date_obj:
            print(f"New date ({new_date_obj}) is not newer than stored date ({last_date_obj}). No update needed.")
            sys.exit(0)
    
    # Save a dated archive file
    dated_file_name = CONFIG['JSON_FILE_ARCHIVE_TEMPLATE'].format(scraped_data['date_yrmo'])
    dated_file_path = JSON_DIR / dated_file_name
    with open(dated_file_path, 'w') as f:
        json.dump(scraped_data, f, indent=4)
    print(f"Archived dated file: {dated_file_path}")

    # If not a custom date, update the main file and send to ERDDAP
    if args.date is None:
        with open(local_file_path, 'w') as f:
            json.dump(scraped_data, f, indent=4)
        print(f"Updated main file: {local_file_path}")

        ## send_to_erddap(local_file_path, CONFIG['ERDDAP_PATH'] / CONFIG['JSON_FILE_NAME'])

    # Send the dated file to ERDDAP
    ## send_to_erddap(dated_file_path, CONFIG['ERDDAP_PATH'] / dated_file_path.name)
    
    sys.exit(0)


if __name__ == "__main__":
    main()
