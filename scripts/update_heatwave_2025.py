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


def get_latest_heatwave_data(session: requests.Session, url: str) -> Dict[str, Any]:
    """Scrapes heatwave narrative and dates from the website.

    Args:
        session (requests.Session): The requests session object to use for the HTTP request.
        url (str): The URL of the website to scrape.

    Returns:
        Dict[str, Any]: A dictionary containing the heatwave status, date, and forecast period.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
        AttributeError: If key HTML elements are not found on the page.
    """
    html = session.get(url)
    html.raise_for_status()
    soup = BeautifulSoup(html.text, 'html.parser')
    
    # Use more specific selectors or compile patterns
    pattern = re.compile(r'North Pacific|Pacific')
    
    try:
        heatwave_text = soup.find('strong', string=pattern).parent.text.strip()
        
        # Using more robust CSS selectors to find the date and period
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


def main():
    """Controls and coordinates updates to the TOTAL heatwave status."""
    
    # Configuration
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

    # Argument parsing
    parser = argparse.ArgumentParser(description='Update TOTAL heatwave status. Use -o to force overwrite.')
    parser.add_argument('-o', '--overwrite', action='store_true', help='Force update and overwrite existing data.')
    args = parser.parse_args()

    # Get new data from website
    print(f"Scraping heatwave data from {CONFIG['SCRAPE_URL']}")
    with requests.Session() as session:
        new_data = get_latest_heatwave_data(session, CONFIG['SCRAPE_URL'])
    
    new_date_obj = parse(new_data['heat_date'])

    # Get local data date
    local_data_path = JSON_DIR / CONFIG['OUT_FILE_NAME']
    local_date_obj = datetime(1990, 1, 1)

    try:
        with open(local_data_path, 'r') as f:
            local_data = json.load(f)
            local_date_obj = parse(local_data.get('heat_date', '1990-01-01'))
    except FileNotFoundError:
        print(f"Local file {local_data_path} not found. A new file will be created.")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error reading or parsing local JSON file: {e}", file=sys.stderr)

    # Compare and update
    print(f"Local data date: {local_date_obj.strftime('%Y-%m-%d')}")
    print(f"Website data date: {new_date_obj.strftime('%Y-%m-%d')}")

    if local_date_obj.date() == new_date_obj.date() and not args.overwrite:
        print("Heatwave info is up to date. No action needed.")
    else:
        print("New heatwave data available. Updating files...")
        
        # Save the new data to the main JSON file
        with open(local_data_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved new data to {local_data_path}")

        # Send to ERDDAP
        remote_path = Path(CONFIG['ERDDAP_PATH'])
        ## send_to_erddap(JSON_DIR, local_data_path.name, remote_path.as_posix(), local_data_path.name)

        # Save a dated copy
        dated_ofile = CONFIG['DATED_OUT_FILE_TEMPLATE'].format(new_date_obj.strftime('%Y%m'))
        dated_path = JSON_DIR / dated_ofile
        with open(dated_path, "w") as outfile:
            json.dump(new_data, outfile, indent=4)
        print(f"Saved dated copy to {dated_path}")

    sys.exit(0)


if __name__ == "__main__":
    main()