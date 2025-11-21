"""Updates the TOTAL data output for the website.

This script updates the TOTAL data results, which are used to update the TOTAL website.
Updated are the monthly SST and SST anomaly maps, the TOTAL indicator, the
TOTAL indicator plot, and the JSON file with SST and anomaly means.

This script triggers other scripts that do the actual updates.
  - update_total_indicator_2025.py
  - plot_total_tool_2025.py
  - make_monthly_maps_2025.py
"""

import os
import subprocess
from pathlib import Path
from datetime import datetime
from dateutil.parser import parse
import pandas as pd
import requests
import sys
import io
import json
import time
from typing import Iterable, Optional


def fetch_with_retry(
    session: requests.Session,
    url: str,
    retries: int = 5,
    timeout: int = 30,
    backoff_seconds: int = 10,
    retriable_statuses: Optional[Iterable[int]] = None,
) -> requests.Response:
    """Fetch a URL with simple retry/backoff logic.

    Parameters
    ----------
    session : requests.Session
        Active session object.
    url : str
        URL to fetch.
    retries : int, optional
        Number of attempts (including the first), by default 5.
    timeout : int, optional
        Per-request timeout in seconds, by default 30.
    backoff_seconds : int, optional
        Base backoff in seconds; actual sleep is backoff_seconds * attempt_index.
    retriable_statuses : Iterable[int], optional
        HTTP status codes that should be treated as transient and retried.

    Returns
    -------
    requests.Response
        Successful Response object.

    Raises
    ------
    requests.exceptions.RequestException
        If all retry attempts fail.
    """
    if retriable_statuses is None:
        # Include 403 here because you've seen it behave like a transient error.
        retriable_statuses = (429, 500, 502, 503, 504, 403)

    retriable_statuses = set(retriable_statuses)

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, timeout=timeout)
            # Explicitly retry if status is in our transient list
            if resp.status_code in retriable_statuses:
                raise requests.HTTPError(
                    f"HTTP {resp.status_code} from {url}",
                    response=resp
                )
            resp.raise_for_status()
            return resp

        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as e:
            is_last = attempt == retries
            print(
                f"[fetch_with_retry] Attempt {attempt}/{retries} failed for {url}: {e}",
                file=sys.stderr
            )

            if is_last:
                print(
                    f"[fetch_with_retry] Giving up after {retries} attempts.",
                    file=sys.stderr
                )
                raise

            sleep_for = backoff_seconds * attempt  # simple linear backoff
            print(
                f"[fetch_with_retry] Sleeping {sleep_for} seconds before retry...",
                file=sys.stderr
            )
            time.sleep(sleep_for)

# Define a function to get the latest available date from ERDDAP
def get_latest_erddap_date(session: requests.Session) -> datetime:
    """Fetches the most recent data date from the ERDDAP server.

    Args
    ----
    session : requests.Session
        The requests session object to use for the HTTP request.

    Returns
    -------
    datetime
        Datetime object of the most recent available data.

    Notes
    -----
    Exits with status 1 if all retry attempts fail or the CSV cannot be parsed.
    """
    url = (
        "https://coastwatch.pfeg.noaa.gov/erddap/griddap/"
        "jplMURSST41anommday.csv0?time[(last)]"
    )

    try:
        # Use the retry helper instead of a single .get()
        resp = fetch_with_retry(
            session,
            url,
            retries=5,
            timeout=30,
            backoff_seconds=10,
            retriable_statuses=(429, 500, 502, 503, 504, 403),
        )
        df = pd.read_csv(io.StringIO(resp.text))
        # The date is in the first column header
        return parse(df.columns[0])
    except (requests.exceptions.RequestException, pd.errors.ParserError) as e:
        print(f"Error fetching or parsing ERDDAP data after retries: {e}", file=sys.stderr)
        sys.exit(1)


# Define a function to parse a date from a filename
def parse_date_from_filename(filename: str) -> datetime:
    """Parses a YYYYMM date from a filename.

    Assumes a format like 'sst_202301_...'.

    Args:
        filename (str): The name of the file to parse.

    Returns:
        datetime: The parsed datetime object.
    """
    return parse(filename[4:12])

# Define a function to find the most recent file in a directory
def find_latest_file_date(directory: Path, pattern: str) -> datetime:
    """Finds the date of the most recent file matching a pattern.

    Args:
        directory (Path): The directory to search within.
        pattern (str): The filename pattern to match (e.g., 'sst_2').

    Returns:
        datetime: The datetime object of the most recently modified file,
                  or `datetime.min` if no matching files are found or an error occurs.
    """
    try:
        files = [f for f in directory.iterdir() if f.name.startswith(pattern)]
        if not files:
            return datetime.min
        latest_file = max(files, key=os.path.getmtime)
        return parse_date_from_filename(latest_file.name)
    except Exception as e:
        print(f"Error finding latest file in {directory}: {e}", file=sys.stderr)
        return datetime.min

def get_latest_indicator_date(csv_path: Path) -> datetime:
    """Return the most recent date from the loggerhead indicator CSV."""
    try:
        df = pd.read_csv(csv_path)
        if "dateyrmo" not in df.columns or df.empty:
            print("CSV has no dateyrmo column or is empty.", file=sys.stderr)
            return datetime.min
        # sort just in case rows are out of order
        df = df.sort_values(by=["dateyrmo"])
        latest_str = str(df["dateyrmo"].iloc[-1])  # e.g. "2025-08"
        return parse(latest_str).replace(tzinfo=None)
    except Exception as e:
        print(f"Error reading indicator CSV {csv_path}: {e}", file=sys.stderr)
        return datetime.min

# Define a function to run a script with subprocess
def run_script(python_path: Path, script_path: Path, args: list = []) -> bool:
    """Runs a Python script and returns its success status.

    Args:
        python_path (Path): The path to the Python interpreter.
        script_path (Path): The path to the script to execute.
        args (list, optional): A list of command-line arguments to pass to the script.
                               Defaults to an empty list.

    Returns:
        bool: True if the script ran successfully, False otherwise.
    """
    cmd = [str(python_path), str(script_path)] + args
    print(f"Executing: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Script failed with exit code {e.returncode}.", file=sys.stderr)
        print("Error details:", e.stderr, file=sys.stderr)
    except FileNotFoundError:
        print(f"Python interpreter or script not found: {cmd[0]}.", file=sys.stderr)
    return False

def main():
    """Controls and coordinates monthly updates to the TOTAL data output.

    This function fetches the latest data date from the ERDDAP server,
    compares it against the dates of local data files, and triggers
    subordinate scripts to update the necessary components if new data is available.
    """
    # Configuration
    CONFIG = {
        'ROOT_DIR': Path(__file__).resolve().parents[1],
        'ERDDAP_URL': 'https://coastwatch.pfeg.noaa.gov/erddap/griddap',
        'PYTHON_PATH': Path(sys.executable),
        'SCRIPTS': {
            'total_py': 'update_total_indicator_2025.py',
            'plot_py': 'plot_total_tool_2025.py',
            'maps_py': 'make_monthly_maps_2025.py'
        },
        'RESOURCE_FILE': 'loggerhead_indx.csv',
        'MAP_FILE_PREFIX': 'sst_2'
    }

    BIN_DIR = CONFIG['ROOT_DIR'] / 'scripts'
    RES_DIR = CONFIG['ROOT_DIR'] / 'data' / 'resources'
    MAP_DIR = CONFIG['ROOT_DIR'] / 'data' / 'images'

    with requests.Session() as session:
        latest_erddap_date = get_latest_erddap_date(session).replace(tzinfo=None)

    latest_total_date = get_latest_indicator_date(RES_DIR / CONFIG['RESOURCE_FILE']).replace(tzinfo=None)
    latest_map_date = find_latest_file_date(MAP_DIR, CONFIG['MAP_FILE_PREFIX']).replace(tzinfo=None)
    
    print(f"Most recent MUR data: {latest_erddap_date.strftime('%Y-%m')}")
    print(f"Most recent indicator: {latest_total_date.strftime('%Y-%m')}")
    print(f"Most recent maps: {latest_map_date.strftime('%Y-%m')}")

    # Check and update the total indicator
    if latest_total_date < latest_erddap_date:
        print("Updating TOTAL indicator...")
        if run_script(
            CONFIG['PYTHON_PATH'],
            BIN_DIR / CONFIG['SCRIPTS']['total_py']
        ):
            print("TOTAL indicator updated successfully. Running plot script.")
            run_script(
                CONFIG['PYTHON_PATH'],
                BIN_DIR / CONFIG['SCRIPTS']['plot_py']
            )
    else:
        print("TOTAL indicator is up to date.")

    # Check and update the maps
    if latest_map_date < latest_erddap_date:
        print("Updating maps...")
        run_script(
            CONFIG['PYTHON_PATH'],
            BIN_DIR / CONFIG['SCRIPTS']['maps_py'],
            args=['-d', latest_erddap_date.strftime('%Y-%m'), '-n', '-t', '-j']
        )
    else:
        print("Maps are up to date.")

    
    # --- Create TOTAL JSON (web_data.json) ---
    print("Creating web_data.json summary...")
    try:
        from datetime import datetime
        df = pd.read_csv(RES_DIR / CONFIG['RESOURCE_FILE'])
        latest_index = float(df["indicator"].iloc[-1])
        alert_status = "Alert" if latest_index >= 0.77 else "No Alert"
        forecast_date = datetime.now().strftime("%B %Y")
        update_date = datetime.now().strftime("%d %b, %Y")
        web_data = {
            "alert": alert_status,
            "fc_date": forecast_date,
            "update_date": update_date,
            "new_index": f"{latest_index:.2f}"
        }

        json_dir = CONFIG['ROOT_DIR'] / "data" / "json"
        json_dir.mkdir(parents=True, exist_ok=True)
        json_path = json_dir / "web_data.json"
        with open(json_path, "w") as f:
            json.dump(web_data, f, indent=4)

        print(f"web_data.json created at {json_path}")
        print(json.dumps(web_data, indent=4))
    except Exception as e:
        print(f"Failed to create web_data.json: {e}")


if __name__ == "__main__":
    main()
