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


def get_latest_erddap_date(session: requests.Session) -> datetime:
    """Fetches the most recent data date from the ERDDAP server."""
    try:
        url_anom = session.get(
            "https://coastwatch.pfeg.noaa.gov/erddap/griddap/jplMURSST41anommday.csv0?time[(last)]"
        )
        url_anom.raise_for_status()
        df = pd.read_csv(io.StringIO(url_anom.text))
        return parse(df.columns[0])
    except (requests.exceptions.RequestException, pd.errors.ParserError) as e:
        print(f"Error fetching or parsing ERDDAP data: {e}", file=sys.stderr)
        sys.exit(1)


def parse_date_from_filename(filename: str) -> datetime:
    """Extract YYYYMM from a filename like sst_202301_..."""
    return parse(filename[4:12])


def find_latest_file_date(directory: Path, pattern: str) -> datetime:
    """Find the most recent file date in a directory.

    - If matching file is a CSV (e.g., loggerhead_indx.csv), use file modification time.
    - Otherwise, assume the filename contains a YYYYMM starting at char 4
      (e.g., sst_20250116.png → 2025-01).
    """
    try:
        files = [f for f in directory.iterdir() if f.name.startswith(pattern)]
        if not files:
            return datetime.min

        latest_file = max(files, key=os.path.getmtime)

        # Special case: CSV indicator file
        if latest_file.suffix == ".csv":
            return datetime.fromtimestamp(os.path.getmtime(latest_file))

        # Default: parse from filename (sst_YYYYMM…)
        return parse_date_from_filename(latest_file.name)

    except Exception as e:
        print(f"Error finding latest file in {directory}: {e}", file=sys.stderr)
        return datetime.min


def run_script(script_path: Path, args: list = []) -> bool:
    """Run a Python script via subprocess."""
    cmd = [sys.executable, str(script_path)] + args
    print(f"Executing: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Script failed with exit code {e.returncode}", file=sys.stderr)
        print("Error details:", e.stderr, file=sys.stderr)
    except FileNotFoundError:
        print(f"Script not found: {cmd[1]}", file=sys.stderr)
    return False


def main():
    """Control monthly updates to TOTAL data output."""
    CONFIG = {
        "ROOT_DIR": Path(__file__).resolve().parents[1],
        "SCRIPTS": {
            "total_py": "update_total_indicator_2025.py",
            "plot_py": "plot_total_tool_2025.py",
            "maps_py": "make_monthly_maps_2025.py",
        },
        "RESOURCE_FILE": "loggerhead_indx.csv",
        "MAP_FILE_PREFIX": "sst_2",
    }

    ROOT_DIR = CONFIG["ROOT_DIR"]
    BIN_DIR = ROOT_DIR / "scripts"
    RES_DIR = ROOT_DIR / "data" / "resources"
    MAP_DIR = ROOT_DIR / "data" / "images"

    with requests.Session() as session:
        latest_erddap_date = get_latest_erddap_date(session)

    latest_total_date = find_latest_file_date(RES_DIR, CONFIG["RESOURCE_FILE"])
    latest_map_date = find_latest_file_date(MAP_DIR, CONFIG["MAP_FILE_PREFIX"])

    # --- Normalize timezone differences ---
    if latest_erddap_date.tzinfo is not None:
        latest_erddap_date = latest_erddap_date.replace(tzinfo=None)
    if latest_total_date != datetime.min and latest_total_date.tzinfo is not None:
        latest_total_date = latest_total_date.replace(tzinfo=None)
    if latest_map_date != datetime.min and latest_map_date.tzinfo is not None:
        latest_map_date = latest_map_date.replace(tzinfo=None)

    print(f"Most recent MUR data: {latest_erddap_date.strftime('%Y-%m')}")
    print(f"Most recent indicator: {latest_total_date.strftime('%Y-%m')}")
    print(f"Most recent maps: {latest_map_date.strftime('%Y-%m')}")

    # Indicator update
    if latest_total_date < latest_erddap_date:
        print("Updating TOTAL indicator...")
        if run_script(BIN_DIR / CONFIG["SCRIPTS"]["total_py"]):
            print("Indicator updated. Running plot script...")
            run_script(BIN_DIR / CONFIG["SCRIPTS"]["plot_py"])
    else:
        print("Indicator is up to date.")

    # Maps update
    if latest_map_date < latest_erddap_date:
        print("Updating maps...")
        run_script(
            BIN_DIR / CONFIG["SCRIPTS"]["maps_py"],
            args=["-d", latest_erddap_date.strftime("%Y-%m"), "-n", "-t", "-j"],
        )
    else:
        print("Maps are up to date.")


if __name__ == "__main__":
    main()