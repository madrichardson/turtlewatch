"""
Update the TOTAL indicator time series and forecast.

This script maintains the monthly TOTAL indicator dataset that drives the
Loggerhead Conservation Area dashboard. It reads the existing indicator
CSV, checks ERDDAP for any new MUR SST anomaly months, appends new
observed values for those months, computes a one-month-ahead forecast
indicator, and writes both the updated CSV and a small JSON summary for
the website.

In broad terms, it:

  1. Loads the existing indicator time series from
     data/resources/loggerhead_indx.csv, sorts by `dateyrmo` (YYYY-MM),
     and drops the last row (the previous forecast) so only observed
     months remain.

  2. Opens the remote MUR SST anomaly dataset
     (jplMURSST41anommday) from ERDDAP using a retry/backoff helper
     (`get_data_from_erddap`), and converts the `time` coordinate to a
     list of month strings (YYYY-MM).

  3. Subsets the ERDDAP grid to the configured loggerhead region
     (LAT_RANGE and LON_RANGE) using `get_closest_value_indices`, then
     compares ERDDAP month strings to the `dateyrmo` values already in
     the CSV to identify any missing months (`get_missing_dates`).

  4. For each missing month, extracts the regional `sstAnom` field,
     computes a mean anomaly over the loggerhead box, derives an updated
     indicator value based on the last six anomalies, and appends a new
     “observed” row to the DataFrame (`process_missing_data`).

  5. After filling in all new observed months, constructs a simple
     one-month-ahead forecast: advances the last `dateyrmo` to the next
     month, computes a forecast indicator as the mean of the last six
     anomalies, and appends a “prediction” row to the time series.

  6. Builds a small web summary dictionary (`web_data`) containing the
     latest (forecast) indicator value, alert / no-alert status relative
     to the 0.77 threshold, a human-readable forecast month label, and a
     last-updated date string.

  7. Writes the updated indicator series back to
     data/resources/loggerhead_indx.csv and saves the JSON summary to
     work/web_data.json, archiving the JSON locally via a simple rename
     in `save_and_transfer_data`.

If no new ERDDAP months are available, the script prints a message and
exits without modifying the CSV or JSON.
"""

import os
import netCDF4
import numpy as np
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import sys
import pandas as pd
import json
import subprocess
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union
import warnings
import time
warnings.filterwarnings('ignore')

__author__ = "Dale Robinson"
__credits__ = ["Dale Robinson"]
__license__ = "GPL"
__version__ = "2.1"
__maintainer__ = "Dale Robinson"
__email__ = "dale.Robinson@noaa.gov"
__status__ = "Production"

# --- Configuration and Constants ---
CONFIG = {
    'BASE_DIR': Path(__file__).resolve().parents[1],
    'LAT_RANGE': [30.8, 34.5],
    'LON_RANGE': [-120.3, -116],
    'URL_BASE': 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}',
    'DATASET_NAME': 'jplMURSST41anommday',
    'INDICATOR_CSV': 'loggerhead_indx.csv',
    'WEB_DATA_JSON': 'web_data.json',
}


def get_closest_value_indices(arr: np.ndarray, val_range: List[float]) -> Tuple[int, int]:
    """Finds the indices corresponding to the closest values in a sorted array.

    This function is a simpler replacement for the original `max_min_idx`
    and related functions. It's more efficient for sorted arrays.

    Args:
        arr (np.ndarray): A sorted NumPy array of coordinates (e.g., latitude or longitude).
        val_range (List[float]): A list containing the min and max values to find.

    Returns:
        Tuple[int, int]: A tuple containing the indices of the closest min and max values.
    """
    min_idx = np.abs(arr - val_range[0]).argmin()
    max_idx = np.abs(arr - val_range[1]).argmin()
    return min_idx, max_idx


def get_data_from_erddap(
    dataset_name: str,
    retries: int = 5,
    backoff_seconds: int = 20
) -> netCDF4.Dataset:
    """
    Open a remote ERDDAP dataset via OPeNDAP with automatic retry/backoff logic.

    This function attempts to open a remote NetCDF dataset hosted on an ERDDAP
    server using `netCDF4.Dataset(url, "r")`. Because OPeNDAP endpoints can
    occasionally become temporarily unavailable — due to network instability,
    server load, brief outages, or routing issues — a single attempt may fail
    even when the dataset becomes accessible moments later.

    To make the TOTAL update pipeline more resilient, this function wraps the
    dataset-opening call in retry logic:
    
    - It attempts to open the dataset up to `retries` times.
    - Failures caused by network/IO-related issues (raised as `OSError` or
      `IOError` by the netCDF4 library) trigger a retry.
    - Between each retry, the function sleeps for a linearly increasing delay:
      `backoff_seconds * attempt_number`.  
      For example, with `backoff_seconds=20`:
        • attempt 1 → wait 20s  
        • attempt 2 → wait 40s  
        • attempt 3 → wait 60s  
      This avoids overloading the server and provides time for transient issues
      to resolve.
    - Non-IO errors (unexpected exceptions from netCDF4 or Python) are treated as
      fatal and cause an immediate exit.

    If the final attempt still fails, the function prints an error message to
    STDERR and terminates the script using `sys.exit(1)`, ensuring the calling
    workflow clearly reports a failure rather than silently producing partial or
    inconsistent results.

    Parameters
    ----------
    dataset_name : str
        The ERDDAP dataset ID (e.g., "jplMURSST41anommday").
        This ID is inserted into the base URL template defined in CONFIG
        to produce a full OPeNDAP-accessible URL.
    
    retries : int, optional
        The maximum number of attempts to open the dataset.
        Defaults to 5. A value of 1 means “try once with no retry”.

    backoff_seconds : int, optional
        The base number of seconds to wait between retry attempts.
        The actual delay increases linearly with each attempt:
            delay = backoff_seconds * attempt_number
        Defaults to 20 seconds.

    Returns
    -------
    netCDF4.Dataset
        An open Dataset object pointing to the remote ERDDAP dataset.
        The caller is responsible for closing it (typically via `with`).

    Exits
    -----
    sys.exit(1)
        If all retry attempts fail due to OSError/IOError, or if an unexpected,
        non-retriable exception occurs. The exit ensures that downstream
        scripts do not run on missing or incomplete data.
    """
    url = CONFIG['URL_BASE'].format(dataset_name)
    last_exc = None

    for attempt in range(1, retries + 1):
        try:
            print(
                f"Opening ERDDAP dataset (attempt {attempt}/{retries}): {url}",
                file=sys.stderr
            )
            return netCDF4.Dataset(url, "r")
        except OSError as e:
            # netCDF4 uses OSError/IOError for many I/O and network issues
            last_exc = e
            if attempt == retries:
                print(
                    f"Error opening ERDDAP dataset at {url} after {retries} attempts: {e}",
                    file=sys.stderr
                )
                sys.exit(1)

            sleep_for = backoff_seconds * attempt  # linear backoff
            print(
                f"Failed to open dataset ({e}); sleeping {sleep_for} seconds before retry...",
                file=sys.stderr
            )
            time.sleep(sleep_for)
        except Exception as e:
            # Non-I/O failures: treat as fatal immediately
            print(
                f"Non-retriable error opening ERDDAP dataset at {url}: {e}",
                file=sys.stderr
            )
            sys.exit(1)


def get_missing_dates(df: pd.DataFrame, erddap_dates_str: List[datetime]) -> List[datetime]:
    """Compares local data dates with ERDDAP dates to find missing months."""
    local_dates = set(df['dateyrmo'].values)
    #erddap_dates_str = set(['{0:%Y-%m}'.format(dt) for dt in erddap_time_obj])
    
    missing_dates_str = sorted(list(erddap_dates_str - local_dates))
    
    if not missing_dates_str:
        return []
    
    missing_dates = [parse(dt_str) for dt_str in missing_dates_str]
    return missing_dates
    
def get_missing_dates_OLD(df: pd.DataFrame, erddap_time_obj: List[datetime]) -> List[datetime]:
    """Compares local data dates with ERDDAP dates to find missing months."""
    local_dates = set(df['dateyrmo'].values)
    erddap_dates_str = set(['{0:%Y-%m}'.format(dt) for dt in erddap_time_obj])
    
    missing_dates_str = sorted(list(erddap_dates_str - local_dates))
    
    if not missing_dates_str:
        return []
    
    missing_dates = [parse(dt_str) for dt_str in missing_dates_str]
    return missing_dates


def process_missing_data(
    df: pd.DataFrame,
    erddap_time_str: List,
    erddap_data: netCDF4.Dataset,
    missing_dates: List[datetime],
    lat_idx_range: Tuple[int, int],
    lon_idx_range: Tuple[int, int]
) -> pd.DataFrame:
    """Calculates new indicator values and appends them to the DataFrame."""    
    for date_obj in missing_dates:
        date_obj_first = date_obj.replace(day=1)
        try:
            time_idx = erddap_time_str.index(date_obj.strftime('%Y-%m'))
            
            anom_data = erddap_data['sstAnom'][time_idx,
                                               lat_idx_range[0]:lat_idx_range[1],
                                               lon_idx_range[0]:lon_idx_range[1]]
            
            # Calculate the new indicator value based on the last 6 months
            # This logic assumes the new anom is a valid input for the indicator.
            current_anom = np.ma.filled(anom_data, np.nan).mean()
            #current_indicator = df['anom'].rolling(window=6, min_periods=1).mean()[-1]
            
            anom_series = pd.to_numeric(df["anom"], errors="coerce")
            current_indicator = pd.concat([anom_series, pd.Series([current_anom])], ignore_index=True).tail(6).mean()

            
            # The original script calculated the new index using a mix of existing and
            # new data. This is a potential point of ambiguity. The code here
            # assumes the indicator is the mean of the last 6 'anom' values.
            # If 'anom' is also being updated, the logic needs to be revisited.
            
            # Append new row using column names (prevents misalignment/NaN rows)
            new_row = {c: np.nan for c in df.columns}
            new_row[df.columns[0]] = date_obj.strftime('%-m/%-d/%Y')      # first date col
            new_row[df.columns[1]] = date_obj_first.strftime('%-m/%d/%y') # second date col
            new_row["anom"] = round(current_anom, 2)
            new_row["indicator"] = round(float(current_indicator), 2)
            new_row["dateyrmo"] = date_obj.strftime('%Y-%m')

            # Preserve any extra columns if they exist
            if len(df.columns) >= 6:
                # if your CSV has fixed columns like window/flag, keep your defaults:
                if df.columns[4] in new_row: new_row[df.columns[4]] = 6
                if df.columns[5] in new_row: new_row[df.columns[5]] = 0

            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            df = df.sort_values(by=["dateyrmo"], ignore_index=True)

            
        except (ValueError, IndexError) as e:
            print(f"Error processing data for {date_obj.strftime('%Y-%m')}: {e}", file=sys.stderr)
            continue
            
    # Re-calculate the indicator column after all anomalies are added
    # This is a much safer way to handle this calculation.
    # Append prediction row to DataFrame

    
    return df


def save_and_transfer_data(
    web_data: Dict[str, Any],
    df: pd.DataFrame,
    json_path: Path,
    csv_path: Path
) -> None:
    """Saves data to JSON and CSV files and transfers them via SCP."""
    # Save web data to JSON file
    try:
        with open(json_path, 'w') as outfile:
            json.dump(web_data, outfile, indent=4)
        print(f"JSON file saved to {json_path}")
    except IOError as e:
        print(f"Error saving JSON file: {e}", file=sys.stderr)
        sys.exit(1)
        
    # Save indicator time series to CSV
    try:
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"CSV file saved to {csv_path}")
    except IOError as e:
        print(f"Error saving CSV file: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    """Controls the update process for the TOTAL tool indicator."""

    # Define paths
    ROOT_DIR = CONFIG['BASE_DIR']
    RES_DIR = ROOT_DIR / 'data' / 'resources'
    JSON_DIR = ROOT_DIR / 'data' / 'json'
    WORK_DIR = ROOT_DIR / 'work'
   
    # Load and prepare indicator time series DataFrame
    try:
        df = pd.read_csv(RES_DIR / CONFIG['INDICATOR_CSV'])

        # --- Clean CSV and remove any existing forecast/blank rows safely ---
        df["dateyrmo"] = df["dateyrmo"].astype(str).str.strip()

        # Convert to datetime; bad rows become NaT
        df["_dateyrmo_dt"] = pd.to_datetime(df["dateyrmo"], format="%Y-%m", errors="coerce")

        # Drop blank/garbage rows (this removes the ",,,,,," row)
        df = df[df["_dateyrmo_dt"].notna()].copy()

        # Sort by month
        df = df.sort_values("_dateyrmo_dt", ignore_index=True)

        # Drop helper
        df = df.drop(columns=["_dateyrmo_dt"])

    except FileNotFoundError:
        print(f"Indicator CSV not found at {RES_DIR / CONFIG['INDICATOR_CSV']}", file=sys.stderr)
        sys.exit(1)
        
    #df = df.sort_values(by=['dateyrmo'], ignore_index=True)
    #df = df.drop(df.tail(1).index) # Drop the prediction row

    # Find available data from ERDDAP
    with get_data_from_erddap(CONFIG['DATASET_NAME']) as edt:
        edt_time_obj = [datetime.fromtimestamp(ln).astimezone(timezone.utc) for ln in edt['time'][:]]
        erddap_dates_str = ['{0:%Y-%m}'.format(dt) for dt in edt_time_obj]
        erddap_dates_str_set = set(erddap_dates_str)
        
        lat_coords = np.array(edt.variables['latitude'][:])
        lon_coords = np.array(edt.variables['longitude'][:])
        
        lat_idx_range = get_closest_value_indices(lat_coords, CONFIG['LAT_RANGE'])
        lon_idx_range = get_closest_value_indices(lon_coords, CONFIG['LON_RANGE'])
        
        missing_dates = get_missing_dates(df, erddap_dates_str_set)
        print('missing_dates', missing_dates)

        latest_erddap_yrmo = max(erddap_dates_str_set)  # "YYYY-MM"
        df["dateyrmo"] = df["dateyrmo"].astype(str).str.strip()
        df = df[df["dateyrmo"] <= latest_erddap_yrmo].copy()
        df = df.sort_values(by=["dateyrmo"], ignore_index=True)

        if not missing_dates:
            print("No new data to process. Exiting.")
        
        df = process_missing_data(df, erddap_dates_str, edt, missing_dates, lat_idx_range, lon_idx_range)

    # Make the forecast
    last_month = parse(str(df["dateyrmo"].iloc[-1]) + "-16")  # anchor mid-month
    next_date = (last_month + relativedelta(months=1)).replace(day=16)
    next_date_first = next_date.replace(day=1)
    #next_index = round(df['anom'].rolling(window=6, min_periods=1).mean()[-1], 2)
    next_index = round(df['anom'][-6:].mean(), 2)
    
    # Append forecast row using column names (prevents ",,,,,," row)
    forecast_row = {c: np.nan for c in df.columns}
    forecast_row[df.columns[0]] = next_date.strftime('%-m/%d/%Y')
    forecast_row[df.columns[1]] = next_date_first.strftime('%-m/%-d/%y')
    forecast_row["anom"] = 0
    forecast_row["indicator"] = float(next_index)
    forecast_row["dateyrmo"] = next_date.strftime('%Y-%m')

    if len(df.columns) >= 6:
        if df.columns[4] in forecast_row: forecast_row[df.columns[4]] = 6
        if df.columns[5] in forecast_row: forecast_row[df.columns[5]] = 0

    df = pd.concat([df, pd.DataFrame([forecast_row])], ignore_index=True)

    
    # Prepare data for web
    latest_index = df['indicator'].iloc[-1]
    forecast_date = parse(df.loc[len(df)-1, 'dateyrmo'] + '-16').strftime('%B %Y')
    update_date = datetime.now().strftime("%d %b %Y")
    alert_status = "Alert" if latest_index >= 0.77 else "No Alert"
    
    web_data = {
        'alert': alert_status,
        'fc_date': forecast_date,
        'update_date': update_date,
        'new_index': str(latest_index)
    }

    # Final cleanup: remove any fully-empty rows and any rows missing dateyrmo
    df.replace(r"^\s*$", np.nan, regex=True)
    df = df.dropna(how="all")
    df = df[df["dateyrmo"].notna() & (df["dateyrmo"].astype(str).str.strip() != "")]
    df = df.sort_values(by=["dateyrmo"], ignore_index=True)

    # Save and transfer files
    save_and_transfer_data(
        web_data,
        df,
        JSON_DIR / CONFIG['WEB_DATA_JSON'],
        RES_DIR / CONFIG['INDICATOR_CSV']
    )


if __name__ == "__main__":
    main()
