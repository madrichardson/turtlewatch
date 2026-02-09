#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update the TOTAL tool indicator."""

import os
import netCDF4
import numpy as np
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import sys
import pandas as pd
import json
import subprocess
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union
import warnings
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


def get_data_from_erddap(dataset_name: str) -> netCDF4.Dataset:
    """Opens and returns a NetCDF dataset from ERDDAP."""
    url = CONFIG['URL_BASE'].format(dataset_name)
    try:
        return netCDF4.Dataset(url, 'r')
    except Exception as e:
        print(f"Error opening ERDDAP dataset at {url}: {e}", file=sys.stderr)
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
            
            tmp = np.ma.filled(anom_data, np.nan)
            print("any NaN?", np.isnan(tmp).any())
            print("mean:", tmp.mean())
            print("nanmean:", np.nanmean(tmp))
            
            # Calculate the new indicator value based on the last 6 months
            # This logic assumes the new anom is a valid input for the indicator.
            current_anom = np.nanmean(np.ma.filled(anom_data, np.nan))
            #current_indicator = df['anom'].rolling(window=6, min_periods=1).mean()[-1]
            current_indicator = df['anom'][-6:].mean()
            
            # The original script calculated the new index using a mix of existing and
            # new data. This is a potential point of ambiguity. The code here
            # assumes the indicator is the mean of the last 6 'anom' values.
            # If 'anom' is also being updated, the logic needs to be revisited.
            
            # Append new row to DataFrame
            df.loc[len(df.index)] =  [
                date_obj.strftime('%-m/%-d/%Y'),
                date_obj_first.strftime('%-m/%d/%y'),
                round(current_anom, 2),
                round(current_indicator, 2), # This needs to be calculated after all 'anom' values are in place
                6, # These columns need to be better named if they represent something
                0,
                date_obj.strftime('%Y-%m')
            ]
            df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
            
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
    csv_path: Path,
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
        df["anom"] = pd.to_numeric(df["anom"], errors="coerce").round(2)
        df["indicator"] = pd.to_numeric(df["indicator"], errors="coerce").round(2)

        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"CSV file saved to {csv_path}")
    except IOError as e:
        print(f"Error saving CSV file: {e}", file=sys.stderr)
        sys.exit(1)

    # Transfer files to Coastwatch server
    #try:
        #subprocess.run(
            #['scp', json_path.as_posix(), remote_path],
            #check=True, capture_output=True, text=True
        #)
        #print(f"Successfully transferred {json_path.name}")
        
        # subprocess.run(
        #    ['mv', json_path.as_posix(), os.path.join(json_path.parent, f"{json_path.stem}_archived.json")],
        #    check=True, capture_output=True, text=True
        #)
        #print("JSON file archived locally.")
        
    #except subprocess.CalledProcessError as e:
    #    print(f"SCP command failed with exit code {e.returncode}.", file=sys.stderr)
    #    print("Error details:", e.stderr, file=sys.stderr)
    #except FileNotFoundError:
    #    print("SCP command not found. Is it installed and in your PATH?", file=sys.stderr)


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
    except FileNotFoundError:
        print(f"Indicator CSV not found at {RES_DIR / CONFIG['INDICATOR_CSV']}", file=sys.stderr)
        sys.exit(1)
        
    df = df.sort_values(by=['dateyrmo'], ignore_index=True)
    df = df.drop(df.tail(1).index) # Drop the prediction row

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

        if not missing_dates:
            print("No new data to process. Exiting.")
            sys.exit(0)
        
        df = process_missing_data(df, erddap_dates_str, edt, missing_dates, lat_idx_range, lon_idx_range)

    # Make the forecast
    next_date = (parse(df.loc[len(df)-1, 'dateyrmo']) + timedelta(days=30)).replace(day=16)
    next_date_first = next_date.replace(day=1)
    #next_index = round(df['anom'].rolling(window=6, min_periods=1).mean()[-1], 2)
    next_index = round(df['anom'][-6:].mean(), 2)
    
    # Add prediction row to the DataFrame
    prediction_row = [
        next_date.strftime('%-m/%d/%Y'),
        next_date_first.strftime('%-m/%-d/%y'),
        0,
        next_index,
        6,
        0,
        next_date.strftime('%Y-%m')
    ]
    df.loc[len(df.index)] = prediction_row
    
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

    # Save and transfer files
    save_and_transfer_data(
        web_data,
        df,
        WORK_DIR / CONFIG['WEB_DATA_JSON'],
        RES_DIR / CONFIG['INDICATOR_CSV']
    )


if __name__ == "__main__":
    main()
