#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update the TOTAL tool indicator (GitHub Actions version)."""

import netCDF4
import numpy as np
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import sys
import pandas as pd
import json
from pathlib import Path
from typing import List, Tuple, Dict, Any
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
    # Base dir = project root (repo root)
    'BASE_DIR': Path(__file__).resolve().parents[1],

    # Local subdirs (no remote server transfers)
    'LAT_RANGE': [30.8, 34.5],
    'LON_RANGE': [-120.3, -116],
    'URL_BASE': 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}',
    'DATASET_NAME': 'jplMURSST41anommday',

    # Local file names
    'INDICATOR_CSV': 'loggerhead_indx.csv',
    'WEB_DATA_JSON': 'web_data.json',
}


def get_closest_value_indices(arr: np.ndarray, val_range: List[float]) -> Tuple[int, int]:
    """Find indices of closest values in a sorted array."""
    min_idx = np.abs(arr - val_range[0]).argmin()
    max_idx = np.abs(arr - val_range[1]).argmin()
    return min_idx, max_idx


def get_data_from_erddap(dataset_name: str) -> netCDF4.Dataset:
    """Opens and returns a NetCDF dataset from ERDDAP."""
    url = CONFIG['URL_BASE'].format(dataset_name)
    return netCDF4.Dataset(url, 'r')


def get_missing_dates(df: pd.DataFrame, erddap_dates_str: List[str]) -> List[datetime]:
    """Find missing dates by comparing local CSV with ERDDAP."""
    local_dates = set(df['dateyrmo'].values)
    missing_dates_str = sorted(list(erddap_dates_str - local_dates))
    return [parse(dt_str) for dt_str in missing_dates_str]


def process_missing_data(df, erddap_time_str, erddap_data, missing_dates, lat_idx_range, lon_idx_range):
    """Append new anomaly and indicator rows to dataframe."""
    for date_obj in missing_dates:
        date_obj_first = date_obj.replace(day=1)
        try:
            time_idx = erddap_time_str.index(date_obj.strftime('%Y-%m'))
            anom_data = erddap_data['sstAnom'][time_idx,
                                               lat_idx_range[0]:lat_idx_range[1],
                                               lon_idx_range[0]:lon_idx_range[1]]
            current_anom = np.ma.filled(anom_data, np.nan).mean()
            current_indicator = df['anom'][-6:].mean()
            df.loc[len(df.index)] = [
                date_obj.strftime('%-m/%-d/%Y'),
                date_obj_first.strftime('%-m/%d/%y'),
                round(current_anom, 2),
                round(current_indicator, 2),
                6,
                0,
                date_obj.strftime('%Y-%m')
            ]
            df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
        except Exception as e:
            print(f"Error processing data for {date_obj.strftime('%Y-%m')}: {e}", file=sys.stderr)
            continue
    return df


def save_outputs(web_data: Dict[str, Any], df: pd.DataFrame, json_path: Path, csv_path: Path):
    """Save outputs locally (tracked in repo)."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(json_path, 'w') as outfile:
        json.dump(web_data, outfile, indent=4)
    print(f"JSON file saved: {json_path}")

    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"CSV file saved: {csv_path}")


def main():
    ROOT_DIR = CONFIG['BASE_DIR']
    RES_DIR = ROOT_DIR / 'data' / 'resources'
    JSON_DIR = ROOT_DIR / 'data' / 'json'

    # Load indicator CSV
    csv_path = RES_DIR / CONFIG['INDICATOR_CSV']
    df = pd.read_csv(csv_path).sort_values(by=['dateyrmo'], ignore_index=True)
    df = df.drop(df.tail(1).index)  # drop prediction row

    # Pull from ERDDAP
    with get_data_from_erddap(CONFIG['DATASET_NAME']) as edt:
        edt_time_obj = [datetime.fromtimestamp(ln).astimezone(timezone.utc) for ln in edt['time'][:]]
        erddap_dates_str = set(['{0:%Y-%m}'.format(dt) for dt in edt_time_obj])
        lat_coords = np.array(edt.variables['latitude'][:])
        lon_coords = np.array(edt.variables['longitude'][:])
        lat_idx_range = get_closest_value_indices(lat_coords, CONFIG['LAT_RANGE'])
        lon_idx_range = get_closest_value_indices(lon_coords, CONFIG['LON_RANGE'])
        missing_dates = get_missing_dates(df, erddap_dates_str)

        if not missing_dates:
            print("No new data to process. Exiting.")
            sys.exit(0)

        df = process_missing_data(df, list(erddap_dates_str), edt, missing_dates, lat_idx_range, lon_idx_range)

    # Forecast
    next_date = (parse(df.loc[len(df)-1, 'dateyrmo']) + timedelta(days=30)).replace(day=16)
    next_index = round(df['anom'][-6:].mean(), 2)
    prediction_row = [
        next_date.strftime('%-m/%d/%Y'),
        next_date.replace(day=1).strftime('%-m/%-d/%y'),
        0,
        next_index,
        6,
        0,
        next_date.strftime('%Y-%m')
    ]
    df.loc[len(df.index)] = prediction_row

    # Prepare JSON for dashboard
    latest_index = df['indicator'].iloc[-1]
    web_data = {
        'alert': "Alert" if latest_index >= 0.77 else "No Alert",
        'fc_date': parse(df.loc[len(df)-1, 'dateyrmo'] + '-16').strftime('%B %Y'),
        'update_date': datetime.now().strftime("%d %b %Y"),
        'new_index': str(latest_index)
    }

    # Save locally (no SCP)
    save_outputs(web_data, df, JSON_DIR / CONFIG['WEB_DATA_JSON'], csv_path)


if __name__ == "__main__":
    main()