# python v 3.
# location home/cwatch/miniconda3/bin/python
# functions
# -*- coding: utf-8 -*-
"""
Update TOTAL loggerhead turtle index, plot, and JSON for web dashboard.

This script updates the TOTAL loggerhead turtle index, generates an indicator plot,
and writes web_data.json (and an archived YYYYMM_web_data.json) in data/json/.
It is designed to run locally or via GitHub Actions.

Created on Sun May  7 07:25:01 2023

@author: dalerobinson
"""

import os
import netCDF4
import numpy as np
import argparse
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import sys
import pandas as pd
import plotnine as p9
import json
import warnings
from pathlib import Path
import shutil
warnings.filterwarnings('ignore')


# === Utility Functions ===
def find_closest(val1, val2, target):
    return val2 if target - val1 >= val2 - target else val1


def get_closest_value(arr, target):
    n = len(arr)
    left = 0
    right = n - 1
    mid = 0
    if target >= arr[n - 1]:
        return arr[n - 1]
    if target <= arr[0]:
        return arr[0]
    while left < right:
        mid = (left + right) // 2
        if target < arr[mid]:
            right = mid
        elif target > arr[mid]:
            left = mid + 1
        else:
            return arr[mid]
    if target < arr[mid]:
        return find_closest(arr[mid - 1], arr[mid], target)
    else:
        return find_closest(arr[mid], arr[mid + 1], target)


def max_min_idx(coord_arr, coord_val):
    min_val = get_closest_value(coord_arr, coord_val[0])
    min_idx = list(coord_arr).index(min_val)
    max_val = get_closest_value(coord_arr, coord_val[1])
    max_idx = list(coord_arr).index(max_val)
    return [min_idx, max_idx]


def plot_index(my_data, png_name, png_dir):
    """Generate a 12-month indicator plot."""
    indx_plot = p9.ggplot(data=my_data, mapping=p9.aes(x='dateyrmo', y='indicator'))
    fig = (indx_plot +
           p9.geom_point(p9.aes(color='indicator'), size=4) +
           p9.labs(x="", y="Indicator") +
           p9.theme_light() +
           p9.geom_hline(yintercept=0.77, linetype="solid", color="red", size=1) +
           p9.theme(text=p9.element_text(size=16),
                    axis_text_x=p9.element_text(angle=60, hjust=1)) +
           p9.ylim(-0.5, 1.5))
    p9.ggsave(plot=fig, filename=os.path.join(png_dir, png_name),
              dpi=72, height=4.5, width=9, units="in")


# === Configuration ===
CONFIG = {
    'ROOT_DIR': Path(__file__).resolve().parents[1],
    'JSON_FILE_NAME': 'web_data.json',
    'JSON_FILE_ARCHIVE_TEMPLATE': '{}_web_data.json',
}

ROOT_DIR = CONFIG['ROOT_DIR']
WORK_DIR = ROOT_DIR / 'work'
IMG_DIR = ROOT_DIR / 'data' / 'images'
JSON_DIR = ROOT_DIR / 'data' / 'json'
RES_DIR = ROOT_DIR / 'data' / 'resources'

for d in [WORK_DIR, IMG_DIR, JSON_DIR]:
    d.mkdir(parents=True, exist_ok=True)

lat_range = [30.8, 34.5]
lon_range = [-120.3, -116]


# === Main ===
def main():
    parser = argparse.ArgumentParser(
        description="Update TOTAL loggerhead turtle index, plot, and JSON.")
    parser.add_argument('-e', '--enddate', help='End date (YYYY-mm)', required=False, type=str)
    parser.add_argument('-u', '--update', action='store_true', help='Update index values')
    parser.add_argument('-p', '--plot', action='store_true', help='Generate indicator plot')
    parser.add_argument('-j', '--json', action='store_true', help='Write web_data.json output')
    args = parser.parse_args()

    if not (args.update or args.plot or args.json):
        print("No operation specified. Use -u, -p, or -j.")
        sys.exit(0)

    loggerhead_indx = 'loggerhead_indx.csv'
    indicator_png = 'indicator_latest.png'
    json_file = CONFIG['JSON_FILE_NAME']

    base_url = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}'
    opendap_url = base_url.format('jplMURSST41anommday')
    opendapsst_url = base_url.format('jplMURSST41')

    # === Update data ===
    if args.update:
        df = pd.read_csv(RES_DIR / loggerhead_indx)
        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
        df.drop(df.tail(1).index, inplace=True)
        index_time = df['dateyrmo'].values

        edt = netCDF4.Dataset(opendap_url, 'r')
        edt_time = edt['time'][:]
        ds_lat = np.array(edt.variables['latitude'][:])
        ds_lon = np.array(edt.variables['longitude'][:])

        latidx_range = max_min_idx(ds_lat, lat_range)
        lonidx_range = max_min_idx(ds_lon, lon_range)

        edt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                           for ln in edt_time]
        edt_time_str_16 = ['{0:%Y-%m}'.format(ln) for ln in edt_time_obj_16]

        missing = sorted(set(edt_time_str_16) - set(index_time))
        if not missing:
            print("No new data to process.")
            sys.exit(0)

        indices_A = [edt_time_str_16.index(x) for x in missing]
        indices_A.sort()

        edsst = netCDF4.Dataset(opendapsst_url, 'r')
        edstt_time = edt['time'][:]
        edstt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                             for ln in edstt_time]
        edstt_time_obj_16 = [ln.replace(hour=0, minute=0, second=0, microsecond=0)
                             for ln in edstt_time_obj_16]

        for indx in indices_A:
            edt_anom = edt['sstAnom'][indx,
                                      latidx_range[0]:latidx_range[1],
                                      lonidx_range[0]:lonidx_range[1]]
            sst_indx = edstt_time_obj_16.index(edt_time_obj_16[indx])
            edsst_sst = edsst['analysed_sst'][sst_indx,
                                              latidx_range[0]:latidx_range[1],
                                              lonidx_range[0]:lonidx_range[1]]
            df.loc[len(df.index)] = [
                round(edt_anom.mean(), 2),
                round(df['anom'].iloc[len(df.index)-7:len(df.index)-1].mean(), 2),
                '{0:%Y-%m}'.format(edt_time_obj_16[indx])
            ]

        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
        df.to_csv(RES_DIR / loggerhead_indx, index=False, encoding='utf-8')
        print("Index updated.")

    # === Plot ===
    if args.plot:
        indx_df = pd.read_csv(RES_DIR / loggerhead_indx)
        indx_df['dateyrmo'] = pd.to_datetime(indx_df['dateyrmo'], format='%Y-%m')
        end_time = indx_df['dateyrmo'].max()
        start_time = end_time - timedelta(days=390)
        global t_range2
        t_range2 = [start_time, end_time]
        global indx_tf
        indx_tf = ["black" if x < 0.77 else "red" for x in indx_df['indicator'].tolist()]
        plot_index(indx_df, indicator_png, IMG_DIR)
        print(f"📊 Plot saved to {IMG_DIR / indicator_png}")

    # === JSON Output ===
    if args.json:
        indx_df = pd.read_csv(RES_DIR / loggerhead_indx)
        latest_index = float(indx_df['indicator'].iloc[-1])
        alert_status = "Alert" if latest_index >= 0.77 else "No Alert"
        forecast_date = datetime.now().strftime("%B %Y")
        update_date = datetime.now().strftime("%d %b, %Y")

        web_data = {
            "alert": alert_status,
            "fc_date": forecast_date,
            "update_date": update_date,
            "new_index": f"{latest_index:.2f}"
        }

        # Write main and archive JSON
        main_json = JSON_DIR / CONFIG['JSON_FILE_NAME']
        dated_json = JSON_DIR / CONFIG['JSON_FILE_ARCHIVE_TEMPLATE'].format(datetime.now().strftime("%Y%m"))

        with open(main_json, 'w') as f:
            json.dump(web_data, f, indent=4)
        with open(dated_json, 'w') as f:
            json.dump(web_data, f, indent=4)

        print(f"JSON saved to {main_json}")
        print(f"Archived JSON: {dated_json}")


if __name__ == "__main__":
    main()