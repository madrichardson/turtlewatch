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

loggerhead_indx = "loggerhead_indx.csv"
indicator_png = "indicator_latest.png"
json_file = "web_data.json"

lat_range = [30.8, 34.5]
lon_range = [-120.3, -116]


# === Main ===

def main():
    """Update TOTAL index, create plot, and generate web_data.json."""
    parser = argparse.ArgumentParser(description="Update TOTAL Loggerhead index")
    parser.add_argument("-u", "--update", action="store_true", help="Update index values from ERDDAP")
    parser.add_argument("-p", "--plot", action="store_true", help="Generate latest indicator plot")
    parser.add_argument("-j", "--json", action="store_true", help="Create web_data.json summary")
    args = parser.parse_args()

    ROOT_DIR = Path(__file__).resolve().parents[1]
    WORK_DIR = ROOT_DIR / "work"
    IMG_DIR = ROOT_DIR / "data" / "images"
    JSON_DIR = ROOT_DIR / "data" / "json"
    RES_DIR = ROOT_DIR / "data" / "resources"
    WORK_DIR.mkdir(exist_ok=True)
    IMG_DIR.mkdir(exist_ok=True)
    JSON_DIR.mkdir(exist_ok=True)

    loggerhead_indx = "loggerhead_indx.csv"
    indicator_png = "indicator_latest.png"
    json_file = "web_data.json"

    # --- UPDATE SECTION (optional)
    if args.update:
        df = pd.read_csv(RES_DIR / loggerhead_indx)
        df.sort_values(by=["dateyrmo"], inplace=True, ignore_index=True)
        df.drop(df.tail(1).index, inplace=True)
        index_time = df["dateyrmo"].values

        base_url = "https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}"
        opendap_url = base_url.format("jplMURSST41anommday")
        opendapsst_url = base_url.format("jplMURSST41")

        edt = netCDF4.Dataset(opendap_url, "r")
        edt_time = edt["time"][:]
        ds_lat = np.array(edt.variables["latitude"][:])
        ds_lon = np.array(edt.variables["longitude"][:])
        lat_range = [30.8, 34.5]
        lon_range = [-120.3, -116]
        latidx_range = max_min_idx(ds_lat, lat_range)
        lonidx_range = max_min_idx(ds_lon, lon_range)
        edt_time_obj = [datetime.fromtimestamp(t).astimezone(timezone.utc) for t in edt_time]
        edt_time_str = [f"{t:%Y-%m}" for t in edt_time_obj]

        missing = sorted(set(edt_time_str) - set(index_time))

        if len(missing) == 0:
            print("No new data to process — continuing to JSON and plot steps.")
            sys.exit()
        else:
            edsst = netCDF4.Dataset(opendapsst_url, "r")
            edstt_time_obj = [datetime.fromtimestamp(t).astimezone(timezone.utc) for t in edt["time"][:]]
            edstt_time_obj = [t.replace(hour=0, minute=0, second=0, microsecond=0) for t in edstt_time_obj]

            indx_missing = sorted(missing)
            both = set(edt_time_str).intersection(indx_missing)
            indices_A = [edt_time_str.index(x) for x in both]
            indices_A.sort()

            
            for i in indices_A:
                dt = edt_time_obj[i]

                edt_anom = edt["sstAnom"][i,
                                        latidx_range[0]:latidx_range[1],
                                        lonidx_range[0]:lonidx_range[1]]

                sst_i = edstt_time_obj.index(dt)
                _ = edsst["analysed_sst"][sst_i,
                                        latidx_range[0]:latidx_range[1],
                                        lonidx_range[0]:lonidx_range[1]]

                df.loc[len(df.index)] = [
                    f"{dt.month}/{dt.day}/{dt.year}",   # date16
                    f"{dt.month}/1/{dt.strftime('%y')}",     # date01
                    round(edt_anom.mean(), 2),   # anom (REAL VALUE)
                    round(df["anom"].iloc[len(df.index)-7:len(df.index)-1].mean(), 2),  # indicator
                    6,                           # count
                    0,                           # stdev
                    f"{dt:%Y-%m}",               # dateyrmo
                ]

        df.sort_values(by=["dateyrmo"], inplace=True, ignore_index=True)

        # add ONE placeholder "next month" row (after loop)
        next_month = parse(df.loc[len(df.index)-1]["dateyrmo"]) + timedelta(days=30)
        
        df.loc[len(df.index)] = [
            f"{next_month.month}/{next_month.day}/{next_month.year}",
            f"{next_month.month}/1/{next_month.strftime('%y')}",
            0,
            round(df["anom"].iloc[len(df.index)-7:len(df.index)-1].mean(), 2),
            6,
            0,
            f"{next_month:%Y-%m}",
        ]

        df.to_csv(RES_DIR / loggerhead_indx, index=False, encoding="utf-8")
        print(f"Updated {loggerhead_indx} with {len(indices_A)} new records.")

    # --- JSON CREATION (always runs if -j)
    if args.json:
        print("Creating web_data.json...")
        df = pd.read_csv(RES_DIR / loggerhead_indx)
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

        json_path = JSON_DIR / json_file
        print(f"Writing JSON to: {json_path.resolve()}")
        json_path.write_text(json.dumps(web_data, indent=4))
        print("JSON successfully written:")
        print(json.dumps(web_data, indent=4))

    # --- PLOT SECTION (optional)
    if args.plot:
        print("Plotting indicator series...")
        df = pd.read_csv(RES_DIR / loggerhead_indx)
        df["dateyrmo"] = pd.to_datetime(df["dateyrmo"], format="%Y-%m")
        end_time = df["dateyrmo"].max()
        start_time = end_time - timedelta(days=395)
        global t_range2, indx_tf
        indx_tf = ["red" if v >= 0.77 else "black" for v in df["indicator"]]
        t_range2 = [start_time, end_time]
        plot_index(df, indicator_png, IMG_DIR)
        print("Plot saved to", IMG_DIR / indicator_png)
