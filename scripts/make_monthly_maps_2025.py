#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Update the TOTAL monthly maps.

This script generates maps for the TOTAL loggerhead turtle app. Monthly maps
for both SST and SST anomaly are produced for a selected date and the
prior 5 months. In addition, a JSON file is created that contains the latest
data to drive the TOTAL dashboard.
"""

# load libraries
import os
import argparse
import numpy as np
import xarray as xr
import calendar
import json
import sys
import subprocess
import warnings
import shutil
from datetime import datetime, timedelta, timezone, date
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from pathlib import Path
from typing import List, Tuple, Dict, Any, Union

# Plotting libraries - moved to the top
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
import matplotlib as mpl


warnings.filterwarnings('ignore')

__author__ = "Dale Robinson"
__credits__ = ["Dale Robinson"]
__license__ = "GPL"
__version__ = "2.1"
__maintainer__ = "Dale Robinson"
__email__ = "dale.Robinson@noaa.gov"
__status__ = "Production"


# --- Configuration ---
CONFIG = {
    # Base dir = repo root
    'BASE_DIR': Path(__file__).resolve().parents[1],
    'URL_BASE': 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}',
    'DATASET_NAMES': {
        'sst': 'jplMURSST41mday',
        'anom': 'jplMURSST41anommday',
    },
    'WEB_DATA_JSON': 'latest.json',
    'LAT_RANGE': [30.0, 38.0],
    'LON_RANGE': [-130.0, -116.0],
}


def plot_map(data_da: xr.DataArray, plot_config: Dict, title_date: str, out_dir: Path) -> Path:
    """Generates and saves a monthly map."""
    out_path = out_dir / f"temp_{plot_config['map_name_var']}.png"
    crs = ccrs.PlateCarree()

    fig = plt.figure(figsize=(12, 8))
    ax = plt.axes(projection=crs)
    ax.coastlines()
    ax.set_title(f"{plot_config['title_text']} {title_date}", fontsize=16, pad=20)

    gl = ax.gridlines(crs=crs, draw_labels=True, linewidth=0.6, color='gray', alpha=0.5, linestyle='-.')
    gl.top_labels = gl.right_labels = False
    gl.ylocator = mticker.FixedLocator(list(range(30, 40, 2)))
    gl.xlocator = mticker.FixedLocator(list(range(-129, -115, 2)))
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER

    if plot_config['plot_var'] == 'anom':
        plt.annotate('4-', xy=(1.03, .86), xycoords='axes fraction', size=12)
        plt.annotate('2-', xy=(1.03, .67), xycoords='axes fraction', size=12)
        plt.annotate('-2-', xy=(1.02, .30), xycoords='axes fraction', size=12)
        plt.annotate('-4-', xy=(1.02, 0.11), xycoords='axes fraction', size=12)
    elif plot_config['plot_var'] == 'sst':
        plt.annotate('82', xy=(1.02, .99), xycoords='axes fraction', size=12)
        plt.annotate('75', xy=(1.02, .74), xycoords='axes fraction', size=12)
        plt.annotate('60', xy=(1.02, .24), xycoords='axes fraction', size=12)
        plt.annotate('54', xy=(1.02, -0.01), xycoords='axes fraction', size=12)

    plt.annotate(plot_config['cbar_label'].replace("C", "F"),
                 xy=(1.03, .50), xycoords='axes fraction',
                 horizontalalignment='center',
                 verticalalignment='center',
                 rotation='vertical',
                 fontsize='large')

    ax.add_feature(cfeature.NaturalEarthFeature('physical', 'land', '50m', edgecolor='face', facecolor='gray'))

    p = data_da.plot(ax=ax,
                     transform=ccrs.PlateCarree(),
                     cmap=plot_config['cbar_cmap'],
                     add_labels=False,
                     add_colorbar=False,
                     vmin=plot_config['cbar_min'],
                     vmax=plot_config['cbar_max'])

    cb = plt.colorbar(p, ticks=plot_config['cbar_ticks'], shrink=0.70, pad=0.04)
    cb.set_label(plot_config['cbar_label'], labelpad=-10, fontsize='large')
    cb.ax.tick_params(labelsize=12)

    # Loggerhead index area
    pt_lon = [-120, -120, -118.6, -117.8, -117.14]
    pt_lat = [34.45, 30.87, 31.12, 32.6, 32.5]
    plt.plot(pt_lon, pt_lat, color='black')

    fig.savefig(out_path, bbox_inches='tight')
    plt.close(fig)

    return out_path


def get_data(da: xr.Dataset, var: str, lat_range: List[float], lon_range: List[float], time_stamp: datetime) -> xr.DataArray:
    """Extract a subset from a dataset."""
    try:
        return da[var].sel(time=time_stamp, method='nearest').sel(
            latitude=slice(lat_range[0], lat_range[1]),
            longitude=slice(lon_range[0], lon_range[1])
        )
    except Exception as e:
        print(f"Error subsetting data: {e}", file=sys.stderr)
        sys.exit(1)


def make_cmap(colors: List[Tuple[float, float, float]], position: Union[List[float], None] = None, bit: bool = False) -> mpl.colors.LinearSegmentedColormap:
    """Make a custom color map from RGB tuples."""
    if not position:
        position = np.linspace(0, 1, len(colors))
    if bit:
        colors = [(r / 255, g / 255, b / 255) for r, g, b in colors]

    cdict = {'red': [], 'green': [], 'blue': []}
    for pos, color in zip(position, colors):
        cdict['red'].append((pos, color[0], color[0]))
        cdict['green'].append((pos, color[1], color[1]))
        cdict['blue'].append((pos, color[2], color[2]))
    return mpl.colors.LinearSegmentedColormap('my_colormap', cdict, 256)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--date', help='Most recent map date (YYYY-MM)', required=True, type=str)
    parser.add_argument('-n', '--numbered', help='Save numbered maps', action='store_true')
    parser.add_argument('-t', '--tstamp', help='Save timestamped maps', action='store_true')
    parser.add_argument('-j', '--json', help='Create latest JSON file', action='store_true')
    args = parser.parse_args()

    if not args.numbered and not args.tstamp:
        print("Error: must specify -n and/or -t")
        sys.exit(1)

    try:
        end_date = parse(args.date).replace(day=16)
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM.", file=sys.stderr)
        sys.exit(1)

    print(f"Generating maps for {end_date.strftime('%Y-%m')}")

    ROOT_DIR = CONFIG['BASE_DIR']
    WORK_DIR = ROOT_DIR / 'work'
    IMAGE_DIR = ROOT_DIR / 'data' / 'images'
    JSON_DIR = ROOT_DIR / 'data' / 'json'
    LAST_DIR = ROOT_DIR / 'data' / 'last'

    for d in [WORK_DIR, IMAGE_DIR, JSON_DIR, LAST_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # Define colormaps
    anom_colors = [(0, 0, 0.5625), (0, 0, 0.625), (0, 0, 0.6875),
                   (1, 0, 0), (0.9375, 0, 0), (0.875, 0, 0)]
    anom_cmap = make_cmap(anom_colors, bit=False)

    plot_configs = [
        {
            'cbar_cmap': 'jet', 'title_text': 'SST, ',
            'cbar_ticks': [12, 16, 24, 28],
            'cbar_label': "SST ($^\circ$C)",
            'cbar_min': 12, 'cbar_max': 28,
            'plot_var': 'sst', 'map_name_var': 'sst'
        },
        {
            'cbar_cmap': anom_cmap, 'title_text': 'Anomaly, ',
            'cbar_ticks': [-3, -2, -1, 1, 2, 3],
            'cbar_label': "Anomaly ($^\circ$C)",
            'cbar_min': -3, 'cbar_max': 3,
            'plot_var': 'anom', 'map_name_var': 'sstAnom'
        }
    ]

    # Open datasets
    try:
        sst_da = xr.open_dataset(CONFIG['URL_BASE'].format(CONFIG['DATASET_NAMES']['sst']))
        anom_da = xr.open_dataset(CONFIG['URL_BASE'].format(CONFIG['DATASET_NAMES']['anom']))
    except Exception as e:
        print(f"Error opening datasets from ERDDAP: {e}", file=sys.stderr)
        sys.exit(1)

    # Make maps for last 6 months
    table_dict = {}
    for i in range(0, 6):
        selected_date = end_date - relativedelta(months=+i)
        dict_id = f"t{i}"
        table_dict[dict_id] = {
            'date': selected_date.strftime('%Y%m'),
            'month': calendar.month_name[selected_date.month][0:3]
        }

        date_for_title = selected_date.strftime('%b-%Y')
        time_stamp = selected_date.strftime('%Y%m%d')

        for pc in plot_configs:
            da_to_plot = sst_da if pc['plot_var'] == 'sst' else anom_da
            data_map = get_data(da_to_plot, pc['map_name_var'],
                                CONFIG['LAT_RANGE'], CONFIG['LON_RANGE'], selected_date)

            temp_map = plot_map(data_map, pc, date_for_title, WORK_DIR)

            # Save images
            if args.numbered:
                shutil.copyfile(temp_map, LAST_DIR / f"{pc['plot_var']}_{i}.png")
            if args.tstamp:
                shutil.copyfile(temp_map, IMAGE_DIR / f"{pc['plot_var']}_{time_stamp}.png")

            # Store mean values
            try:
                mean_val = np.nanmean(data_map.values)
                table_dict[dict_id][pc['plot_var']] = str(round(mean_val, 2))
            except Exception as e:
                print(f"Error calculating mean: {e}", file=sys.stderr)
                table_dict[dict_id][pc['plot_var']] = "N/A"

    # Save JSON
    if args.json:
        json_path = JSON_DIR / CONFIG['WEB_DATA_JSON']
        with open(json_path, 'w') as outfile:
            json.dump(table_dict, outfile, indent=4)
        print(f"Saved JSON: {json_path}")


if __name__ == "__main__":
    main()