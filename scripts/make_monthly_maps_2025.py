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
import time
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


# --- Configuration and Constants ---
CONFIG = {
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


def send_to_erddap(local_file: Path, remote_path: Path) -> bool:
    """Sends a local file to a remote ERDDAP server via SCP.

    Args:
        local_file (Path): The path to the local file to send.
        remote_path (Path): The path on the remote server where the file should be saved.

    Returns:
        bool: True if the transfer was successful, False otherwise.
    """
    cmd = ['scp', str(local_file), f'{CONFIG["CW_SERVER"]}:{remote_path.as_posix()}']
    
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


def open_xr_dataset_with_retry(
    url: str,
    retries: int = 5,
    backoff_seconds: int = 20,
) -> xr.Dataset:
    """
    Open a remote ERDDAP dataset with xarray using simple retry/backoff logic.

    Parameters
    ----------
    url : str
        Full URL to the ERDDAP griddap endpoint (e.g., CONFIG['URL_BASE'].format(...)).
    retries : int, optional
        Maximum number of attempts. Defaults to 5.
    backoff_seconds : int, optional
        Base delay between attempts. The actual delay is
        backoff_seconds * attempt_number (20, 40, 60, ... with default).
    
    Returns
    -------
    xr.Dataset
        Open xarray Dataset pointing to the remote resource.

    Exits
    -----
    sys.exit(1)
        If all attempts fail, the function prints an error and exits to avoid
        running the map-generation logic on missing data.
    """
    for attempt in range(1, retries + 1):
        try:
            print(
                f"Opening xarray dataset (attempt {attempt}/{retries}): {url}",
                file=sys.stderr,
            )
            return xr.open_dataset(url, engine="netcdf4")
        except OSError as e:
            # Common for network / remote I/O problems
            if attempt == retries:
                print(
                    f"Error opening xarray dataset at {url} after {retries} attempts: {e}",
                    file=sys.stderr,
                )
                sys.exit(1)
            sleep_for = backoff_seconds * attempt
            print(
                f"Failed to open dataset ({e}); sleeping {sleep_for} seconds before retry...",
                file=sys.stderr,
            )
            time.sleep(sleep_for)
        except Exception as e:
            print(
                f"Non-retriable error opening xarray dataset at {url}: {e}",
                file=sys.stderr,
            )
            sys.exit(1)


def plot_map(data_da: xr.DataArray, plot_config: Dict, title_date: str, work_dir: Path) -> Path:
    """Generates a TOTAL monthly map and saves it to a file.

    Args:
        data_da (xr.DataArray): Xarray DataArray with the plot data.
        plot_config (dict): Dictionary with plot constants.
        title_date (str): Title to place on the map.
        work_dir (Path): Path to the work directory.

    Returns:
        Path: The path to the saved map image file.
    """
    temp_map = work_dir / f"temp_{plot_config['map_name_var']}.png"
    crs = ccrs.PlateCarree()

    fig = plt.figure(figsize=(12, 8))
    ax = plt.axes(projection=crs)
    ax.coastlines()
    ax.set_title(f"{plot_config['title_text']} {title_date}", fontsize=16, pad=20, horizontalalignment='center', verticalalignment='top')
    
    gl = ax.gridlines(crs=crs, draw_labels=True, linewidth=0.6, color='gray', alpha=0.5, linestyle='-.')
    gl.top_labels = gl.right_labels = False
    gl.ylocator = mticker.FixedLocator(list(range(30, 40, 2)))
    gl.xlocator = mticker.FixedLocator(list(range(-129, -115, 2)))
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    gl.xlabel_style = {'size': 12, 'color': 'black'}
    gl.ylabel_style = {'size': 12, 'color': 'black'}

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
                 xy=(1.03, .50),
                 xycoords='axes fraction',
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
    
    # Plotting the loggerhead index area
    pt_lon = [-120, -120, -118.6, -117.8, -117.14]
    pt_lat = [34.45, 30.87, 31.12, 32.6, 32.5]
    plt.plot(pt_lon, pt_lat, color='black')

    fig.savefig(temp_map, bbox_inches='tight')
    plt.close(fig) # Close the figure to free up memory

    return temp_map


def get_data(da: xr.DataArray, var: str, lat_range: List[float], lon_range: List[float], time_stamp: datetime) -> xr.DataArray:
    """Extracts a subset of data from an Xarray DataArray.

    Args:
        da (xr.DataArray): The input DataArray.
        var (str): The variable name to select.
        lat_range (List[float]): The latitude range [min, max].
        lon_range (List[float]): The longitude range [min, max].
        time_stamp (datetime): The specific time to select.

    Returns:
        xr.DataArray: The subsetted DataArray.
    """
    try:
        data_subset = da[var].sel(time=time_stamp, method='nearest').sel(
            latitude=slice(lat_range[0], lat_range[1]),
            longitude=slice(lon_range[0], lon_range[1])
        )
        return data_subset
    except Exception as e:
        print(f"Error subsetting data: {e}", file=sys.stderr)
        sys.exit(1)


def make_cmap(colors: List[Tuple[float, float, float]], position: Union[List[float], None] = None, bit: bool = False) -> mpl.colors.LinearSegmentedColormap:
    """Make a color map from a list of RGB values.

    Args:
        colors (list): list of tuples which contain RGB values.
        position (list, optional): list containing values (0 to 1) to dictate color location.
        bit (bool, optional): Boolean is True if RGB values are 8-bit [0 to 255]. Default is False.

    Returns:
        matplotlib.colors.LinearSegmentedColormap: The created color map.
    """
    if position and len(position) != len(colors):
        sys.exit("Position length must be the same as colors.")
    
    if position and (position[0] != 0 or position[-1] != 1):
        sys.exit("Position must start with 0 and end with 1.")

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
    """Runs the main function to generate and transfer maps."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-d', '--date', help='Date of the most recent map to make, i.e., YYYY-MM', required=True, type=str)
    parser.add_argument('-n', '--numbered', help='Create numbered maps', action='store_true')
    parser.add_argument('-t', '--tstamp', help='Create timestamped maps', action='store_true')
    parser.add_argument('-j', '--json', help='Create latest JSON file', action='store_true')
    args = parser.parse_args()

    if not args.numbered and not args.tstamp:
        print("Error: -n [--numbered] or -t [--tstamp] or both must be selected.")
        sys.exit(1)
    
    try:
        end_date = parse(args.date).replace(day=16)
    except ValueError:
        print("Error: Invalid date format. Use YYYY-MM.", file=sys.stderr)
        sys.exit(1)
    
    print(f"Start date for maps: {end_date.strftime('%Y-%m')}")
    
    # Define directories
    ROOT_DIR = CONFIG['BASE_DIR']
    WORK_DIR = ROOT_DIR / 'work'
    IMAGE_DIR = ROOT_DIR / 'data' / 'images'
    JSON_DIR = ROOT_DIR / 'data' / 'json'
    LAST_DIR = ROOT_DIR / 'data' / 'last'

    # Ensure directories exist
    for d in [WORK_DIR, IMAGE_DIR, JSON_DIR, LAST_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    # Define plot configuration dictionaries
    anom_colors = [(0, 0, 0.5625), (0, 0, 0.625), (0, 0, 0.6875), (0, 0, 0.75), (0, 0, 0.8125), (0, 0, 0.875), (0, 0, 0.9375), (0, 0, 1), (0, 0.0625, 1), (0, 0.125, 1), (0, 0.1875, 1), (0, 0.25, 1), (0, 0.3125, 1), (0, 0.375, 1), (0, 0.4375, 1), (0.1, 0.55, 1), (0.2, 0.65, 1), (0.3, 0.7375, 1), (0.4, 0.8125, 1), (0.5, 0.875, 1), (0.6, 0.925, 1), (0.7, 0.9625, 1), (0.8, 0.9875, 1), (0.9, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 1), (1, 1, 0.9), (1, 0.9875, 0.8), (1, 0.9625, 0.7), (1, 0.925, 0.6), (1, 0.875, 0.5), (1, 0.8125, 0.4), (1, 0.7375, 0.3), (1, 0.65, 0.2), (1, 0.55, 0.1), (1, 0.4375, 0), (1, 0.375, 0), (1, 0.3125, 0), (1, 0.25, 0), (1, 0.1875, 0), (1, 0.125, 0), (1, 0.0625, 0), (1, 0, 0), (0.9375, 0, 0), (0.875, 0, 0), (0.8125, 0, 0), (0.75, 0, 0), (0.6875, 0, 0), (0.625, 0, 0), (0.5625, 0, 0), (0.5, 0, 0)]
    anom_cmap = make_cmap(anom_colors, bit=False)

    plot_configs = [
        {
            'cbar_cmap': 'jet', 'title_text': 'SST, ', 'cbar_ticks': [12, 16, 24, 28],
            'cbar_label': "SST ($^\circ$C)", 'cbar_min': 12, 'cbar_max': 28,
            'plot_var': 'sst', 'map_name_var': 'sst'
        },
        {
            'cbar_cmap': anom_cmap, 'title_text': 'Anomaly, ', 'cbar_ticks': [-3, -2, -1, 1, 2, 3],
            'cbar_label': "Anomaly ($^\circ$C)", 'cbar_min': -3, 'cbar_max': 3,
            'plot_var': 'anom', 'map_name_var': 'sstAnom'
        }
    ]

    # Make pointers to datasets (with retry)
    sst_url = CONFIG['URL_BASE'].format(CONFIG['DATASET_NAMES']['sst'])
    anom_url = CONFIG['URL_BASE'].format(CONFIG['DATASET_NAMES']['anom'])

    sst_da = open_xr_dataset_with_retry(sst_url)
    anom_da = open_xr_dataset_with_retry(anom_url)


    # Make maps for the last 6 months
    table_dict = {}
    
    for i in range(0, 6):
        selected_date = end_date - relativedelta(months=+i)
        
        # Determine the key for the JSON dictionary
        dict_id = f"t{i}"
        table_dict[dict_id] = {}
        table_dict[dict_id]['date'] = selected_date.strftime('%Y%m')
        table_dict[dict_id]['month'] = calendar.month_name[selected_date.month][0:3]
        
        date_for_title = selected_date.strftime('%b-%Y')
        time_stamp = selected_date.strftime('%Y%m%d')
        
        for pc in plot_configs:
            da_to_plot = sst_da if pc['plot_var'] == 'sst' else anom_da
            
            data_map = get_data(da_to_plot, pc['map_name_var'],
                                CONFIG['LAT_RANGE'], CONFIG['LON_RANGE'], selected_date)
            
            temp_map_name = plot_map(data_map, pc, date_for_title, WORK_DIR)
            
            map_name_numbered = f"{pc['plot_var']}_{i}.png"
            map_name_tstamp = f"{pc['plot_var']}_{time_stamp}.png"
            
            # Transfer and archive maps
            if args.numbered:
                ##send_to_erddap(temp_map_name, cw_image_dir / map_name_numbered)
                shutil.copyfile(temp_map_name, LAST_DIR / map_name_numbered)
                
            if args.tstamp:
                #send_to_erddap(temp_map_name, cw_dash_im / map_name_tstamp)
                shutil.copyfile(temp_map_name, IMAGE_DIR / map_name_tstamp)

            # Calculate and store mean values for JSON file
            try:
                mean_val = np.nanmean(data_map.values)
                table_dict[dict_id][pc['plot_var']] = str(round(mean_val, 2))
                print(f"Mean for {pc['plot_var']} on {time_stamp}: {mean_val:.2f}")
            except Exception as e:
                print(f"Error calculating mean for {pc['plot_var']}: {e}", file=sys.stderr)
                table_dict[dict_id][pc['plot_var']] = "N/A"
    
    # Save JSON file and transfer
    if args.json:
        last_data_file = JSON_DIR / CONFIG['WEB_DATA_JSON']
        try:
            with open(last_data_file, 'w') as outfile:
                json.dump(table_dict, outfile, indent=4)
            print(f"JSON file saved to {last_data_file}")
            ## send_to_erddap(last_data_file, cw_json_dir / last_data_file.name)
        except IOError as e:
            print(f"Error saving JSON file: {e}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
