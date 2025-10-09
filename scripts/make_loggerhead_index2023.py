# python v 3.
# location home/cwatch/miniconda3/bin/python
# functions
# -*- coding: utf-8 -*-
"""
Created on Sun May  7 07:25:01 2023

@author: dalerobinson
"""

# load libraries
import os
import netCDF4
import numpy as np
import argparse
# import numpy.ma as ma
from datetime import datetime, timedelta, timezone
from pathlib import Path
# import csv
# import dateutil
from dateutil.parser import parse
# import urllib.request
# import codecs
import sys
# from decimal import Decimal
import pandas as pd
import plotnine as p9
import json
import subprocess
import warnings
import pprint
import shutil
warnings.filterwarnings('ignore')


def find_closest(val1, val2, target):
    return val2 if target - val1 >= val2 - target else val1


def get_closest_value(arr, target):
    n = len(arr)
    left = 0
    right = n - 1
    mid = 0
    # edge case - last or above all
    if target >= arr[n - 1]:
        return arr[n - 1]
    # edge case - first or below all
    if target <= arr[0]:
        return arr[0]
    # BSearch solution: Time & Space: Log(N)
    while left < right:
        mid = (left + right) // 2  # find the mid
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


def year_month(word):
    return word[6::]+word[:2:]


def plot_index(my_data, png_name, png_dir):
    # make 12 month indicator plot
    indx_plot = p9.ggplot(data=my_data,
                          mapping=p9.aes(x='dateyrmo', y='indicator'))

    fig = (indx_plot +
           p9.geom_point(p9.aes(color=indx_tf), size=4) +
           p9.labs(x="", y="Indicator") +
           p9.theme_light() +
           p9.scale_x_date(limits=t_range2, date_labels="%m/%Y") +
           p9.geom_hline(yintercept=0.77, linetype="solid", color="red", size=1) +
           p9.theme(text=p9.element_text(size=16), axis_text_x=p9.element_text(angle=60, hjust=1)) +
           p9.scale_color_manual(name="Status", values=['black', 'red'], labels=["No Alert", "Alert"]) +
           p9.ylim(-0.5, 1.5))

    # create png of YTD plot
    p9.ggsave(plot=fig,
              filename=os.path.join(png_dir, png_name),
              dpi=72,
              height=4.5,
              width=9,
              units="in"
              )


# Set Directories
ROOT_DIR = Path(__file__).resolve().parents[1]
WORK_DIR = ROOT_DIR / "work"
IMG_DIR = ROOT_DIR / "data" / "images"
JSON_DIR = ROOT_DIR / "data" / "json"
RES_DIR = ROOT_DIR / "data" / "resources"


# set lat/lon range for indicator area
lat_range = [30.8, 34.5]
lon_range = [-120.3, -116]

today = datetime.now()


def main():

    # Set up argument parsing
    help_text = {
                 "arg_help": "This program updates the TOTAL loggerhead turtle index, creates json file with most recent information, and plots a 1 year graph of the index. A user can customize the plot date (default is last 12 months) with -e (default is the most recent 12 months). Argument switches intitial plot (-p), json (-j), or the index update (-u).",
                 "end": "end date (most recent) date as YYYY-mm. The plot will go back in time one year. Default is most recent data for index",
                 "update": "Switch to turn on index update. -u or -p or both must be selected",
                 "json": "Switch to turn on status json creation",
                 "plot": "Switch to turn on plotting. -u or -p or both must be selected"
                }
    
    # set the stand and end dates with args
    parser = argparse.ArgumentParser(
        description="Update TOTAl loggerhead index, plot, and JSON."
    )

    parser.add_argument('-e', '--enddate',
                        help='End date (YYYY-mm)',
                        required=False,
                        type=str
                        )
    parser.add_argument('-u', '--update',
                        action='store_true',
                        help='Update indicator data',
                        )
    parser.add_argument('-p', '--plot',
                        action='store_true',
                        help='Generate latest indicator plot',
                        )
    parser.add_argument('-j', '--json',
                        action='store_true',
                        help='Create web_data.json output',
                        )
    args = parser.parse_args()

    if not args.plot and not args.update:
        print('either -u or -p or both must be selected')
        pprint.pprint(help_text)
        sys.exit()

    # set file names
    loggerhead_indx = 'loggerhead_indx.csv'  # indicator file
    indicator_png = 'indicator_latest.png'  # indicator plot
    json_file = 'web_data.json'

    # set dataset urls
    base_url = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}'
    opendap_url = base_url.format('jplMURSST41anommday')
    opendapsst_url = base_url.format('jplMURSST41')

    if args.update:
        # read stored index data
        df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
        df.drop(df.tail(1).index, inplace=True)
        index_time = df['dateyrmo'].values

        edt = netCDF4.Dataset(opendap_url, 'r')
        edt_time = edt['time'][:]
        ds_lat = np.array(edt.variables['latitude'][:])
        ds_lon = np.array(edt.variables['longitude'][:])

        latidx_range = max_min_idx(ds_lat, lat_range)
        lonidx_range = max_min_idx(ds_lon, lon_range)

        # edt.close()
        edt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                           for ln in edt_time
                           ]

        edt_time_str_16 = ['{0:%Y-%m}'.format(ln) for ln in edt_time_obj_16]

        edt_time_set1 = set(edt_time_str_16)
        index_time_set2 = set(index_time)

        missing = list(sorted(edt_time_set1 - index_time_set2))
        if len(missing) == 0:
            print('no new data to process')
            sys.exit()

        indx_missing = sorted(missing)
        both = set(edt_time_str_16).intersection(indx_missing)
        indices_A = [edt_time_str_16.index(x) for x in both]
        indices_A.sort()

        edsst = netCDF4.Dataset(opendapsst_url, 'r')
        edstt_time = edt['time'][:]
        edstt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                             for ln in edstt_time
                             ]
        edstt_time_obj_16 = [ln.replace(hour=0,
                                        minute=0,
                                        second=0,
                                        microsecond=0
                                        )
                             for ln in edstt_time_obj_16
                             ]

        for indx in indices_A:
            edt_anom = edt['sstAnom'][indx,
                                      latidx_range[0]:latidx_range[1],
                                      lonidx_range[0]:lonidx_range[1]
                                      ]
            sst_indx = edstt_time_obj_16.index(edt_time_obj_16[indx])

            edsst_sst = edsst['analysed_sst'][sst_indx,
                                              latidx_range[0]:latidx_range[1],
                                              lonidx_range[0]:lonidx_range[1]
                                              ]

            df.loc[len(df.index)] = [round(edt_anom.mean(), 2),
                                     round(df['anom'].iloc[len(df.index)-7:
                                                           len(df.index)-1
                                                           ].mean(), 2),
                                     '{0:%Y-%m}'.format(edt_time_obj_16[indx])
                                     ]

        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)

        df.loc[len(df.index)] = [0,
                                 round(df['anom'].iloc[len(df.index)-7:
                                                       len(df.index)-1
                                                       ].mean(), 2),
                                 '{0:%Y-%m}'.format(parse(df.loc[len(df.index)-1]
                                                          ['dateyrmo']
                                                          ) + timedelta(days=30))
                                 ]

        df.to_csv(loggerhead_indx, index=False, encoding='utf-8')

    # new plot prep
    if args.plot:
        indx_df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        # Convert mm/01/YYYY column to date
        indx_df['dateyrmo'] = pd.to_datetime(indx_df['dateyrmo'],
                                             format='%Y-%m'
                                             )

        # pull indicator data and create list to use for ggplot
        indx_list = indx_df['indicator'].tolist()
        indx_list2 = [False if x < 0.77 else True for x in indx_list]
        indx_tf = ["black" if x < 0.77 else "red" for x in indx_list]

        # Pull last date and create YTD range list for ggplot
        if args.end is not None:
            end_time = indx_df['dateyrmo'].max()
        else:
            end_time = parse(args.end).replace(day=1)

        start_time = end_time - timedelta(days=365+25)
        print(start_time)
        start_time = start_time.replace(day=1)
        print(start_time)
        t_range2 = [start_time, end_time]

        plot_index(indx_df, indicator_png, WORK_DIR)

        #myCmd = ' '.join(['scp', os.path.join(WORK_DIR, indicator_png), cw_dir])
        #print('copy index image to coastwatch', subprocess.call(myCmd, shell=True))

        myCmd = ' '.join(['mv',
                          os.path.join(WORK_DIR, indicator_png),
                          os.path.join(IMG_DIR, indicator_png)
                          ])
        print('mv index image to archive', subprocess.call(myCmd, shell=True))

        if end_time.month == 12:
            yearly_plot = "indicator_" + str(end_time.year) + ".png"
            shutil.copyfile(os.path.join(WORK_DIR, indicator_png),
                            os.path.join(WORK_DIR, yearly_plot)
                            )

        indx_df.close()

    if args.json:
        indx_df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        latest_index = indx_df['indicator'].iloc[-1]
        # alert status
        if (latest_index >= 0.77):
            alert_status = "Alert"
        else:
            alert_status = "No Alert"
        # Dates
        forecast_date = end_time.strftime("%B, %Y")
        update_date = datetime.now().strftime("%d %b, %Y")
        latest_index = str(latest_index)

        web_data = {
                    'alert': alert_status,
                    'fc_date': forecast_date,
                    'update_date': update_date,
                    'new_index': latest_index
                    }

        # web infor saved to json file
        with open(os.path.join(WORK_DIR, json_file), 'w') as outfile:
            json.dump(web_data, outfile)

        print('json file saved')
        indx_df.close()

        myCmd = ' '.join(['scp', os.path.join(WORK_DIR, json_file), cw_dir])
        print('copy json to coastwatch', subprocess.call(myCmd, shell=True))

        myCmd = ' '.join(['mv',
                          os.path.join(WORK_DIR, json_file),
                          os.path.join(JSON_DIR, json_file)
                          ])
        print('mv json to coastwatch', subprocess.call(myCmd, shell=True))



## END
    parser.add_argument('-e', '--enddate',
                        help=help_text["enddate"],
                        required=False,
                        type=str
                        )
    parser.add_argument('-u', '--update',
                        action='store_true',
                        help=help_text["update"],
                        )
    parser.add_argument('-p', '--plot',
                        action='store_true',
                        help=help_text["update"],
                        )
    parser.add_argument('-j', '--json',
                        action='store_true',
                        help=help_text["json"],
                        )
    args = parser.parse_args()

    if not args.plot and not args.update:
        print('either -u or -p or both must be selected')
        pprint.pprint(help_text)
        sys.exit()

    # set file names
    loggerhead_indx = 'loggerhead_indx.csv'  # anomaly and indicator file
    indicator_png = 'indicator_latest.png'  # indicator plot
    json_file = 'web_data.json'

    # set dataset urls
    base_url = 'https://coastwatch.pfeg.noaa.gov/erddap/griddap/{}'
    opendap_url = base_url.format('jplMURSST41anommday')
    opendapsst_url = base_url.format('jplMURSST41')

    if args.update:
        # read stored index data
        df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)
        df.drop(df.tail(1).index, inplace=True)
        index_time = df['dateyrmo'].values

        edt = netCDF4.Dataset(opendap_url, 'r')
        edt_time = edt['time'][:]
        ds_lat = np.array(edt.variables['latitude'][:])
        ds_lon = np.array(edt.variables['longitude'][:])
        # edt.close()
        edt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                           for ln in edt_time
                           ]

        edt_time_str_16 = ['{0:%Y-%m}'.format(ln) for ln in edt_time_obj_16]

        edt_time_set1 = set(edt_time_str_16)
        index_time_set2 = set(index_time)

        missing = list(sorted(edt_time_set1 - index_time_set2))
        if len(missing) == 0:
            print('no new data to process')
            sys.exit()

        indx_missing = sorted(missing)
        both = set(edt_time_str_16).intersection(indx_missing)
        indices_A = [edt_time_str_16.index(x) for x in both]
        indices_A.sort()

        edsst = netCDF4.Dataset(opendapsst_url, 'r')
        edstt_time = edt['time'][:]
        edstt_time_obj_16 = [datetime.fromtimestamp(ln).astimezone(timezone.utc)
                             for ln in edstt_time
                             ]
        edstt_time_obj_16 = [ln.replace(hour=0,
                                        minute=0,
                                        second=0,
                                        microsecond=0
                                        )
                             for ln in edstt_time_obj_16
                             ]

        for indx in indices_A:
            edt_anom = edt['sstAnom'][indx,
                                     latidx_range[0]:latidx_range[1], 
                                     lonidx_range[0]:lonidx_range[1]
                                     ]
            sst_indx = edstt_time_obj_16.index(edt_time_obj_16[indx])
    
            edsst_sst = edsst['analysed_sst'][sst_indx,
                                     latidx_range[0]:latidx_range[1], 
                                     lonidx_range[0]:lonidx_range[1]
                                     ]

            df.loc[len(df.index)] = [round(edt_anom.mean(), 2),
                                     round(df['anom'].iloc[len(df.index)-7:
                                                           len(df.index)-1
                                                           ].mean(), 2), 
                                     '{0:%Y-%m}'.format(edt_time_obj_16[indx])
                                     ]

        #df.sort_values(by=['date16'], ignore_index=True, inplace=True)
        df.sort_values(by=['dateyrmo'], ignore_index=True, inplace=True)

        df.loc[len(df.index)] = [0,
                                 round(df['anom'].iloc[len(df.index)-7:
                                                       len(df.index)-1
                                                       ].mean(), 2),
                                 '{0:%Y-%m}'.format(parse(df.loc[len(df.index)-1]
                                                          ['dateyrmo']
                                                          ) + timedelta(days=30))
                                 ]

        df.to_csv(loggerhead_indx, index=False, encoding='utf-8')

    # new plot prep
    if args.plot:
        indx_df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        # Convert mm/01/YYYY column to date
        indx_df['dateyrmo'] =  pd.to_datetime(indx_df['dateyrmo'],
                                      format='%Y-%m')

        # pull indicator data and create list to use for ggplot
        indx_list = indx_df['indicator'].tolist()
        indx_list2 = [ False if x < 0.77 else True for x in indx_list]
        indx_tf = [ "black" if x < 0.77 else "red" for x in indx_list]

        # Pull last date and create YTD range list for ggplot
        if args.end is not None:
            end_time = indx_df['dateyrmo'].max()
        else:
            end_time = parse(args.end).replace(day=1)

        start_time = end_time - timedelta(days=365+25)
        print(start_time)
        start_time = start_time.replace(day=1)
        print(start_time)
        t_range2 = [start_time, end_time]
        
        plot_index(indx_df, indicator_png, WORK_DIR)

        myCmd = ' '.join(['scp', os.path.join(WORK_DIR, indicator_png), cw_dir])
        print('copy index image to coastwatch', subprocess.call(myCmd, shell=True))

        myCmd = ' '.join(['mv', 
                          os.path.join(WORK_DIR, indicator_png), 
                          os.path.join(IMG_DIR, indicator_png)
                          ])
        print('mv index image to archive', subprocess.call(myCmd, shell=True))

        if end_time.month == 12:
            yearly_plot = "indicator_" + str(end_time.year) + ".png"
            shutil.copyfile(WORK_DIR, indicator_png), os.path.join(WORK_DIR, yearly_plot)
            
        indx_df.close()

    if args.json:
        indx_df = pd.read_csv(os.path.join(RES_DIR, loggerhead_indx))
        latest_index = indx_df['indicator'].iloc[-1]
        # alert status
        if (latest_index >= 0.77):
            alert_status = "Alert"
        else:
            alert_status = "No Alert"
        # Dates
        forecast_date = end_time.strftime("%B, %Y")
        update_date = datetime.now().strftime("%d %b, %Y")
        latest_index = str(latest_index)

        web_data = {
                    'alert': alert_status,
                    'fc_date': forecast_date,
                    'update_date': update_date,
                    'new_index': latest_index
                    }

        # web infor saved to json file
        with open(os.path.join(WORK_DIR, json_file), 'w') as outfile:
            json.dump(web_data, outfile)

        print('json file saved')
        indx_df.close()

            
        myCmd = ' '.join(['scp', os.path.join(WORK_DIR, json_file), cw_dir])
        print('copy json to coastwatch', subprocess.call(myCmd, shell=True))

        myCmd = ' '.join(['mv', 
                          os.path.join(WORK_DIR, json_file), 
                          os.path.join(JSON_DIR, json_file)
                          ])
        print('mv json to coastwatch', subprocess.call(myCmd, shell=True))

if __name__ == "__main__":
    main()