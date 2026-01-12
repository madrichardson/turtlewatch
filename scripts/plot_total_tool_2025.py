#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Plot the TOTAL index.

This script generates time-series plots of the TOTAL indicator index over
the past year, highlighting periods when the indicator exceeds a specified
alert threshold (0.77). The resulting plot is saved locally and optionally
sent to an ERDDAP server for visualization. If the specified end date is
December, a yearly summary plot is also created and uploaded.

Example:
    Run from the command line to plot the most recent year of data:

        $ python plot_total_index.py

    Or specify a custom end date:

        $ python plot_total_index.py -e 2024-06
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import warnings
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import plotnine as p9
from dateutil.parser import parse

warnings.filterwarnings("ignore")

__author__ = "Dale Robinson"
__credits__ = ["Dale Robinson"]
__license__ = "GPL"
__version__ = "2.1"
__maintainer__ = "Dale Robinson"
__email__ = "dale.Robinson@noaa.gov"
__status__ = "Production"


def send_to_erddap(work_dir: Path, infile: str, erddap_path: Path, ofile: str) -> None:
    """Transfer a file to a remote ERDDAP directory using SCP.

    Constructs and executes a secure copy (SCP) command to transfer a file
    from a local working directory to a remote ERDDAP server.

    Args:
        work_dir (Path): Path to the local directory containing the file.
        infile (str): Name of the local file to send.
        erddap_path (Path): Remote directory path on the ERDDAP server.
        ofile (str): Desired name of the file on the ERDDAP server.

    Returns:
        None

    Raises:
        subprocess.CalledProcessError: If the SCP command fails.

    Example:
        >>> send_to_erddap(
        ...     work_dir=Path("/home/cwatch/production/turtles/maps"),
        ...     infile="indicator_latest.png",
        ...     erddap_path=Path("/var/www/html/elnino/dash"),
        ...     ofile="indicator_latest.png"
        ... )
    """
    cmd = [
        "scp",
        str(work_dir / infile),
        f"cwatch@192.168.31.15:{erddap_path / ofile}",
    ]
    print(" ".join(cmd))
    result = subprocess.call(cmd)
    print("Send to ERDDAP:", ofile, "return code:", result)
    if result != 0:
        raise subprocess.CalledProcessError(result, cmd)


def plot_index(
    my_data: pd.DataFrame, png_name: str, png_dir: Path, t_range: list[pd.Timestamp]
) -> int:
    """Generate and save a 12-month TOTAL indicator time-series plot.

    Creates a plot of the TOTAL indicator values over a one-year period,
    highlighting months where the indicator exceeds the alert threshold (0.77).
    The plot is saved as a PNG file to the specified output directory.

    Args:
        my_data (pd.DataFrame):
            A DataFrame containing at least two columns:
                - 'dateyrmo': datetime-like monthly timestamps.
                - 'indicator': float indicator values.
        png_name (str): Name of the output PNG file (e.g., "indicator_latest.png").
        png_dir (Path): Directory path where the PNG file will be saved.
        t_range (list[pd.Timestamp]): Two-element list defining the start
            and end date limits for the plot’s x-axis.

    Returns:
        int: Returns 0 on successful plot creation and save.

    Raises:
        ValueError: If `my_data` is missing required columns.
        OSError: If there is a problem saving the PNG file.

    Example:
        >>> df = pd.DataFrame({
        ...     'dateyrmo': pd.date_range('2023-01-01', periods=12, freq='M'),
        ...     'indicator': [0.5, 0.6, 0.8, 0.9, 0.75, 0.78,
        ...                   0.82, 0.7, 0.68, 0.9, 0.85, 0.73]
        ... })
        >>> plot_index(df, "indicator_latest.png", Path("/tmp"),
        ...             [df['dateyrmo'].min(), df['dateyrmo'].max()])
        0
    """
    required_cols = {"indicator", "dateyrmo"}
    if not required_cols.issubset(my_data.columns):
        raise ValueError("DataFrame must contain 'indicator' and 'dateyrmo' columns.")

    # Determine color for each point (alert threshold = 0.77)
    indx_tf = ["black" if x < 0.77 else "red" for x in my_data["indicator"]]

    fig = (
        p9.ggplot(data=my_data, mapping=p9.aes(x="dateyrmo", y="indicator"))
        + p9.geom_point(p9.aes(color=indx_tf), size=4)
        + p9.labs(x="", y="Indicator")
        + p9.theme_light()
        + p9.scale_x_date(limits=t_range, date_labels="%m/%Y")
        + p9.geom_hline(yintercept=0.77, linetype="solid", color="red", size=1)
        + p9.theme(text=p9.element_text(size=16),
                   axis_text_x=p9.element_text(angle=60, hjust=1))
        + p9.scale_color_manual(name="Status",
                                values=["black", "red"],
                                labels=["No Alert", "Alert"])
        + p9.ylim(-0.5, 1.5)
    )

    p9.ggsave(
        fig,
        filename=str(png_dir / png_name),
        dpi=72,
        height=4.5,
        width=9,
        units="in",
    )
    return 0


def main() -> None:
    """Main execution function for generating and publishing the TOTAL index plots.

    Loads the TOTAL indicator CSV dataset, generates a one-year plot up to
    the most recent or user-specified month, and saves and uploads the plot
    to the ERDDAP server. If the end month is December, a yearly summary
    plot is also created.

    Command-line Arguments:
        -e, --enddate (Optional[str]): End date in 'YYYY-mm' format.
            If not provided, the most recent available month in the dataset is used.

    Returns:
        None

    Raises:
        TypeError: If the provided `--enddate` does not match the format 'YYYY-mm'.
        FileNotFoundError: If the indicator CSV file cannot be found.
        subprocess.CalledProcessError: If the SCP upload fails.

    Example:
        Run with default (latest date):
            $ python plot_total_index.py

        Run with specific end date:
            $ python plot_total_index.py -e 2024-12
    """
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-e",
        "--enddate",
        help="End date in 'YYYY-mm' format. Default is the most recent available date.",
        required=False,
        default=None,
        type=str,
    )
    args = parser.parse_args()

    if args.enddate and len(args.enddate) != 7:
        raise TypeError("-e / --enddate must be in the form YYYY-mm")

    # Define paths
    base_dir = Path.cwd()
    #work_dir = base_dir / "data" / "work"

    res_dir = base_dir / "data" / "resources"
    csv_file = res_dir / "loggerhead_indx.csv"

    results_dir = base_dir / "data" / "images"

    erddap_dir = base_dir / "data" / "upload"

    indicator_png = "indicator_latest.png"

    if not csv_file.exists():
        raise FileNotFoundError(f"Indicator CSV file not found: {csv_file}")

    # Load data
    indx_df = pd.read_csv(csv_file)
    indx_df["dateyrmo"] = pd.to_datetime(indx_df["dateyrmo"], format="%Y-%m")

    # Determine plotting range
    end_time = parse(args.enddate) if args.enddate else indx_df["dateyrmo"].max()
    start_time = (end_time - timedelta(days=390)).replace(day=1)
    end_time = end_time.replace(day=1)
    time_range = [start_time, end_time]

    print(f"Plot time range: {start_time:%Y-%m} to {end_time:%Y-%m}")

    # Plot and save
    plot_index(indx_df, indicator_png, results_dir, time_range)
    #shutil.copyfile(work_dir / indicator_png, results_dir / indicator_png)
    #send_to_erddap(results_dir, indicator_png, erddap_dir, indicator_png)

    # Generate yearly summary if December
    if end_time.month == 12:
        yearly_plot = f"indicator_{end_time.year}.png"
        results_dir.joinpath(indicator_png).replace(results_dir / yearly_plot)
        if "GITHUB_ACTIONS" not in os.environ:
            #shutil.copyfile(results_dir / indicator_png, results_dir / yearly_plot)
            send_to_erddap(results_dir, yearly_plot, erddap_dir, yearly_plot)


if __name__ == "__main__":
    main()
