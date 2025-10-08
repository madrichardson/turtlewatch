
"""
update_ltca_closure.py

Overview
--------
Checks the Federal Register API for new Pacific Loggerhead Conservation Area closure notices
published by NOAA/NMFS and updates the local dataset (`data/resources/ltca_closure.csv`)
accordingly.

Each entry in the CSV includes:
- Year of the closure notice (`year`)
- Start date of closure (`start`)
- End date of closure (`end`)
- Federal Register link path (`cl_link`)

If new closure notices are detected (based on missing URLs), they are appended to the CSV file.
This ensures the Quarto-based Data Dashboard remains current without manual edits.

Usage
-----
Run manually or as part of a scheduled GitHub Actions workflow:
    python scripts/update_ltca_closure.py

Description
-----------
1. Loads existing closure records from `data/resources/ltca_closure.csv`.
2. Fetches new closure notices using the Federal Register API.
3. Filters for records mentioning “loggerhead” or “highly migratory”.
4. Merges new entries with existing ones and writes updated results to CSV.

Dependencies
------------
- Python ≥ 3.8
- requests

Directory Structure
-------------------
project_root/
├── scripts/
│   ├── control_total_data_2025.py
│   ├── plot_total_tool_2025.py
│   └── update_ltca_closure.py        ← this script
└── data/
    └── resources/
        └── ltca_closure.csv          ← closure dataset
"""

import csv
import json
import requests
from datetime import datetime
from pathlib import Path

# === Constants ===
CSV_PATH = Path("data/resources/ltca_closure.csv")
API_URL = (
    "https://www.federalregister.gov/api/v1/documents.json?"
    "conditions[term]=highly+migratory+species+fishery+closure&"
    "conditions[publication_date][gte]=2010-01-01&order=newest"
)

def load_existing_records():
    """
    Load existing closure records from the local CSV file.

    Returns
    -------
    list of dict
        A list containing one dictionary per closure record with keys:
        `year`, `start`, `end`, and `cl_link`.

    Notes
    -----
    - Returns an empty list if the CSV file does not exist.
    - Ensures UTF-8 encoding to support all special characters.
    """
    if not CSV_PATH.exists():
        return []
    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def get_new_closures():
    """
    Query the Federal Register API for new closure notices.

    Returns
    -------
    list of dict
        A list of newly detected closures with fields:
        - `year`: Publication year of the closure
        - `start`: Placeholder (future enhancement)
        - `end`: Placeholder (future enhancement)
        - `cl_link`: Relative URL path on federalregister.gov

    Description
    -----------
    1. Sends a GET request to the Federal Register API.
    2. Extracts document title, publication date, and URL.
    3. Filters for closure notices containing “loggerhead” or “highly migratory”.
    4. Standardizes results for appending to the closure CSV.

    Raises
    ------
    requests.exceptions.RequestException
        If the API request fails or the response is invalid.
    """
    resp = requests.get(API_URL)
    resp.raise_for_status()
    docs = resp.json().get("results", [])

    closures = []
    for d in docs:
        title = d.get("title", "").lower()
        url = d.get("html_url", "")
        pub_date = d.get("publication_date")
        year = pub_date.split("-")[0] if pub_date else None

        # Filter for relevant closure notices
        if "loggerhead" in title or "highly migratory" in title:
            closures.append({
                "year": year,
                "start": "",  # placeholder for extracted start date
                "end": "",    # placeholder for extracted end date
                "cl_link": url.replace("https://www.federalregister.gov/", "")
            })
    return closures

def merge_records(existing, new):
    """
    Merge newly fetched closure records with existing ones.

    Parameters
    ----------
    existing : list of dict
        Existing closure records loaded from CSV.
    new : list of dict
        Newly fetched records from the Federal Register API.

    Returns
    -------
    list of dict
        Combined list of existing and new closure entries.

    Notes
    -----
    - Compares closures using the `cl_link` field.
    - Prints a message for each newly added record.
    """
    existing_links = {r["cl_link"] for r in existing}
    merged = existing.copy()

    for n in new:
        if n["cl_link"] not in existing_links:
            print(f"Adding new closure: {n['cl_link']}")
            merged.append(n)

    return merged

def save_records(records):
    """
    Save closure records to CSV in standardized format.

    Parameters
    ----------
    records : list of dict
        All closure records to write.

    Notes
    -----
    - Overwrites the existing file.
    - Writes using UTF-8 encoding.
    - Ensures column order consistency.
    """
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["year", "start", "end", "cl_link"])
        writer.writeheader()
        writer.writerows(records)

def main():
    """
    Main driver function.
    ---------------------
    Executes the full closure update workflow:
    1. Loads existing records.
    2. Fetches new closure notices from the Federal Register API.
    3. Merges new and existing data.
    4. Saves the updated CSV file.

    Prints progress updates and completion summary to stdout.
    """
    existing = load_existing_records()
    new = get_new_closures()
    merged = merge_records(existing, new)
    save_records(merged)
    print(f"Closure CSV updated ({len(merged)} total records)")

# === Script Entry Point ===
if __name__ == "__main__":
    main()
