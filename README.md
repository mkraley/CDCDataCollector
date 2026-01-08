# CDC Data Collector

A collection of Python programs for collecting CDC datasets and publishing them to DataLumos.

Note that this code is written by Cursor/Claude. This is very convenient for the author, but does result in somewhat less than elegantly structured code.

## Overview

This repository contains three independent programs:

1. **`collector.py`** - Downloads datasets and landing pages from their original sources (CDC data.cdc.gov). The datasets to download are specified in an Excel spreadsheet which is derived from the DRP Data_Inventories google sheet. (We use a copy rather than the original for historical reasons.) We are only taking files from data.cdc.gov (because these all appear to be formatted the same), and which are unclaimed and do not have a Download location (ie. have not previously been uploaded to DataLumos).

   For each selected row in the source datasheet, information is collected from that row. A temporary data folder is created where the files to be uploaded are stored.
   The collector reads the source landing page and parses it to obtain some metadata, e.g. the summary, keywords, etc, using playwright. It expands "read more" links and maniuplates some paging functionality so that all data columns are exposed. Then it converts that snapshow to a PDF which is saved in the data folder. 

   The collector then clicks the necessary buttons to export the associated data set, which is also stored in the data folder.

   The results are stored in a csv file which is formatted to be used as input to chiara_upload.py

2. **`chiara_upload.py`** - Uploads and publishes the collected data to DataLumos. Much of this code was based on a program originally written by @chiara. This program automates the form-filling process on the DataLumos workspace. In this case, follwoing the original code by @chiara, we use selenium instead of playwright. The program is driven by a CSV file output by the collector.

   First the automated browser attempts to sign in to DataLumos. Then, for each row in the csv file, a new DataLumos project is created. Then all the fields are filled in, using the appropriate cells in the csv file. This is all done via Selenium, as there is no API for DataLumos. The collected files are also submitted for upload to the project form. 

   The original source URL is submitted to the US Government Web & Data Archive (https://digital2.library.unt.edu/nomination/GWDA-US-2025/add/).

   Finally, the actual Data_Inventories google sheet is updated to indicate successful uploading including the URL of the newly created DataLumos project.

3. **`playwright_upload.py`** - An incomplete experiment in using Playwright to automate the uploading task. This approach was abandoned in favor of using @chiara's code (which uses Selenium).

## Setup

1. Create and activate a virtual environment:
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```

## Usage

### collector.py (Data Collector)

Process the default input file (first 10 eligible rows):
```powershell
python collector.py --start-row 0 --num-rows 10
```

Process all eligible rows:
```powershell
python collector.py
```

Process rows 50-99 (50 rows starting from index 50):
```powershell
python collector.py --start-row 50 --num-rows 50
```

#### Command-Line Options

- `--input`: Path to input Excel file (default: `C:\Documents\DataRescue\Data_Inventories - cdc.xlsx`)
- `--start-row`: First eligible row to process, 0-indexed (default: 0)
- `--num-rows`: Number of eligible rows to process (default: all remaining)
- `--output`: Output file path to save results to Excel file
- `--headless`: Run browser in visible mode for debugging (default: False)

#### Examples

Process first 20 rows and save to output file:
```powershell
python collector.py --start-row 0 --num-rows 20 --output output.xlsx
```

Process with custom input file:
```powershell
python collector.py --input "path\to\your\file.xlsx" --start-row 0 --num-rows 100
```

### chiara_upload.py (DataLumos Uploader)

This script automatically fills in DataLumos fields from a CSV file (exported spreadsheet) and uploads files. Login, checking, and publishing are done manually to avoid errors.

See GOOGLE_SHEETS_SETUP.md for instructions needed to update the google sheet.

#### Command-Line Options

**Required:**
- `--csv` or `--csv-file-path`: Path to the CSV file containing the data to upload
- `--start-row`: Starting row number (counting starts at 1, excluding header row)
- `--end-row`: Ending row number (to process only one row, set start-row and end-row to the same number)

**Optional:**
- `--folder` or `--folder-path-uploadfiles`: Path to the folder where upload files are located (subfolders for each project should be in here)
- `--username`: Username/email for automated login to DataLumos (if not provided, manual login will be required)
- `--password`: Password for automated login to DataLumos (if not provided, manual login will be required)
- `--browser`: Browser to use: `chrome`, `chromium`, or `firefox` (default: `chrome`)
- `--verbose`: Enable verbose logging (default: one line per asset with summary)
- `--publish-mode`: Publishing mode: `default` (run all steps including publish), `no-publish` (skip publishing), or `only-publish` (only publish, skip form-filling) (default: `default`)
- `--google-sheet-id`: Google Sheet ID from the URL (default: CDC Data Inventories sheet)
- `--google-credentials`: Path to Google service account credentials JSON file (required for Google Sheets updates) (See GOOGLE_SHEETS_SETUP.md)
- `--google-sheet-name`: Name of the worksheet/tab to update (default: `CDC`)
- `--google-username`: Username to write in the "Claimed" column (default: `mkraley`)
- `--GWDA-your-name`: Name to enter in GWDA nomination form (default: `Michael Kraley`)
- `--GWDA-institution`: Institution to enter in GWDA nomination form (default: `Data Rescue Project`)
- `--GWDA-email`: Email to enter in GWDA nomination form (default: uses `--username` value if provided)

#### Examples

Process rows 1-5 with automated login:
```powershell
python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --username "user@example.com" --password "pass123" --folder "C:\data"
```

Process rows 1-5 with manual login:
```powershell
python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --folder "C:\data"
```

Process with Firefox browser:
```powershell
python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --browser firefox --username "user@example.com" --password "pass123"
```

### playwright_upload.py (Playwright Experiment - Incomplete)

This is an abandoned experiment using Playwright for automation. It is not recommended for use. The project uses `chiara_upload.py` instead.

## Development

This project uses Python and is configured for Windows development.
