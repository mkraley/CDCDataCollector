# Google Sheets Integration Setup

This guide explains how to set up Google Sheets integration to automatically update a shared spreadsheet with publishing results.

## Prerequisites

1. A Google account with access to Google Cloud Console
2. A Google Sheet that you want to update
3. Python packages (will be installed with `pip install -r requirements.txt`)

**Important Note**: Even if your Google Sheet is set to "Anyone with the link can edit", you still need service account credentials. The Google Sheets API requires authentication for write operations, regardless of the sheet's sharing settings. If you don't provide credentials, the script will simply skip updating the Google Sheet and continue with other operations.

## Setup Steps

### 1. Create a Google Cloud Project and Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Enable the Google Sheets API:
   - Navigate to "APIs & Services" > "Library"
   - Search for "Google Sheets API"
   - Click "Enable"

4. Create a Service Account:
   - Go to "APIs & Services" > "Credentials"
   - Click "Create Credentials" > "Service Account"
   - Give it a name (e.g., "datalumos-automation")
   - Click "Create and Continue"
   - Skip the optional steps and click "Done"

5. Create a Key for the Service Account:
   - Click on the service account you just created
   - Go to the "Keys" tab
   - Click "Add Key" > "Create new key"
   - Choose "JSON" format
   - Download the JSON file and save it securely (e.g., `google-credentials.json`)

### 2. Share Your Google Sheet

1. Open your Google Sheet
2. Click the "Share" button
3. Add the service account email (found in the JSON file as `client_email`)
   - Example: `datalumos-automation@your-project.iam.gserviceaccount.com`
4. Give it "Editor" permissions
5. Click "Send"

### 3. Get Your Sheet ID

From your Google Sheet URL:
```
https://docs.google.com/spreadsheets/d/SHEET_ID_HERE/edit
```

Copy the `SHEET_ID_HERE` part - this is your Sheet ID.

### 4. Get Your Sheet ID

From your Google Sheet URL:
```
https://docs.google.com/spreadsheets/d/1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY/edit?gid=101637367
```

The Sheet ID is: `1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY`

The tab name is determined by the `gid` parameter or defaults to "CDC".

### 5. Run the Script with Google Sheets Options

```powershell
python chiara_upload.py \
  --csv "your_file.csv" \
  --start-row 1 \
  --end-row 10 \
  --google-sheet-id "1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY" \
  --google-credentials "path/to/google-credentials.json" \
  --google-sheet-name "CDC" \
  --google-username "mkraley"
```

## Command-Line Arguments

- `--google-sheet-id`: Required. The Google Sheet ID from the URL (default: `1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY`)
- `--google-credentials`: Required. Path to the service account JSON file
- `--google-sheet-name`: Optional. Name of the worksheet/tab (default: "CDC")
- `--google-username`: Optional. Username to write in "Claimed" column (default: "mkraley")

## How It Works

- After successful publishing, the script searches for the row in the Google Sheet by matching the source URL (from CSV column `7_original_distribution_url`) against column F (URL) in the sheet
- If a matching row is found, it updates the following columns:
  - **Column A**: Claimed (add your name) - writes the username
  - **Column B**: Data Added (Y/N/IP) - writes "Y"
  - **Column G**: Dataset Download Possible? - writes "Y"
  - **Column I**: Nominated to EOT / USGWDA - writes "Y"
  - **Column J**: Date Downloaded - writes value from CSV `12_download_date_original_source`
  - **Column K**: Download Location - writes `https://www.datalumos.org/datalumos/project/{workspace_id}/version/V1/view`
  - **Column L**: Dataset Size - writes value from CSV `dataset_size` (if available)
  - **Column M**: File extensions of data uploads - writes value from CSV `file_extensions` (if available)
  - **Column N**: Metadata availability info - writes "Y"
- If no matching row is found, an error is recorded and the script continues to the next workspace
- Errors are logged but don't stop the script execution

## Troubleshooting

**Do I need credentials if the sheet is public?**
- Yes, you still need service account credentials even if the sheet is set to "Anyone with the link can edit"
- The Google Sheets API requires authentication for write operations, regardless of sharing settings
- If you don't want to use Google Sheets updates, simply omit the `--google-credentials` argument and the script will skip that step

**Error: "Credentials file not found"**
- Check that the path to your JSON credentials file is correct
- Use absolute paths if relative paths don't work
- If you don't want to update Google Sheets, you can omit the `--google-credentials` argument

**Error: "Google Sheets API error: 403"**
- Make sure you've shared the sheet with the service account email
- Verify the service account has "Editor" permissions
- Even if the sheet is publicly editable, the service account still needs to be explicitly shared with it

**Error: "Google Sheets API error: 400"**
- Check that the sheet name (tab name) is correct (default: "CDC")
- Verify the URL in your CSV matches the URL in column F of the sheet
- URLs are matched case-insensitively and can be partial matches

**Error: "Could not find row with matching URL"**
- Verify the source URL in your CSV (column `7_original_distribution_url`) exists in column F of the Google Sheet
- Check for URL formatting differences (trailing slashes, http vs https, etc.)
- The script tries to match URLs flexibly (exact match, contains, or contained in)

**Import errors when running the script**
- Install the required packages: `pip install -r requirements.txt`
- Make sure you're in the correct virtual environment

## Security Notes

- **Never commit your credentials JSON file to version control**
- Add `google-credentials.json` (or similar) to your `.gitignore`
- Store credentials in a secure location
- Consider using environment variables for sensitive paths in production

