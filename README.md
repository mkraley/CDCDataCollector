# CDC Data Collector

A desktop Python application for Windows that processes Excel files with filtering criteria.

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

The application processes Excel files and filters rows based on the following criteria:
- Column B is blank
- Column L is blank
- Column G starts with 'https://data.cdc.gov'

### Basic Usage

Process the default input file (first 10 eligible rows):
```powershell
python main.py --start-row 0 --num-rows 10
```

Process all eligible rows:
```powershell
python main.py
```

Process rows 50-99 (50 rows starting from index 50):
```powershell
python main.py --start-row 50 --num-rows 50
```

### Command-Line Options

- `--input`: Path to input Excel file (default: `C:\Documents\DataRescue\Data_Inventories - cdc.xlsx`)
- `--start-row`: First eligible row to process, 0-indexed (default: 0)
- `--num-rows`: Number of eligible rows to process (default: all remaining)
- `--output`: Optional output file path to save results to Excel file

### Examples

Process first 20 rows and save to output file:
```powershell
python main.py --start-row 0 --num-rows 20 --output output.xlsx
```

Process with custom input file:
```powershell
python main.py --input "path\to\your\file.xlsx" --start-row 0 --num-rows 100
```

## Development

This project uses Python and is configured for Windows development.

