import os
import pandas as pd
from pathlib import Path

# Paths
base_folder = r"C:\Documents\DataRescue\CDC data"
csv_file = r"C:\Documents\DataRescue\CDCCollectedData - Copy (2).csv"
log_file = "missing_file_search_results.log"

# Read the CSV file
print(f"Reading CSV file: {csv_file}")
df = pd.read_csv(csv_file)

# Check if required columns exist
if 'path' not in df.columns:
    print(f"Error: 'path' column not found in CSV. Available columns: {df.columns.tolist()}")
    exit(1)
if '7_original_distribution_url' not in df.columns:
    print(f"Error: '7_original_distribution_url' column not found in CSV. Available columns: {df.columns.tolist()}")
    exit(1)
if 'datalumos_id' not in df.columns:
    print(f"Error: 'datalumos_id' column not found in CSV. Available columns: {df.columns.tolist()}")
    exit(1)

# Create dictionaries mapping paths to URLs and datalumos_id for quick lookup
path_to_url = dict(zip(df['path'], df['7_original_distribution_url']))
path_to_datalumos_id = dict(zip(df['path'], df['datalumos_id']))

print(f"Loaded {len(path_to_url)} path-URL mappings from CSV")
print(f"Loaded {len(path_to_datalumos_id)} path-datalumos_id mappings from CSV")

# Check base folder exists
if not os.path.exists(base_folder):
    print(f"Error: Base folder does not exist: {base_folder}")
    exit(1)

# Get all subfolders
print(f"\nScanning subfolders in: {base_folder}")
subfolders = [d for d in os.listdir(base_folder) 
              if os.path.isdir(os.path.join(base_folder, d))]

print(f"Found {len(subfolders)} subfolders")

# Analyze each subfolder
results = []

for folder_name in subfolders:
    folder_path = os.path.join(base_folder, folder_name)
    
    # Count files in the folder
    files = [f for f in os.listdir(folder_path)]
             #if os.path.isfile(os.path.join(folder_path, f))]
    file_count = len(files)
    
    # Check if it has exactly 2 files
    if file_count != 2:
        # Look up the URL and datalumos_id in the CSV
        # Try to match the folder path
        folder_path_for_match = folder_path
        url = path_to_url.get(folder_path_for_match, "NOT FOUND")
        datalumos_id = path_to_datalumos_id.get(folder_path_for_match, "NOT FOUND")
        
        # If not found, try just the folder name or other variations
        if url == "NOT FOUND":
            # Try with forward slashes
            folder_path_slash = folder_path.replace('\\', '/')
            url = path_to_url.get(folder_path_slash, "NOT FOUND")
            datalumos_id = path_to_datalumos_id.get(folder_path_slash, "NOT FOUND")
        
        # Format datalumos_id - handle NaN and convert to string
        if pd.isna(datalumos_id) or datalumos_id == "NOT FOUND":
            datalumos_id_str = "N/A" if datalumos_id != "NOT FOUND" else "NOT FOUND"
        else:
            # Convert float to int, then to string
            datalumos_id_str = str(int(datalumos_id))
        
        results.append({
            'folder_name': folder_name,
            'folder_path': folder_path,
            'file_count': file_count,
            'url': url,
            'datalumos_id': datalumos_id_str
        })

print(f"\nFound {len(results)} subfolders with issues (not exactly 2 files)")

# Write results to log file
print(f"\nWriting results to: {log_file}")
with open(log_file, 'w', encoding='utf-8') as f:
    f.write("Folder Name\tNumber of Files\tURL\tdatalumos_id\n")
    f.write("-" * 100 + "\n")
    
    for result in results:
        line = f"{result['folder_name']}\t{result['file_count']}\t{result['url']}\t{result['datalumos_id']}\n"
        f.write(line)
        print(f"  {result['folder_name']}: {result['file_count']} files, URL: {result['url']}, datalumos_id: {result['datalumos_id']}")

print(f"\nResults written to: {log_file}")
print(f"Total issues found: {len(results)}")

