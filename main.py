"""
CDC Data Collector
A desktop Python application for Windows
Processes Excel files to extract rows matching specific criteria and collects data from URLs
"""

import pandas as pd
import argparse
import sys
import requests
from pathlib import Path
from datetime import datetime
import time
import re
import os
import shutil
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


def find_column(df, column_names):
    """
    Find a column in DataFrame by trying multiple possible names (case-insensitive partial match)
    
    Args:
        df: DataFrame to search
        column_names: List of possible column names to search for
    
    Returns:
        Column name if found, None otherwise
    """
    df_cols_lower = {col.lower(): col for col in df.columns}
    for name in column_names:
        name_lower = name.lower()
        for col_lower, col in df_cols_lower.items():
            if name_lower in col_lower or col_lower in name_lower:
                return col
    return None


def get_filtered_rows(source_file):
    """
    Get filtered rows from source Excel file based on criteria:
    - Column B is blank
    - Column L is blank
    - Column G starts with 'https://data.cdc.gov'
    
    Args:
        source_file: Path to source Excel file
    
    Returns:
        DataFrame with filtered rows and their original indices
    """
    print(f"Reading source sheet: {source_file}")
    try:
        df = pd.read_excel(source_file)
    except FileNotFoundError:
        print(f"Error: File not found: {source_file}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        sys.exit(1)
    
    # Get column names (Excel columns are 1-indexed, pandas is 0-indexed)
    col_b = df.columns[1]  # Column B
    col_g = df.columns[6]  # Column G
    col_l = df.columns[11]  # Column L
    
    print(f"Total rows in source: {len(df)}")
    
    # Filter rows
    b_is_blank = df[col_b].isna() | (df[col_b].astype(str).str.strip() == '')
    l_is_blank = df[col_l].isna() | (df[col_l].astype(str).str.strip() == '')
    g_starts_with_cdc = df[col_g].astype(str).str.startswith('https://data.cdc.gov', na=False)
    
    filtered_df = df[b_is_blank & l_is_blank & g_starts_with_cdc].copy()
    filtered_df['_original_index'] = filtered_df.index
    
    print(f"Eligible rows after filtering: {len(filtered_df)}")
    
    return filtered_df, col_g


def sanitize_folder_name(name, max_length=100):
    """
    Sanitize a folder name to be valid for Windows filesystem
    
    Args:
        name: Original folder/file name
        max_length: Maximum length for the sanitized name (default: 100)
    
    Returns:
        Sanitized folder/file name
    """
    if not name:
        return "Untitled"
    
    # Remove invalid Windows characters: < > : " / \ | ? *
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', str(name))
    
    # Remove leading/trailing dots and spaces (Windows doesn't allow these)
    sanitized = sanitized.strip('. ')
    
    # Remove control characters
    sanitized = re.sub(r'[\x00-\x1f\x7f]', '', sanitized)
    
    # Limit length to avoid Windows path issues
    # Windows has 260 char path limit, so keep folder names shorter
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
    
    # If empty after sanitization, use default
    if not sanitized:
        sanitized = "Untitled"
    
    return sanitized


def create_title_folder(base_dir, title):
    """
    Create or reuse a folder named after the title and return the full path.
    If the folder already exists, clears all files in it.
    
    Args:
        base_dir: Base directory path
        title: Title to use for folder name
    
    Returns:
        Path object for the created/cleared folder, or None if creation failed
    """
    base_path = Path(base_dir)
    
    # Create base directory if it doesn't exist
    try:
        base_path.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"  ERROR: Could not create base directory: {e}")
        return None
    
    # Sanitize the title for folder name
    folder_name = sanitize_folder_name(title, max_length=120)
    folder_path = base_path / folder_name
    
    # If folder exists, clear all files in it
    if folder_path.exists() and folder_path.is_dir():
        try:
            # Remove all files and subdirectories
            for item in folder_path.iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    shutil.rmtree(item)
            print(f"  Cleared existing folder: {folder_path}")
        except Exception as e:
            print(f"  WARNING: Could not clear existing folder: {e}")
            # Try to continue anyway
    
    # Create the folder (or ensure it exists)
    try:
        folder_path.mkdir(parents=True, exist_ok=True)
        return folder_path
    except Exception as e:
        print(f"  ERROR: Could not create folder: {e}")
        return None


def access_url(url, timeout=30):
    """
    Try to access a URL and return status information and HTML content
    
    Args:
        url: URL to access
        timeout: Request timeout in seconds
    
    Returns:
        Tuple of (success: bool, status_message: str, status_code: int or None, html_content: str or None)
    """
    try:
        response = requests.get(url, timeout=timeout, allow_redirects=True)
        if response.status_code == 200:
            html_content = response.text
            return True, "Success", response.status_code, html_content
        else:
            return False, f"HTTP {response.status_code}", response.status_code, None
    except requests.exceptions.Timeout:
        return False, "Timeout", None, None
    except requests.exceptions.ConnectionError:
        return False, "Connection Error", None, None
    except requests.exceptions.TooManyRedirects:
        return False, "Too Many Redirects", None, None
    except requests.exceptions.RequestException as e:
        return False, f"Error: {str(e)}", None, None
    except Exception as e:
        return False, f"Unexpected Error: {str(e)}", None, None


def download_dataset(page, output_path, timeout=60000):
    """
    Download dataset by clicking Export button and then Download button in the dialog.
    Intercepts the download request to get the file.
    
    Args:
        page: Playwright page object
        output_path: Path object where the downloaded file should be saved
        timeout: Timeout in milliseconds (default: 60 seconds)
    
    Returns:
        Tuple of (success: bool, status_message: str)
    """
    try:
        # Find and click Export button first
        export_clicked = page.evaluate("""
            () => {
                const exportButtons = Array.from(document.querySelectorAll('button, a, [role="button"]')).filter(el => {
                    const text = (el.textContent || el.innerText || '').trim().toLowerCase();
                    return text.includes('export') && text.length < 50;
                });
                
                if (exportButtons.length === 0) {
                    return { success: false, message: 'Export button not found' };
                }
                
                try {
                    exportButtons[0].scrollIntoView({ behavior: 'auto', block: 'center' });
                    exportButtons[0].click();
                    return { success: true, message: 'Export button clicked' };
                } catch (e) {
                    return { success: false, message: 'Could not click Export button: ' + e.message };
                }
            }
        """)
        
        if not export_clicked.get('success'):
            return False, export_clicked.get('message', 'Could not find Export button')
        
        # Wait for dialog to appear
        page.wait_for_timeout(2000)
        
        # Set up download listener before clicking Download
        with page.expect_download(timeout=timeout) as download_info:
            # Find and click Download button in the dialog - look for exact text "Download"
            download_clicked = page.evaluate("""
                () => {
                    // Try to find Download button with exact label "Download"
                    let downloadButtons = Array.from(document.querySelectorAll('button, a, [role="button"]')).filter(el => {
                        const text = (el.textContent || el.innerText || '').trim();
                        return text === 'Download';
                    });
                    
                    // If not found, try looking in dialogs/modals
                    if (downloadButtons.length === 0) {
                        const dialogs = document.querySelectorAll('dialog, [role="dialog"], .modal, [class*="dialog"]');
                        for (const dialog of dialogs) {
                            const buttons = Array.from(dialog.querySelectorAll('button, a, [role="button"]'));
                            downloadButtons = buttons.filter(el => {
                                const text = (el.textContent || el.innerText || '').trim();
                                return text === 'Download';
                            });
                            if (downloadButtons.length > 0) break;
                        }
                    }
                    
                    if (downloadButtons.length === 0) {
                        return { success: false, message: 'Download button with exact label "Download" not found in dialog' };
                    }
                    
                    try {
                        downloadButtons[0].scrollIntoView({ behavior: 'auto', block: 'center' });
                        downloadButtons[0].click();
                        return { success: true, message: 'Download button clicked' };
                    } catch (e) {
                        return { success: false, message: 'Could not click Download button: ' + e.message };
                    }
                }
            """)
            
            if not download_clicked.get('success'):
                return False, download_clicked.get('message', 'Could not find Download button')
        
        # Wait for download to complete and save the file
        download = download_info.value
        download.save_as(output_path)
        
        return True, f"Dataset downloaded: {output_path.name}"
    
    except PlaywrightTimeoutError:
        return False, "Timeout waiting for download (check if Export/Download buttons work)"
    except Exception as e:
        return False, f"Error downloading dataset: {str(e)[:100]}"


def url_to_pdf(url, output_path, timeout=120000, headless=True):
    """
    Convert a URL to PDF using Playwright (headless browser)
    Expands "Read more" links before generating PDF to capture full content
    Changes "Rows per page" dropdown to 100 if available
    
    Args:
        url: URL to convert to PDF
        output_path: Path object where PDF should be saved
        timeout: Timeout in milliseconds (default: 120 seconds)
        headless: If False, run browser in visible mode for debugging (default: True)
    
    Returns:
        Tuple of (success: bool, status_message: str, total_rows: int or None)
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, slow_mo=500 if not headless else 0)
            page = browser.new_page()
            
            page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            page.wait_for_timeout(500)
            
            # Find and click "Read more" links/buttons to expand content
            expand_keywords = ['read more', 'show more', 'expand', 'see more', 'view more', 
                             'read full', 'show full', 'view full', 'continue reading']
            
            expand_js = """
            (keywords) => {
                const clicked = new Set();
                let count = 0;
                const maxClicks = 50;
                
                function findAndClick(keyword) {
                    const allElements = document.querySelectorAll('a, button, [role="button"], span, div');
                    
                    for (const el of allElements) {
                        if (count >= maxClicks) break;
                        
                        const text = (el.textContent || el.innerText || '').trim();
                        const textLower = text.toLowerCase();
                        
                        if (textLower.includes(keyword.toLowerCase()) && text.length < 100 && text.length > 0) {
                            const elId = el.tagName + '|' + (el.className || '') + '|' + text.substring(0, 50);
                            
                            if (!clicked.has(elId)) {
                                const rect = el.getBoundingClientRect();
                                const style = window.getComputedStyle(el);
                                
                                if (rect.width > 0 && rect.height > 0 && 
                                    style.display !== 'none' && style.visibility !== 'hidden' &&
                                    parseFloat(style.opacity) > 0) {
                                    
                                    try {
                                        el.scrollIntoView({ behavior: 'auto', block: 'center' });
                                        el.click();
                                        clicked.add(elId);
                                        count++;
                                    } catch (e) {
                                        // Element might not be clickable, skip
                                    }
                                }
                            }
                        }
                    }
                }
                
                for (const keyword of keywords) {
                    if (count >= maxClicks) break;
                    findAndClick(keyword);
                }
                
                return count;
            }
            """
            
            try:
                clicked_count = page.evaluate(expand_js, expand_keywords)
                if clicked_count > 0:
                    print(f"  Expanded {clicked_count} 'Read more' sections")
                    # page.wait_for_timeout(1500)
            except Exception as e:
                print(f"  Note: Could not expand 'Read more' links: {e}")
            
            # Change "Rows per page" to 100 to show more data
            total_rows = None
            rows_status_msg = None
            try:
                page.wait_for_timeout(500)
                
                set_rows_js = """
                () => {
                    try {
                        const fp = document.querySelector('forge-paginator');
                        if (!fp) {
                            return { success: false, message: 'forge-paginator not found', totalRows: null };
                        }
                        
                        const fs = fp.shadowRoot.querySelector('forge-select');
                        if (!fs) {
                            return { success: false, message: 'forge-select not found', totalRows: null };
                        }
                        
                        fs.value = '100';
                        fp.pageSize = 100;
                        
                        const changeEvent = new Event('change', { bubbles: true, cancelable: true });
                        fs.dispatchEvent(changeEvent);
                        
                        const paginatorChangeEvent = new CustomEvent('forge-paginator-change', {
                            bubbles: true,
                            cancelable: true,
                            detail: {
                                type: 'page-size',
                                pageSize: 100,
                                pageIndex: fp.pageIndex || 0,
                                offset: fp.offset || 0
                            }
                        });
                        fp.dispatchEvent(paginatorChangeEvent);
                        
                        return { success: true, message: 'Set to 100', totalRows: null };
                    } catch (e) {
                        return { success: false, message: 'Error: ' + e.message, totalRows: null };
                    }
                }
                """
                
                rows_result = page.evaluate(set_rows_js)
                
                if rows_result and rows_result.get('success'):
                    page.wait_for_timeout(500)
                    
                    read_total_js = """
                    () => {
                        try {
                            const fp = document.querySelector('forge-paginator');
                            if (!fp || !fp.shadowRoot) return null;
                            
                            const rangeLabel = fp.shadowRoot.querySelector('.range-label');
                            if (!rangeLabel) return null;
                            
                            let rangeText = (rangeLabel.textContent || rangeLabel.innerText || '').trim();
                            const slot = rangeLabel.querySelector('slot[name="range-label"]');
                            if (slot && slot.assignedNodes) {
                                const assigned = slot.assignedNodes();
                                if (assigned.length > 0) {
                                    rangeText = assigned.map(n => n.textContent || '').join(' ').trim();
                                }
                            }
                            
                            const match = rangeText.match(/of\\s+(\\d+)/i);
                            return match ? parseInt(match[1]) : null;
                        } catch (e) {
                            return null;
                        }
                    }
                    """
                    
                    total_rows = page.evaluate(read_total_js)
                    
                    if total_rows is not None:
                        print(f"  Set rows per page to 100. Total rows: {total_rows}")
                        if total_rows > 100:
                            rows_status_msg = f"Set to 100 (Note: {total_rows} total rows > 100)"
                            print(f"  WARNING: {total_rows} total rows exceeds 100, not all rows may be visible")
                        else:
                            rows_status_msg = "Set to 100"
                    else:
                        rows_status_msg = "Set to 100 (count unknown)"
                else:
                    rows_status_msg = rows_result.get('message', 'Could not change rows per page') if rows_result else 'Dropdown not found'
                    print(f"  Note: {rows_status_msg}")
                    total_rows = None
            except Exception as e:
                rows_status_msg = f"Error changing rows per page: {str(e)[:50]}"
                print(f"  Note: Could not change rows per page dropdown: {e}")
            
            # Generate PDF
            page.pdf(path=str(output_path), format='A4', print_background=True)
            
            # Close browser
            browser.close()
        
        # Build status message
        status_parts = []
        if rows_status_msg and 'Set to 100' in rows_status_msg:
            status_parts.append(rows_status_msg)
        
        status_message = "; ".join(status_parts) if status_parts else "PDF generated"
        
        return True, status_message, total_rows
    except Exception as e:
        error_msg = f"ERROR: Could not convert URL to PDF: {e}"
        print(f"  {error_msg}")
        return False, error_msg, None


def export_dataset(url, output_path, timeout=120000, headless=True):
    """
    Export dataset by navigating to URL, clicking Export button, and downloading the file.
    
    Args:
        url: URL to navigate to
        output_path: Path object where the downloaded file should be saved
        timeout: Timeout in milliseconds (default: 120 seconds)
        headless: If False, run browser in visible mode for debugging (default: True)
    
    Returns:
        Tuple of (success: bool, status_message: str)
    """
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, slow_mo=500 if not headless else 0)
            page = browser.new_page()
            
            page.goto(url, wait_until='domcontentloaded', timeout=timeout)
            page.wait_for_timeout(1000)
            
            # Download the dataset via Export button
            download_success, download_status = download_dataset(page, output_path, timeout=60000)
            
            browser.close()
            
            return download_success, download_status
    except Exception as e:
        error_msg = f"ERROR: Could not export dataset: {e}"
        print(f"  {error_msg}")
        return False, error_msg


def process_row(row, url_source_col, title_source_col, office_source_col, agency_source_col,
                base_data_dir, output_df, output_file, output_columns, headless=True):
    """
    Process a single row from the source sheet.
    
    Args:
        row: Pandas Series representing a single row from the source sheet
        url_source_col: Column name for URL
        title_source_col: Column name for title
        office_source_col: Column name for office
        agency_source_col: Column name for agency
        base_data_dir: Base directory for creating title folders
        output_df: DataFrame to append results to
        output_file: Path to output Excel file
        output_columns: List of output column names
        headless: If False, run browser in visible mode for debugging
    
    Returns:
        Updated output_df
    """
    # Extract data from source row
    url = str(row[url_source_col]).strip() if pd.notna(row[url_source_col]) else ""
    title = str(row[title_source_col]).strip() if title_source_col and pd.notna(row.get(title_source_col)) else ""
    office = str(row[office_source_col]).strip() if office_source_col and pd.notna(row.get(office_source_col)) else ""
    agency = str(row[agency_source_col]).strip() if agency_source_col and pd.notna(row.get(agency_source_col)) else ""
    
    print(f"  URL: {url}")
    print(f"  Title: {title}")
    
    # Create folder based on title
    folder_path = None
    files_path_str = None
    if title:
        folder_path = create_title_folder(base_data_dir, title)
        if folder_path:
            files_path_str = str(folder_path)
            print(f"  Created folder: {files_path_str}")
        else:
            print(f"  WARNING: Could not create folder for title")
    else:
        print(f"  WARNING: No title available, skipping folder creation")
    
    # Create new row for output
    new_row = {
        '7_original_distribution_url': url,
        '4_title': title,
        '5_agency': office,
        '5_agency2': agency,
        'Status': None,
        'files_path': files_path_str
    }
    
    # Try to access the URL and convert to PDF
    if url and url.startswith('http'):
        print(f"  Attempting to access URL...")
        success, status_msg, status_code, html_content = access_url(url)
        base_status = status_msg
        if success:
            print(f"  ✓ Status: {status_msg}")
            
            # Convert URL to PDF and save to working folder
            if folder_path:
                pdf_filename = sanitize_folder_name(title, max_length=100) + ".pdf"
                pdf_path = folder_path / pdf_filename
                
                print(f"  Converting URL to PDF using browser...")
                pdf_success, pdf_status, total_rows = url_to_pdf(url, pdf_path, headless=headless)
                if pdf_success:
                    print(f"  ✓ PDF saved: {pdf_path}")
                    
                    # Export dataset
                    dataset_filename = sanitize_folder_name(title, max_length=80) + ".csv"
                    dataset_path = folder_path / dataset_filename
                    
                    print(f"  Exporting dataset...")
                    download_success, download_status = export_dataset(url, dataset_path, headless=headless)
                    if download_success:
                        print(f"  ✓ {download_status}")
                    elif download_status != "Not attempted":
                        print(f"  Note: {download_status}")
                    
                    # Combine status messages
                    status_parts = [base_status]
                    if pdf_status:
                        status_parts.append(pdf_status)
                    new_row['Status'] = "; ".join(status_parts)
                else:
                    print(f"  ✗ Failed to save PDF")
                    new_row['Status'] = f"{base_status}; {pdf_status}"
            else:
                print(f"  ✗ No folder available for PDF")
                new_row['Status'] = base_status
        else:
            print(f"  ✗ Status: {status_msg}")
            new_row['Status'] = status_msg
    else:
        new_row['Status'] = "Invalid URL"
        print(f"  ✗ Status: Invalid URL")
    
    # Append new row to output DataFrame
    output_df = pd.concat([output_df, pd.DataFrame([new_row])], ignore_index=True)
    
    # Save output file after each row
    try:
        output_df.to_excel(output_file, index=False, engine='openpyxl')
        print(f"  Saved to output file")
    except Exception as e:
        print(f"  ERROR: Could not save output file: {e}")
        sys.exit(1)
    
    return output_df


def process_rows(source_file, output_file, start_row=0, num_rows=None, headless=True):
    """
    Process rows from source sheet and write to output sheet.
    Handles setup and cleanup, then calls process_row for each row.
    
    Args:
        source_file: Path to source Excel file
        output_file: Path to output Excel file
        start_row: First eligible row to process (0-indexed)
        num_rows: Number of eligible rows to process (None = all remaining)
        headless: If False, run browser in visible mode for debugging (default: True)
    """
    # Setup: Get filtered rows
    filtered_df, url_col = get_filtered_rows(source_file)
    
    if len(filtered_df) == 0:
        print("No eligible rows to process.")
        return
    
    # Apply subset
    if start_row < 0:
        start_row = 0
    if start_row >= len(filtered_df):
        print(f"Warning: start_row ({start_row}) is >= number of eligible rows ({len(filtered_df)})")
        return
    
    end_row = start_row + num_rows if num_rows is not None else len(filtered_df)
    end_row = min(end_row, len(filtered_df))
    
    rows_to_process = filtered_df.iloc[start_row:end_row].copy()
    print(f"\nProcessing rows {start_row} to {end_row-1} of eligible rows ({len(rows_to_process)} rows)")
    
    # Find source columns
    url_source_col = url_col  # Column G
    title_source_col = find_column(filtered_df, ['Title of Site', 'Title', 'Site Title'])
    office_source_col = find_column(filtered_df, ['Office'])
    agency_source_col = find_column(filtered_df, ['Agency'])
    
    # Define output columns
    output_columns = ['7_original_distribution_url', '4_title', '5_agency', '5_agency2', 'Status', 'files_path']
    
    # Base directory for creating title folders
    base_data_dir = r'C:\Documents\DataRescue\CDC data'
    
    # Load or create output file
    output_path = Path(output_file)
    if output_path.exists():
        try:
            output_df = pd.read_excel(output_file)
            # Ensure all required columns exist
            for col in output_columns:
                if col not in output_df.columns:
                    output_df[col] = None
        except Exception as e:
            print(f"Warning: Could not read existing output file, creating new one: {e}")
            output_df = pd.DataFrame(columns=output_columns)
    else:
        output_df = pd.DataFrame(columns=output_columns)
    
    print(f"\nBase data directory: {base_data_dir}")
    if not headless:
        print("DEBUG MODE: Browser will be visible")
    
    # Process each row
    for idx, (_, row) in enumerate(rows_to_process.iterrows(), start=start_row):
        print(f"\n[{idx+1}/{len(rows_to_process)}] Processing row {idx}...")
        output_df = process_row(
            row, url_source_col, title_source_col, office_source_col, agency_source_col,
            base_data_dir, output_df, output_file, output_columns, headless=headless
        )
    
    # Cleanup: Print summary
    print(f"\n{'='*80}")
    print(f"Processing complete! {len(rows_to_process)} rows processed.")
    print(f"Output saved to: {output_file}")


def main():
    """Main entry point for the application"""
    parser = argparse.ArgumentParser(
        description='CDC Data Collector - Process Excel files and collect data from URLs'
    )
    parser.add_argument(
        '--input',
        type=str,
        default=r'C:\Documents\DataRescue\Data_Inventories - cdc.xlsx',
        help='Path to source Excel file'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=r'C:\Documents\DataRescue\CDCCollectedData.xlsx',
        help='Path to output Excel file'
    )
    parser.add_argument(
        '--start-row',
        type=int,
        default=0,
        help='First eligible row to process (0-indexed, default: 0)'
    )
    parser.add_argument(
        '--num-rows',
        type=int,
        default=None,
        help='Number of eligible rows to process (default: all remaining)'
    )
    parser.add_argument(
        '--headless',
        action='store_false',
        dest='headless',
        default=True,
        help='Run browser in visible mode for debugging (default: headless)'
    )
    
    args = parser.parse_args()
    
    process_rows(args.input, args.output, args.start_row, args.num_rows, headless=args.headless)


if __name__ == "__main__":
    main()
