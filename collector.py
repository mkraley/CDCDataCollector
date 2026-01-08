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
    
    # Remove control characters
    sanitized = re.sub(r'[\x00-\x1f\x7f]', '', sanitized)
    
    # Remove leading/trailing dots and spaces (Windows doesn't allow these)
    sanitized = sanitized.strip('. ')
    
    # Limit length to avoid Windows path issues
    # Windows has 260 char path limit, so keep folder names shorter
    if len(sanitized) > max_length:
        sanitized = sanitized[:max_length]
        # Strip again after truncation in case truncation left trailing spaces
        sanitized = sanitized.strip('. ')
    
    # If empty after sanitization, use default
    if not sanitized:
        sanitized = "Untitled"
    
    return sanitized


def create_title_folder(base_dir, title, verbose=False):
    """
    Create or reuse a folder named after the title and return the full path.
    If the folder already exists, clears all files in it.
    
    Args:
        base_dir: Base directory path
        title: Title to use for folder name
        verbose: If True, print status messages
    
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
            if verbose:
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


def get_number_of_column_rows(page):
    """
    Get the total number of rows from the paginator legend (e.g., "1-15 of 125" -> 125).
    
    Args:
        page: Playwright page object
    
    Returns:
        int or None - total number of rows, or None if not found
    """
    try:
        # Playwright can pierce shadow DOM with locator, but for complex shadow DOM
        # operations like accessing slots, we use a minimal evaluate call
        result = page.evaluate("""
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
        """)
        return result
    except Exception:
        return None


def format_file_size(size_bytes):
    """
    Format file size in human-readable format.
    
    Args:
        size_bytes: File size in bytes
    
    Returns:
        Formatted string (e.g., "1.5 MB", "500 KB")
    """
    if size_bytes is None:
        return "unknown"
    
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            if unit == 'B':
                return f"{int(size_bytes)} {unit}"
            else:
                return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} TB"


def get_dataset_metadata(page):
    """
    Get dataset metadata (rows and columns) from the metadata-row element.
    
    Args:
        page: Playwright page object
    
    Returns:
        Tuple of (rows: str or None, columns: str or None)
    """
    try:
        metadata_row = page.locator('dl.metadata-row')
        if metadata_row.count() == 0:
            return None, None
        
        rows = None
        columns = None
        
        pairs = metadata_row.locator('.metadata-pair')
        pair_count = pairs.count()
        
        for i in range(pair_count):
            pair = pairs.nth(i)
            key_locator = pair.locator('.metadata-pair-key')
            value_locator = pair.locator('.metadata-pair-value')
            
            if key_locator.count() == 0 or value_locator.count() == 0:
                continue
            
            key_text = key_locator.first.inner_text().strip()
            value_text = value_locator.first.inner_text().strip()
            
            if key_text == 'Rows':
                rows = value_text
            elif key_text == 'Columns':
                columns = value_text
        
        return rows, columns
    except Exception:
        return None, None


def get_description(page):
    """
    Get description text from div.description-section element.
    
    Args:
        page: Playwright page object
    
    Returns:
        Description text as string, or None if not found
    """
    try:
        description_locator = page.locator('div.description-section')
        if description_locator.count() == 0:
            return None
        text = description_locator.first.inner_text()
        return text.strip() if text else None
    except Exception:
        return None


def get_keywords(page):
    """
    Get keywords from the metadata table.
    Looks for div.metadata-table with h3 child "Topics", then finds tr with first td "Tags"
    and extracts textContent from the second td.
    
    Args:
        page: Playwright page object
    
    Returns:
        Keywords text as string, or None if not found
    """
    try:
        metadata_tables = page.locator('div.metadata-table')
        table_count = metadata_tables.count()
        
        for i in range(table_count):
            table = metadata_tables.nth(i)
            # Check if it has an immediate child h3 with text "Topics"
            h3 = table.locator('> h3').first
            if h3.count() == 0:
                continue
            
            h3_text = h3.inner_text().strip()
            if h3_text != 'Topics':
                continue
            
            # Find tr whose first td has text "Tags"
            rows = table.locator('tr')
            row_count = rows.count()
            
            for j in range(row_count):
                row = rows.nth(j)
                tds = row.locator('td')
                if tds.count() < 2:
                    continue
                
                first_td_text = tds.nth(0).inner_text().strip()
                if first_td_text == 'Tags':
                    keywords = tds.nth(1).inner_text().strip()
                    return keywords if keywords else None
        
        return None
    except Exception:
        return None


def show_all_column_rows(page, total_rows, verbose=False):
    """
    Set the rows per page dropdown to show all rows (or 100, whichever is appropriate).
    If total_rows > 100, updates the "100" option to the actual number first.
    
    Args:
        page: Playwright page object
        total_rows: Total number of rows (int or None)
        verbose: If True, print status messages
    
    Returns:
        bool - True if successful, False otherwise
    """
    try:
        if total_rows is not None:
            if verbose:
                print(f"  Total rows: {total_rows}")
            target_page_size = total_rows if total_rows > 100 else 100
            
            # For shadow DOM manipulation, we use evaluate with minimal JavaScript
            rows_result = page.evaluate(f"""
                () => {{
                    try {{
                        const fp = document.querySelector('forge-paginator');
                        if (!fp) {{
                            return {{ success: false, message: 'forge-paginator not found' }};
                        }}
                        
                        const fs = fp.shadowRoot.querySelector('forge-select');
                        if (!fs) {{
                            return {{ success: false, message: 'forge-select not found' }};
                        }}
                        
                        const targetSize = {target_page_size};
                        
                        // If target size > 100, update the "100" option to the actual number
                        if (targetSize > 100) {{
                            const option100 = fs.querySelector('forge-option[label="100"]');
                            if (option100) {{
                                option100.setAttribute('label', targetSize.toString());
                                option100.textContent = targetSize.toString();
                            }}
                        }}
                        
                        // Set the value
                        fs.value = targetSize.toString();
                        fp.pageSize = targetSize;
                        
                        const changeEvent = new Event('change', {{ bubbles: true, cancelable: true }});
                        fs.dispatchEvent(changeEvent);
                        
                        const paginatorChangeEvent = new CustomEvent('forge-paginator-change', {{
                            bubbles: true,
                            cancelable: true,
                            detail: {{
                                type: 'page-size',
                                pageSize: targetSize,
                                pageIndex: fp.pageIndex || 0,
                                offset: fp.offset || 0
                            }}
                        }});
                        fp.dispatchEvent(paginatorChangeEvent);
                        
                        return {{ success: true, message: 'Set to ' + targetSize }};
                    }} catch (e) {{
                        return {{ success: false, message: 'Error: ' + e.message }};
                    }}
                }}
            """)
            
            if rows_result and rows_result.get('success'):
                if verbose:
                    print(f"  Set rows per page to {target_page_size}")
                page.wait_for_timeout(2000)  # Wait for rows to load
                return True
            else:
                error_msg = rows_result.get('message', 'Could not change rows per page') if rows_result else 'Dropdown not found'
                if verbose:
                    print(f"  Note: {error_msg}")
                return False
        else:
            if verbose:
                print(f"  Note: Could not read total rows, defaulting to 100")
            # Fallback to 100 if we can't read the total
            rows_result = page.evaluate("""
                () => {
                    try {
                        const fp = document.querySelector('forge-paginator');
                        if (!fp) {
                            return { success: false, message: 'forge-paginator not found' };
                        }
                        
                        const fs = fp.shadowRoot.querySelector('forge-select');
                        if (!fs) {
                            return { success: false, message: 'forge-select not found' };
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
                        
                        return { success: true, message: 'Set to 100' };
                    } catch (e) {
                        return { success: false, message: 'Error: ' + e.message };
                    }
                }
            """)
            page.wait_for_timeout(2000)
            return True
    except Exception as e:
        if verbose:
            print(f"  Note: Could not change rows per page dropdown: {e}")
        return False


def expand_read_more_links(page, verbose=False):
    """
    Find and click "Read more" links/buttons to expand content.
    
    Args:
        page: Playwright page object
        verbose: If True, print status messages
    
    Returns:
        int - number of links clicked
    """
    try:
        buttons = page.locator('forge-button.collapse-button')
        button_count = buttons.count()
        
        max_clicks = min(button_count, 100)
        clicked_count = 0
        
        for i in range(max_clicks):
            try:
                buttons.nth(i).click(timeout=1000)
                clicked_count += 1
            except Exception:
                # Button might not be clickable, continue
                continue
        
        if clicked_count > 0:
            if verbose:
                print(f"  Expanded {clicked_count} 'Read more' sections")
            page.wait_for_timeout(1500)
        return clicked_count
    except Exception as e:
        if verbose:
            print(f"  Note: Could not expand 'Read more' links: {e}")
        return 0


def download_dataset(page, output_path, timeout=60000):
    """
    Download dataset by clicking Export button and then Download button in the dialog.
    Intercepts the download request to get the file.
    
    Args:
        page: Playwright page object
        output_path: Path object where the downloaded file should be saved
        timeout: Timeout in milliseconds (default: 60 seconds)
    
    Returns:
        Tuple of (success: bool, status_message: str, file_extension: str or None)
    """
    try:
        # Find and click Export button using Playwright Python API
        # Get all potential export buttons and filter by text in Python
        all_buttons = page.locator('button, a, [role="button"]')
        button_count = all_buttons.count()
        export_button = None
        
        for i in range(button_count):
            try:
                button = all_buttons.nth(i)
                text = button.inner_text().strip().lower()
                if 'export' in text and len(text) < 50:
                    export_button = button
                    break
            except Exception:
                continue
        
        if export_button is None:
            return False, 'Export button not found', None
        
        try:
            export_button.scroll_into_view_if_needed()
            export_button.click()
        except Exception as e:
            return False, f'Could not click Export button: {str(e)}', None
        
        # Wait for dialog to appear
        page.wait_for_timeout(1000)
        
        # Check for large dataset warning before attempting download
        try:
            warning_element = page.locator('div.message-title[slot="title"]')
            warning_count = warning_element.count()
            if warning_count > 0:
                warning_text = warning_element.first.inner_text().strip()
                if 'Large dataset warning' in warning_text:
                    return False, 'Large dataset warning - download skipped', None
        except Exception:
            # If we can't check for the warning, continue with download attempt
            pass
        
        # Set up download listener before clicking Download
        with page.expect_download(timeout=timeout) as download_info:
            # Find and click Download button in the dialog - look for exact text "Download"
            # First try all buttons
            all_buttons = page.locator('button, a, [role="button"]')
            button_count = all_buttons.count()
            download_button = None
            
            for i in range(button_count):
                try:
                    button = all_buttons.nth(i)
                    if button.inner_text().strip() == 'Download':
                        download_button = button
                        break
                except Exception:
                    continue
            
            # If not found, try looking in dialogs/modals
            if download_button is None:
                dialogs = page.locator('dialog, [role="dialog"], .modal, [class*="dialog"]')
                dialog_count = dialogs.count()
                for i in range(dialog_count):
                    dialog = dialogs.nth(i)
                    dialog_buttons = dialog.locator('button, a, [role="button"]')
                    dialog_button_count = dialog_buttons.count()
                    for j in range(dialog_button_count):
                        try:
                            button = dialog_buttons.nth(j)
                            if button.inner_text().strip() == 'Download':
                                download_button = button
                                break
                        except Exception:
                            continue
                    if download_button is not None:
                        break
            
            if download_button is None:
                return False, 'Download button with exact label "Download" not found in dialog', None
            
            try:
                download_button.scroll_into_view_if_needed()
                download_button.click()
            except Exception as e:
                return False, f'Could not click Download button: {str(e)}', None
        
        # Wait for download to complete and save the file
        download = download_info.value
        
        # Get the original suggested filename to detect file extension
        suggested_filename = download.suggested_filename()
        file_extension = None
        if suggested_filename:
            # Extract extension from suggested filename
            ext_with_dot = Path(suggested_filename).suffix
            if ext_with_dot:
                # Remove the leading dot
                file_extension = ext_with_dot[1:].lower()
        
        download.save_as(output_path)
        
        # If we couldn't detect extension from suggested filename, try from saved file
        if file_extension is None and output_path.exists():
            ext_with_dot = output_path.suffix
            if ext_with_dot:
                file_extension = ext_with_dot[1:].lower()
        
        return True, f"Dataset downloaded: {output_path.name}", file_extension
    
    except PlaywrightTimeoutError:
        return False, "Timeout waiting for download (check if Export/Download buttons work)", None
    except Exception as e:
        return False, f"Error downloading dataset: {str(e)[:100]}", None


def get_source_data(row, url_source_col, title_source_col, office_source_col, agency_source_col):
    """
    Extract data from a source row.
    
    Args:
        row: Pandas Series representing a single row from the source sheet
        url_source_col: Column name for URL
        title_source_col: Column name for title
        office_source_col: Column name for office
        agency_source_col: Column name for agency
    
    Returns:
        Tuple of (url: str, title: str, office: str, agency: str)
    """
    url = str(row[url_source_col]).strip() if pd.notna(row[url_source_col]) else ""
    title = str(row[title_source_col]).strip() if title_source_col and pd.notna(row.get(title_source_col)) else ""
    office = str(row[office_source_col]).strip() if office_source_col and pd.notna(row.get(office_source_col)) else ""
    agency = str(row[agency_source_col]).strip() if agency_source_col and pd.notna(row.get(agency_source_col)) else ""
    
    # Expand "CDC" to full agency name
    if agency and agency.upper() == "CDC":
        agency = "United States Department of Health and Human Services. Centers for Disease Control and Prevention"
    
    return url, title, office, agency


def create_data_folder(base_data_dir, title, verbose=False):
    """
    Create a data folder based on title (alias for create_title_folder for consistency).
    
    Args:
        base_data_dir: Base directory for creating title folders
        title: Title to use for folder name
        verbose: If True, print status messages
    
    Returns:
        Path object for the created folder, or None if creation failed
    """
    return create_title_folder(base_data_dir, title, verbose=verbose)


def create_new_output_row(url, title, office, agency, files_path_str):
    """
    Create a new output row dictionary with the given data.
    
    Args:
        url: URL string
        title: Title string
        office: Office string
        agency: Agency string
        files_path_str: Files path string (or None)
    
    Returns:
        Dictionary representing the output row
    """
    today = datetime.now().strftime('%Y-%m-%d')
    return {
        '7_original_distribution_url': url,
        '4_title': title,
        '5_agency': agency,  # Swapped: Agency goes to 5_agency
        '5_agency2': office,  # Swapped: Office goes to 5_agency2
        'Status': None,
        'path': files_path_str,
        'dataset_rows': None,
        'dataset_cols': None,
        'dataset_size': None,
        'file_extensions': 'PDF, csv',
        '12_download_date_original_source': today,
        '6_summary_description': None,
        '8_keywords': None
    }


def update_output_data(output_df, new_row, output_file, verbose=False):
    """
    Update or append a row to the output DataFrame and save to file.
    If a row with the same URL already exists, it will be updated instead of creating a duplicate.
    
    Args:
        output_df: DataFrame to update/append to
        new_row: Dictionary representing the new row
        output_file: Path to output Excel file
        verbose: If True, print status messages
    
    Returns:
        Updated output_df
    """
    url = new_row.get('7_original_distribution_url')
    
    # Check if a row with the same URL already exists
    if url and '7_original_distribution_url' in output_df.columns:
        matching_indices = output_df[output_df['7_original_distribution_url'] == url].index
        
        if len(matching_indices) > 0:
            # Update the first matching row (in case there are duplicates)
            idx = matching_indices[0]
            # Ensure columns that may contain strings are object dtype to avoid dtype warnings
            string_columns = ['dataset_size', 'dataset_rows', 'dataset_cols', 'file_extensions', 
                            '12_download_date_original_source', '6_summary_description', '8_keywords', 'Status', 
                            'path', '7_original_distribution_url', '4_title', '5_agency', '5_agency2']
            for col in string_columns:
                if col in output_df.columns and output_df[col].dtype != 'object':
                    output_df[col] = output_df[col].astype('object')
            
            for key, value in new_row.items():
                output_df.at[idx, key] = value
            if verbose:
                print(f"  Updated existing row in output file")
        else:
            # No matching row found, append new row
            output_df = pd.concat([output_df, pd.DataFrame([new_row])], ignore_index=True)
            if verbose:
                print(f"  Added new row to output file")
    else:
        # No URL to match on, just append
        output_df = pd.concat([output_df, pd.DataFrame([new_row])], ignore_index=True)
        if verbose:
            print(f"  Added new row to output file")
    
    try:
        output_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        if verbose:
            print(f"  Saved to output file")
    except Exception as e:
        print(f"  ERROR: Could not save output file: {e}")
        sys.exit(1)
    
    return output_df




def convert_source_to_pdf(url, pdf_path, timeout=120000, headless=True, verbose=False):
    """
    Convert a source URL to PDF in a browser session.
    Sets rows per page, expands content, and generates PDF.
    
    Args:
        url: URL to process
        pdf_path: Path object where PDF should be saved
        timeout: Timeout in milliseconds (default: 120 seconds)
        headless: If False, run browser in visible mode for debugging (default: True)
        verbose: If True, print status messages
    
    Returns:
        Tuple of (page: Playwright page object, browser: Playwright browser object, 
                 pdf_status: str, total_rows: int or None)
        Caller is responsible for closing the browser.
    """
    playwright = None
    browser = None
    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=headless, slow_mo=500 if not headless else 0)
        page = browser.new_page()
        
        page.goto(url, wait_until='domcontentloaded', timeout=timeout)
        page.wait_for_timeout(500)
        
        # Get number of column rows
        total_rows = get_number_of_column_rows(page)
        
        # Show all column rows (set dropdown)
        show_all_column_rows(page, total_rows, verbose=verbose)
        
        # Expand read more links
        expand_read_more_links(page, verbose=verbose)
        
        # Generate PDF
        page.pdf(path=str(pdf_path), format='A4', print_background=True)
        pdf_status = "PDF generated"
        
        return page, browser, playwright, pdf_status, total_rows
    except Exception as e:
        if browser:
            browser.close()
        if playwright:
            playwright.stop()
        error_msg = f"ERROR: Could not convert source to PDF: {e}"
        if verbose:
            print(f"  {error_msg}")
        raise Exception(error_msg)


def process_row(row, url_source_col, title_source_col, office_source_col, agency_source_col,
                base_data_dir, output_df, output_file, output_columns, headless=True, verbose=False, ordinal=None, total=None, spreadsheet_row=None):
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
        verbose: If True, show detailed logging
        ordinal: Ordinal number (1-based) of dataset in current batch (optional)
        total: Total number of rows in batch for logging (optional)
        spreadsheet_row: Original spreadsheet row index (optional)
    
    Returns:
        Updated output_df
    """
    # Get source data
    url, title, office, agency = get_source_data(row, url_source_col, title_source_col, office_source_col, agency_source_col)
    
    if verbose:
        print(f"  URL: {url}")
        print(f"  Title: {title}")
    
    # Create data folder
    folder_path = create_data_folder(base_data_dir, title, verbose=verbose)
    if not folder_path:
        if verbose:
            print(f"  ERROR: Could not create folder for title")
        sys.exit(1)
    
    files_path_str = str(folder_path)
    if verbose:
        print(f"  Created folder: {files_path_str}")
    
    # Create new output row
    new_row = create_new_output_row(url, title, office, agency, files_path_str)
    
    # Validate URL
    if not url or not url.startswith('http'):
        new_row['Status'] = "Invalid URL"
        if verbose:
            print(f"  ✗ Status: Invalid URL")
        else:
            if ordinal is not None and total is not None and spreadsheet_row is not None:
                idx_str = f"[{ordinal}/{total} row: {spreadsheet_row}] "
            else:
                idx_str = ""
            print(f"{idx_str}{url} - Invalid URL")
        output_df = update_output_data(output_df, new_row, output_file)
        return output_df
    
    # Access URL
    if verbose:
        print(f"  Attempting to access URL...")
    success, status_msg, status_code, html_content = access_url(url)
    if not success:
        new_row['Status'] = status_msg
        if verbose:
            print(f"  ✗ Status: {status_msg}")
        else:
            if ordinal is not None and total is not None and spreadsheet_row is not None:
                idx_str = f"[{ordinal}/{total} row: {spreadsheet_row}] "
            else:
                idx_str = ""
            print(f"{idx_str}{url} - {status_msg}")
        output_df = update_output_data(output_df, new_row, output_file)
        return output_df
    
    if verbose:
        print(f"  ✓ Status: {status_msg}")
    base_status = status_msg
    
    # Prepare file paths
    pdf_filename = sanitize_folder_name(title, max_length=100) + ".pdf"
    pdf_path = folder_path / pdf_filename
    
    dataset_filename = sanitize_folder_name(title, max_length=80) + ".csv"
    dataset_path = folder_path / dataset_filename
    
    if verbose:
        print(f"  Processing URL (PDF + Export)...")
    
    # Convert source to PDF
    browser = None
    playwright = None
    problems = []
    try:
        page, browser, playwright, pdf_status, total_rows = convert_source_to_pdf(url, pdf_path, headless=headless, verbose=verbose)
        if verbose:
            print(f"  ✓ PDF saved: {pdf_path}")
        
        # Download dataset
        download_success, download_status, dataset_extension = download_dataset(page, dataset_path, timeout=60000)
        if download_success:
            if verbose:
                print(f"  ✓ {download_status}")
        else:
            problems.append(download_status)
            if verbose:
                print(f"  Note: {download_status}")
        
        # Get metadata and file size
        metadata_rows, metadata_columns = get_dataset_metadata(page)
        dataset_size = None
        if dataset_path.exists():
            dataset_size = dataset_path.stat().st_size
        
        # Get description (after read more links have been expanded in convert_source_to_pdf)
        description = get_description(page)
        
        # Get keywords from metadata table
        keywords = get_keywords(page)
        
        # Determine file extensions: PDF + detected dataset extension, or just PDF if download failed/skipped
        if download_success and dataset_extension:
            file_extensions = f"PDF, {dataset_extension}"
        elif download_success:
            # Download succeeded but couldn't detect extension, default to csv
            file_extensions = "PDF, csv"
        else:
            # Download failed or was skipped, only PDF available
            file_extensions = "PDF"
        
        # Update output row with dataset information
        new_row['dataset_rows'] = metadata_rows
        new_row['dataset_cols'] = metadata_columns
        new_row['dataset_size'] = format_file_size(dataset_size) if dataset_size is not None else None
        new_row['file_extensions'] = file_extensions
        new_row['6_summary_description'] = description
        new_row['8_keywords'] = keywords
        
        # Combine status messages
        status_parts = [base_status, pdf_status]
        if not download_success:
            status_parts.append(download_status)
        new_row['Status'] = "; ".join(status_parts)
        
        # Log success (normal or verbose mode)
        if verbose:
            if metadata_rows and metadata_columns:
                print(f"  Dataset: {metadata_rows} rows, {metadata_columns} columns")
            if dataset_size is not None:
                print(f"  Dataset size: {format_file_size(dataset_size)}")
        else:
            if ordinal is not None and total is not None and spreadsheet_row is not None:
                idx_str = f"[{ordinal}/{total} row: {spreadsheet_row}] "
            else:
                idx_str = ""
            rows_str = metadata_rows if metadata_rows else "?"
            cols_str = metadata_columns if metadata_columns else "?"
            size_str = format_file_size(dataset_size) if dataset_size is not None else "unknown"
            print(f"{idx_str}{url} - {rows_str} rows, {cols_str} columns, {size_str}")
            if problems:
                for problem in problems:
                    print(f"  {problem}")
    except Exception as e:
        problems.append(str(e))
        new_row['Status'] = f"{base_status}; {str(e)}"
        # Set dataset fields to None on error
        new_row['dataset_rows'] = None
        new_row['dataset_cols'] = None
        new_row['dataset_size'] = None
        new_row['6_summary_description'] = None
        new_row['8_keywords'] = None
        if verbose:
            print(f"  ✗ Error: {e}")
        else:
            if ordinal is not None and total is not None and spreadsheet_row is not None:
                idx_str = f"[{ordinal}/{total} row: {spreadsheet_row}] "
            else:
                idx_str = ""
            print(f"{idx_str}{url} - Error: {e}")
    finally:
        if browser:
            browser.close()
        if playwright:
            playwright.stop()
    
    # Update output data (append row and save)
    output_df = update_output_data(output_df, new_row, output_file, verbose=verbose)
    
    return output_df


def process_rows(source_file, output_file, start_row=0, num_rows=None, headless=True, verbose=False):
    """
    Process rows from source sheet and write to output sheet.
    Handles setup and cleanup, then calls process_row for each row.
    
    Args:
        source_file: Path to source Excel file
        output_file: Path to output Excel file
        start_row: First eligible row to process (0-indexed)
        num_rows: Number of eligible rows to process (None = all remaining)
        headless: If False, run browser in visible mode for debugging (default: True)
        verbose: If True, show detailed logging (default: False)
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
    output_columns = ['7_original_distribution_url', '4_title', '5_agency', '5_agency2', 'Status', 'path',
                      'dataset_rows', 'dataset_cols', 'dataset_size', 'file_extensions', '12_download_date_original_source', '6_summary_description', '8_keywords']
    
    # Base directory for creating title folders
    base_data_dir = r'C:\Documents\DataRescue\CDC data'
    
    # Load or create output file
    output_path = Path(output_file)
    if output_path.exists():
        try:
            output_df = pd.read_csv(output_file, encoding='utf-8-sig')
            # Ensure all required columns exist
            for col in output_columns:
                if col not in output_df.columns:
                    output_df[col] = None
        except Exception as e:
            print(f"Warning: Could not read existing output file, creating new one: {e}")
            output_df = pd.DataFrame(columns=output_columns)
    else:
        output_df = pd.DataFrame(columns=output_columns)
    
    if verbose:
        print(f"\nBase data directory: {base_data_dir}")
    if not headless:
        print("DEBUG MODE: Browser will be visible")
    
    # Process each row
    for ordinal, (spreadsheet_row, row) in enumerate(rows_to_process.iterrows(), start=1):
        if verbose:
            print(f"\n[{ordinal}/{len(rows_to_process)} row: {spreadsheet_row}] Processing row {spreadsheet_row}...")
        output_df = process_row(
            row, url_source_col, title_source_col, office_source_col, agency_source_col,
            base_data_dir, output_df, output_file, output_columns, headless=headless,
            verbose=verbose, ordinal=ordinal, total=len(rows_to_process), spreadsheet_row=spreadsheet_row
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
        default=r'C:\Documents\DataRescue\CDCCollectedData.csv',
        help='Path to output CSV file'
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
