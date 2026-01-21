

"""
Script to automatically fill in the DataLumos fields from a csv file (exported spreadsheet), and upload the files.
Login, checking and publishing is done manually to avoid errors.

The path of the csv file has to be set before starting the script, the path to the folder with the data files too.
Also, the rows to be processed have to be set (start_row and end_row) - counting starts at 1 and doesn't include the column names row.

There is no error handling. But the browser remains open even if the script crashes, so the inputs could be checked and/or completed manually.
"""



import math
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.keys import Keys
from time import sleep
import csv
import traceback
import os
import re
import argparse
import pandas as pd
from datetime import datetime

# Google Sheets API imports (optional - only used if Google Sheets is configured)
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    GOOGLE_SHEETS_AVAILABLE = False


#########################################################
# Command line arguments are now used instead of hardcoded variables
# See parse_arguments() function below
#########################################################

url_datalumos = "https://www.datalumos.org/datalumos/workspace"


class BatchRestartException(Exception):
    """Exception raised when a batch needs to be restarted due to an error."""
    def __init__(self, error_message, remaining_rows):
        self.error_message = error_message
        self.remaining_rows = remaining_rows
        super().__init__(self.error_message)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Automate DataLumos form filling and file uploads from CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # With automated login using row range:
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --username "user@example.com" --password "pass123" --folder "C:\\data"
  
  # With specific rows (comma-delimited, supports ranges):
  python chiara_upload.py --csv "data.csv" --rows "1,3,5,7-10,15" --username "user@example.com" --password "pass123" --folder "C:\\data"
  
  # With manual login (no username/password):
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --folder "C:\\data"
  
  # Using Firefox instead of Chrome:
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --browser firefox --username "user@example.com" --password "pass123"
        """
    )
    
    parser.add_argument('--csv', '--csv-file-path', dest='csv_file_path', required=True,
                        help='Path to the CSV file containing the data to upload')
    
    row_group = parser.add_mutually_exclusive_group(required=True)
    row_group.add_argument('--start-row', type=int,
                          help='Starting row number (counting starts at 1, excluding header row). Must be used with --end-row.')
    row_group.add_argument('--rows', type=str,
                          help='Comma-delimited list of row numbers to process (e.g., "1,3,5,7-10"). Incompatible with --start-row and --end-row.')
    
    parser.add_argument('--end-row', type=int,
                        help='Ending row number (to process only one row, set start-row and end-row to the same number). Must be used with --start-row.')
    
    parser.add_argument('--folder', '--folder-path-uploadfiles', dest='folder_path_uploadfiles', default='',
                        help='Path to the folder where upload files are located (subfolders for each project should be in here)')
    
    parser.add_argument('--username', default=None,
                        help='Username/email for automated login to DataLumos (if not provided, manual login will be required)')
    
    parser.add_argument('--password', default=None,
                        help='Password for automated login to DataLumos (if not provided, manual login will be required)')
    
    parser.add_argument('--browser', choices=['chrome', 'chromium', 'firefox'], default='chrome',
                        help='Browser to use: chrome/chromium or firefox (default: chrome)')
    
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose logging (default: one line per asset with summary)')
    
    parser.add_argument('--publish-mode', choices=['default', 'no-publish', 'only-publish'], default='default',
                        help='Publishing mode: default (run all steps including publish), no-publish (skip publishing), only-publish (only publish, skip form-filling)')
    
    parser.add_argument('--google-sheet-id', default='1OYLn6NBWStOgPUTJfYpU0y0g4uY7roIPP4qC2YztgWY',
                        help='Google Sheet ID (from the URL: https://docs.google.com/spreadsheets/d/SHEET_ID/edit). Default: CDC Data Inventories sheet')
    
    parser.add_argument('--google-credentials', default=None,
                        help='Path to Google service account credentials JSON file (required for Google Sheets updates, even if sheet is publicly editable)')
    
    parser.add_argument('--google-sheet-name', default='CDC',
                        help='Name of the worksheet/tab to update (default: CDC)')
    
    parser.add_argument('--google-username', default='mkraley',
                        help='Username to write in the "Claimed" column (default: mkraley)')
    
    parser.add_argument('--GWDA-your-name', dest='gwda_your_name', default='Michael Kraley',
                        help='Name to enter in GWDA nomination form (default: Michael Kraley)')
    
    parser.add_argument('--GWDA-institution', dest='gwda_institution', default='Data Rescue Project',
                        help='Institution to enter in GWDA nomination form (default: Data Rescue Project)')
    
    parser.add_argument('--GWDA-email', dest='gwda_email', default=None,
                        help='Email to enter in GWDA nomination form (default: uses --username value if provided)')
    
    args = parser.parse_args()
    
    # Validate that --rows is incompatible with --start-row and --end-row
    if args.rows and (args.start_row is not None or args.end_row is not None):
        parser.error('--rows cannot be used with --start-row or --end-row')
    
    # Validate that if --start-row is provided, --end-row must also be provided (and vice versa)
    if args.start_row is not None and args.end_row is None:
        parser.error('--start-row requires --end-row to be specified')
    if args.end_row is not None and args.start_row is None:
        parser.error('--end-row requires --start-row to be specified')
    
    # Parse --rows if provided
    if args.rows:
        try:
            # Parse comma-delimited list and handle ranges (e.g., "1,3,5,7-10")
            rows_list = []
            for part in args.rows.split(','):
                part = part.strip()
                if '-' in part:
                    # Handle range (e.g., "7-10")
                    start, end = part.split('-', 1)
                    start = int(start.strip())
                    end = int(end.strip())
                    rows_list.extend(range(start, end + 1))
                else:
                    # Single number
                    rows_list.append(int(part))
            # Remove duplicates and sort
            args.rows = sorted(list(set(rows_list)))
            if not args.rows or any(row < 1 for row in args.rows):
                parser.error('--rows must contain only positive integers (row numbers start at 1)')
        except ValueError as e:
            parser.error(f'--rows must be a comma-delimited list of integers and optional ranges (e.g., "1,3,5,7-10"): {e}')
    else:
        args.rows = None
    
    return args


def initialize_browser(browser_choice='chrome'):
    """
    Initialize the appropriate browser driver.
    
    Args:
        browser_choice: 'chrome', 'chromium', or 'firefox'
    
    Returns:
        WebDriver instance
    """
    if browser_choice.lower() in ['chrome', 'chromium']:
        # Set up Chrome options
        chrome_options = ChromeOptions()
        # Uncomment the line below to run in headless mode (no visible browser window)
        # chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Initialize Chrome driver using webdriver-manager to automatically handle ChromeDriver
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print(f"✓ Initialized Chrome browser")
        return driver
    
    elif browser_choice.lower() == 'firefox':
        # Set up Firefox options
        firefox_options = FirefoxOptions()
        # Uncomment the line below to run in headless mode
        # firefox_options.add_argument("--headless")
        
        # Initialize Firefox driver using webdriver-manager to automatically handle GeckoDriver
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=firefox_options)
        print(f"✓ Initialized Firefox browser")
        return driver
    
    else:
        raise ValueError(f"Unsupported browser choice: {browser_choice}. Use 'chrome' or 'firefox'.")


def wait_for_verification(driver, timeout=30):
    """
    Wait for "Verifying you are human" message to complete.
    
    Args:
        driver: Selenium WebDriver object
        timeout: Maximum time to wait in seconds (default: 30 seconds)
    
    Returns:
        bool: True if verification completed, False if timeout
    """
    try:
        # Check for various forms of verification message
        verification_selectors = [
            (By.XPATH, "//*[contains(text(), 'Verifying you are human')]"),
            (By.XPATH, "//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'verifying') and contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'human')]"),
            (By.CSS_SELECTOR, "[class*='verifying']"),
            (By.CSS_SELECTOR, "[id*='verifying']")
        ]
        
        verification_found = False
        for selector_type, selector in verification_selectors:
            try:
                elements = driver.find_elements(selector_type, selector)
                if len(elements) > 0:
                    verification_found = True
                    print("Human verification detected, waiting for completion...")
                    # Wait for the verification message to disappear
                    WebDriverWait(driver, timeout).until(EC.invisibility_of_element_located((selector_type, selector)))
                    print("✓ Verification completed")
                    break
            except Exception:
                continue
        
        # Additional wait for page to be ready after verification
        sleep(2)
        return True
    except Exception as e:
        # If we can't find the verification message or it times out, continue anyway
        print(f"Note: Verification check completed (or not needed)")
        sleep(2)
        return True


def sign_in(driver, username=None, password=None):
    """
    Automate the sign-in process for DataLumos.
    
    Args:
        driver: Selenium WebDriver object
        username: Optional username/email for automated login
        password: Optional password for automated login
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Navigate to home page
        print("Navigating to DataLumos home page...")
        driver.get("https://www.icpsr.umich.edu/sites/datalumos/home")
        wait_for_verification(driver)
        
        # Click Login button
        print("Looking for 'Login' button...")
        login_found = False
        
        # Try to find Login button by text
        all_buttons = driver.find_elements(By.CSS_SELECTOR, "button, a, [role='button']")
        
        for button in all_buttons:
            try:
                text = button.text.strip()
                if text.lower() == 'login' or 'login' in text.lower():
                    print(f"Found Login button: '{text}'")
                    button.click()
                    login_found = True
                    break
            except Exception:
                continue
        
        if not login_found:
            return False, "Could not find Login button"
        
        # Wait for login page to load and any verification
        wait_for_verification(driver)
        
        # Click "Sign in with Email" button
        print("Looking for 'Sign in with Email' button...")

        
        email_button = driver.find_element(By.ID, 'kc-emaillogin')
        
        if not email_button:
            return False, "Could not find 'Sign in with Email' button"
        email_button.click()

        # Wait for email form to appear and any verification
        wait_for_verification(driver)
        
        # If username and password are provided, automate login
        if username and password:
            # Fill in username/email
            print("Filling in username/email address...")
            try:
                username_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#username, input[name='username']")))
                username_input.clear()
                username_input.send_keys(username)
                print("✓ Username field filled")
            except Exception:
                return False, "Could not find username input field"
            
            # Fill in password
            print("Filling in password...")
            try:
                password_input = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "input#password, input[name='password']")))
                password_input.clear()
                password_input.send_keys(password)
                print("✓ Password field filled")
            except Exception:
                return False, "Could not find password input field"
            
            sleep(0.5)
            
            # Submit the form by clicking the Sign In button
            print("Clicking Sign In button...")
            try:
                submit_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit'][value='Sign In'], input.pf-c-button.btn.btn-primary[type='submit']")))
                submit_button.click()
                print("✓ Sign In button clicked")
            except Exception:
                # Fallback: try pressing Enter on the password field
                print("Sign In button not found, trying Enter key...")
                password_input.send_keys(Keys.RETURN)
            
            # Wait for sign-in to complete
            print("Waiting for sign-in to complete...")
            sleep(3)
        else:
            # No credentials provided - pause for manual login
            print("\n" + "=" * 80)
            print("MANUAL LOGIN REQUIRED")
            print("=" * 80)
            print("Username and password not provided. Please log in manually in the browser.")
            input("Press Enter after you have completed the login...")
            print("Continuing with script execution...\n")
        
       
        return True, "Successfully signed in"
    
    except Exception as e:
        error_msg = f"Error during sign-in: {str(e)}"
        print(f"✗ {error_msg}")
        return False, error_msg


def wait_for_obscuring_elements(current_driver_obj, verbose):
    overlays = current_driver_obj.find_elements(By.ID, "busy")  # caution: find_elements, not find_element
    if len(overlays) != 0:  # there is an overlay
        # verbose_print(f"... (Waiting for overlay to disappear. Overlay(s): {overlays})", verbose)
        for overlay in overlays:
            # Wait until the overlay becomes invisible:
            WebDriverWait(current_driver_obj, 360).until(EC.invisibility_of_element_located(overlay))
            sleep(0.5)

def read_csv_line(csv_file, line_to_process):
    # gets the input from the specified line of the csv file, to put it in the datalumos forms.
    # CSV files are expected to be UTF-8-sig encoded
    with open(csv_file, "r", encoding='utf-8-sig', newline='') as datafile:
        datareader = csv.DictReader(datafile)
        for i, singlerow in enumerate(datareader):
            if i == (line_to_process - 1):  # -1 because i starts counting at 0
                return singlerow  # is already a dictionary
        # If we get here, the line wasn't found
        raise ValueError(f"Line {line_to_process} not found in CSV file (file has fewer rows)")

def get_paths_uploadfiles(folderpath, projectfolder):
    # Builds a list with all the single file paths to be uploaded. Takes as argument the path to the parent folder,
    #   where all the data folders are located (for example, the path to the external USB drive).
    mypath = projectfolder
    if mypath[0:2] == ".\\" or mypath[0:2] == "./":
        # eliminate the first two characters, the dot and the slash:
        mypath = mypath[2:]
    operatingsystem = os.name
    if operatingsystem == "posix":  # for linux or mac
        mypath = mypath.replace("\\", "/")
        folderpath = folderpath.replace("\\", "/")
    elif operatingsystem == "nt":  # for windows
        mypath = mypath.replace("/", "\\")
        folderpath = folderpath.replace("\\", "/")
    combinedpath = os.path.join(folderpath, mypath)
    uploadfiles_names = os.listdir(combinedpath)
    # build the complete paths for the files that should be uploaded, by joining the single parts of the path:
    uploadfiles_paths = [os.path.join(combinedpath, filename) for filename in uploadfiles_names]
    return uploadfiles_paths

def drag_and_drop_file(drop_target, path):
    # the function fakes the drag-and-drop that drags a file from the computer into a specific area to upload it.
    # THE COMPLETE CODE OF THIS FUNCTION IS TAKEN FROM STACKOVERFLOW:
    #   https://stackoverflow.com/questions/43382447/python-with-selenium-drag-and-drop-from-file-system-to-webdriver

    # javascript code that will be executed by selenium:
    JS_DROP_FILE = """
        var target = arguments[0],
            offsetX = arguments[1],
            offsetY = arguments[2],
            document = target.ownerDocument || document,
            window = document.defaultView || window;
    
        var input = document.createElement('INPUT');
        input.type = 'file';
        input.onchange = function () {
          var rect = target.getBoundingClientRect(),
              x = rect.left + (offsetX || (rect.width >> 1)),
              y = rect.top + (offsetY || (rect.height >> 1)),
              dataTransfer = { files: this.files };
    
          ['dragenter', 'dragover', 'drop'].forEach(function (name) {
            var evt = document.createEvent('MouseEvent');
            evt.initMouseEvent(name, !0, !0, window, 0, 0, 0, x, y, !1, !1, !1, !1, 0, null);
            evt.dataTransfer = dataTransfer;
            target.dispatchEvent(evt);
          });
    
          setTimeout(function () { document.body.removeChild(input); }, 25);
        };
        document.body.appendChild(input);
        return input;
    """
    driver = drop_target.parent
    file_input = driver.execute_script(JS_DROP_FILE, drop_target, 0, 0)
    file_input.send_keys(path)


def check_csv_writability(csv_file_path):
    """
    Check if the CSV file is writable by attempting to read and write it.
    
    Args:
        csv_file_path: Path to the CSV file
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    try:
        # CSV files are expected to be UTF-8-sig encoded
        df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        
        # Ensure datalumos_id column exists
        if 'datalumos_id' not in df.columns:
            df['datalumos_id'] = ''
        
        # Try to write the file back (this checks writability)
        df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
        return True, None
    except (PermissionError, IOError, OSError) as e:
        error_msg = f"CSV file is not writable: {csv_file_path}\n   Error: {str(e)}\n   Please close the file if it's open in Excel or another program."
        return False, error_msg
    except Exception as e:
        error_msg = f"Error checking CSV file writability: {str(e)}"
        return False, error_msg


def update_csv_workspace_id(csv_file_path, row_number, workspace_id):
    """
    Update the CSV file with the workspace ID in the datalumos_id column.
    
    Args:
        csv_file_path: Path to the CSV file
        row_number: Row number (1-indexed, excluding header)
        workspace_id: Workspace ID to write
    """
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # CSV files are expected to be UTF-8-sig encoded
            df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
            
            # Ensure datalumos_id column exists and is string type
            if 'datalumos_id' not in df.columns:
                df['datalumos_id'] = ''
            
            # Convert column to string type to avoid dtype warnings
            df['datalumos_id'] = df['datalumos_id'].astype(str)
            
            # Update the specific row (row_number is 1-indexed, excluding header, so it maps to index row_number - 1)
            df.loc[row_number - 1, 'datalumos_id'] = str(workspace_id)
            
            # Write back to CSV using UTF-8-sig
            df.to_csv(csv_file_path, index=False, encoding='utf-8-sig')
            return  # Success, exit the function
        except (PermissionError, IOError, OSError) as e:
            # File is likely open in another program (Excel, etc.)
            retry_count += 1
            if retry_count < max_retries:
                print(f"\n⚠ WARNING: Could not write to CSV file: {csv_file_path}")
                print(f"   Error: {str(e)}")
                print(f"   Please close the file if it's open in Excel or another program.")
                input("   Press Enter after closing the file to retry...")
            else:
                print(f"\n⚠ WARNING: Could not update CSV file after {max_retries} attempts: {csv_file_path}")
                print(f"   Error: {str(e)}")
                print(f"   Please manually update row {row_number} with workspace ID: {workspace_id}")
        except Exception as e:
            # Other errors - don't retry, just report
            print(f"⚠ Warning: Could not update CSV file with workspace ID: {e}")
            return


def verbose_print(message, verbose=False):
    """
    Print message only if verbose mode is enabled.
    
    Args:
        message: Message to print
        verbose: Whether verbose mode is enabled
    """
    if verbose:
        print(message)


def format_exception_for_logging(exception, include_location=True):
    """
    Format an exception for non-verbose logging.
    Includes exception message and code location (file:line) if available.
    Avoids "symbols not available" and unresolved backtraces.
    
    Args:
        exception: The exception object
        include_location: Whether to include file and line number
    
    Returns:
        Formatted error message string
    """
    error_msg = str(exception)
    
    if include_location:
        try:
            # Get the traceback
            tb = exception.__traceback__
            if tb:
                # Get the last frame (where the exception was raised)
                frame = tb
                while frame.tb_next:
                    frame = frame.tb_next
                
                # Extract file and line number
                filename = frame.tb_frame.f_code.co_filename
                lineno = frame.tb_lineno
                
                # Get just the filename (not full path) for cleaner output
                filename_short = os.path.basename(filename)
                
                # Format: "Error message (file:line)"
                return f"{error_msg} ({filename_short}:{lineno})"
        except (AttributeError, TypeError):
            # If we can't extract location info, just return the message
            pass
    
    return error_msg


def fill_project_forms(mydriver, datadict, args, row_errors, row_warnings):
    """
    Fill in all project forms with data from CSV.
    
    Args:
        mydriver: WebDriver instance
        datadict: Dictionary containing CSV row data
        args: Parsed command-line arguments
        row_errors: List to append errors to
        row_warnings: List to append warnings to
    
    Returns:
        workspace_id: Extracted workspace ID or None
    """
    workspace_id = None
    
    # Normal mode: create project and fill forms
    
    # Navigate to workspace page
    verbose_print("Navigating to workspace...", args.verbose)
    # Increase page load timeout for this operation (extend to 120 seconds)
    if hasattr(mydriver, 'set_page_load_timeout'):
        original_timeout = None
        try:
            original_timeout = mydriver.get_page_load_timeout()
        except:
            pass  # get_page_load_timeout() might not be available on all drivers
        
        try:
            mydriver.set_page_load_timeout(120)  # 120 seconds timeout
            mydriver.get("https://www.datalumos.org/datalumos/workspace")
        finally:
            # Restore original timeout if it existed
            if original_timeout is not None:
                try:
                    mydriver.set_page_load_timeout(original_timeout)
                except:
                    pass
            else:
                # Reset to default (typically 30 seconds) if no original timeout was set
                try:
                    mydriver.set_page_load_timeout(30)
                except:
                    pass
    else:
        # Fallback if set_page_load_timeout is not available
        mydriver.get("https://www.datalumos.org/datalumos/workspace")
    wait_for_verification(mydriver)
    
    new_project_btn = WebDriverWait(mydriver, 360).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".btn > span:nth-child(3)"))) # .btn > span:nth-child(3)
    verbose_print("button found", args.verbose)
    wait_for_obscuring_elements(mydriver, args.verbose)
    new_project_btn.click()

    verbose_print(f"Processing row, Title: {datadict['4_title']}\n", args.verbose)


    # --- Title

    # <input type="text" class="form-control" name="title" id="title" value="" data-reactid=".2.0.0.1.2.0.$0.$0.$0.$displayPropKey2.0.2.0">
    project_title_form = WebDriverWait(mydriver, 10).until(EC.presence_of_element_located((By.ID, "title")))
    # title with pre-title (if existent):
    title = datadict.get("4_title", "") or ""
    pre_title = datadict.get("4_pre_title", "") or ""
    pojecttitle = title if len(pre_title) == 0 else pre_title + " " + title
    project_title_form.send_keys(pojecttitle)
    # .save-project
    project_title_apply = WebDriverWait(mydriver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".save-project")))
    verbose_print("project_title_apply - found", args.verbose)
    project_title_apply.click()
    # <a role="button" class="btn btn-primary" href="workspace?goToPath=/datalumos/239181&amp;goToLevel=project" data-reactid=".2.0.0.1.2.1.0.0.0">Continue To Project Workspace</a>
    #   CSS-selector: a.btn-primary
    project_title_apply2 = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.LINK_TEXT, "Continue To Project Workspace")))
    verbose_print("Continue To Project Workspace - found", args.verbose)
    project_title_apply2.click()
    
    # Wait for navigation to complete
    wait_for_obscuring_elements(mydriver, args.verbose)
    sleep(1)
    
    # Extract workspace ID from current URL after navigating to workspace
    current_url = mydriver.current_url
    # Look for /datalumos/ followed by digits in the URL
    match = re.search(r'/datalumos/(\d+)', current_url)
    if match:
        workspace_id = match.group(1)
        verbose_print(f"✓ Workspace ID: {workspace_id} (from URL: {current_url})", args.verbose)
    else:
        warning_msg = f"Could not extract workspace ID from URL: {current_url}"
        row_warnings.append(warning_msg)
        verbose_print(f"⚠ {warning_msg}", args.verbose)


    # --- expand everything

    # collapse all: <span data-reactid=".0.3.1.1.0.1.2.0.1.0.1.1"> Collapse All</span>
    #   css-selector: #expand-init > span:nth-child(2)
    collapse_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#expand-init > span:nth-child(2)")))
    wait_for_obscuring_elements(mydriver, args.verbose)
    collapse_btn.click()
    sleep(2)
    # expand all: <span data-reactid=".0.3.1.1.0.1.2.0.1.0.1.1"> Expand All</span>
    #   CSS-selector:    #expand-init > span:nth-child(2)
    expand_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#expand-init > span:nth-child(2)")))
    wait_for_obscuring_elements(mydriver, args.verbose)
    expand_btn.click()
    sleep(2)



    # --- Government agency

    # government add value: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey1.0.2.2"> add value</span>
    #   CSS-selector: #groupAttr0 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)
    agency_investigator = [datadict["5_agency"], datadict["5_agency2"]]
    for singleinput in agency_investigator:
        if len(singleinput) != 0 and singleinput != " ":
            add_gvmnt_value = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#groupAttr0 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)")))
            verbose_print("add_gvmnt_value found", args.verbose)
            wait_for_obscuring_elements(mydriver, args.verbose)
            add_gvmnt_value.click()
            # <a href="#org" aria-controls="org" role="tab" data-toggle="tab" data-reactid=".2.0.0.1.0.1.0">Organization/Agency</a>
            #    css-selector: div.modal:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > a:nth-child(1)
            agency_tab = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.LINK_TEXT, "Organization/Agency")))
            verbose_print("agency_tab found", args.verbose)
            wait_for_obscuring_elements(mydriver, args.verbose)
            agency_tab.click()
            # <input type="text" name="orgName" id="orgName" required="" class="form-control ui-autocomplete-input" value="" data-reactid=".2.0.0.1.1.1.0.0.0.1.0.0.0.1.0" autocomplete="off">
            agency_field = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.ID, "orgName")))
            agency_field.send_keys(singleinput)
            # Wait a moment for the dropdown to appear
            sleep(0.5)
            # Click on the Organization Name label to dismiss the dropdown
            try:
                # Try to find label associated with orgName field
                org_label = mydriver.find_element(By.CSS_SELECTOR, "label[for='orgName']")
                org_label.click()
                sleep(0.3)
            except Exception:
                # Fallback: try clicking on text "Organization Name" or modal header
                try:
                    org_label = WebDriverWait(mydriver, 5).until(EC.element_to_be_clickable((By.XPATH, "//label[contains(text(), 'Organization') or contains(text(), 'Agency')]")))
                    org_label.click()
                    sleep(0.3)
                except Exception:
                    # If label not found, try clicking elsewhere in the modal to dismiss dropdown
                    try:
                        modal_header = mydriver.find_element(By.CSS_SELECTOR, ".modal-header, .modal-title")
                        modal_header.click()
                        sleep(0.3)
                    except Exception:
                        # Last resort: press Escape key to close dropdown
                        agency_field.send_keys(Keys.ESCAPE)
                        sleep(0.3)
            # submit: <button type="button" class="btn btn-primary save-org" data-reactid=".2.0.0.1.1.1.0.0.0.1.0.0.1.0.0">Save &amp; Apply</button>
            #   .save-org
            wait_for_obscuring_elements(mydriver, args.verbose)
            submit_agency_btn = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".save-org")))
            submit_agency_btn.click()


    # --- Summary

    summarytext = datadict.get("6_summary_description", "") or ""
    
    if len(summarytext) != 0 and summarytext != " ":
        # summary edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey2.$dcterms_description_0.1.0.0.0.2.1"> edit</span>
        #   CSS-selector: #edit-dcterms_description_0 > span:nth-child(2)
        edit_summary = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-dcterms_description_0 > span:nth-child(2)")))
        verbose_print("edit_summary found", args.verbose)
        wait_for_obscuring_elements(mydriver, args.verbose)
        edit_summary.click()
        # summary form: The WYSIWYG editor is inside an iframe with class "wysihtml5-sandbox"
        #   First, find and switch to the iframe
        wysihtml5_iframe = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe.wysihtml5-sandbox")))
        mydriver.switch_to.frame(wysihtml5_iframe)
        # Now find the body element inside the iframe
        summary_form = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        # Click to focus the contenteditable element
        summary_form.click()
        sleep(0.3)
        # Clear any existing content
        summary_form.send_keys(Keys.CONTROL + "a")
        sleep(0.2)
        # Use JavaScript to set the text content (more reliable for contenteditable elements)
        # Use the fixed summary text
        mydriver.execute_script("arguments[0].textContent = arguments[1];", summary_form, summarytext)
        # Trigger input event to ensure the editor recognizes the change
        mydriver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", summary_form)
        sleep(0.3)
        # Switch back to default content before clicking save button (which is outside iframe)
        mydriver.switch_to.default_content()
        wait_for_obscuring_elements(mydriver, args.verbose)
        # save: <i class="glyphicon glyphicon-ok"></i>
        #   .glyphicon-ok
        save_summary_btn = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".glyphicon-ok")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        save_summary_btn.click()
    else:
        warning_msg = "The summary is mandatory for the DataLumos project! Please fill it in manually."
        row_warnings.append(warning_msg)
        verbose_print(warning_msg, args.verbose)


    # --- Original Distribution url

    original_url_text = datadict["7_original_distribution_url"]
    if len(original_url_text) != 0 and original_url_text != " ":
        # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey4.$imeta_sourceURL_0.1.0.0.0.2.0.1"> edit</span>
        #   css-sel: #edit-imeta_sourceURL_0 > span:nth-child(1) > span:nth-child(2)
        orig_distr_edit = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-imeta_sourceURL_0 > span:nth-child(1) > span:nth-child(2)")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        orig_distr_edit.click()
        # form: <input type="text" class="form-control input-sm" style="padding-right: 24px;">
        #   css-sel.: .editable-input > input:nth-child(1)
        orig_distr_form = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".editable-input > input:nth-child(1)")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        orig_distr_form.send_keys(original_url_text)
        # save: <button type="submit" class="btn btn-primary btn-sm editable-submit"><i class="glyphicon glyphicon-ok"></i> save</button>
        #   css-sel: .editable-submit
        orig_distr_form.submit()


    # --- Subject Terms / keywords

    # form: <input class="select2-search__field" type="search" tabindex="0" autocomplete="off" autocorrect="off" autocapitalize="none" spellcheck="false" role="textbox" aria-autocomplete="list" placeholder="" style="width: 0.75em;">
    #   css-sel: .select2-search__field
    # scroll bar: <li class="select2-results__option select2-results__option--highlighted" role="treeitem" aria-selected="false">HIFLD Open</li>
    #    css-sel: .select2-results__option
    keywordcells = [
        datadict.get("8_subject_terms1", "") or "",
        datadict.get("8_subject_terms2", "") or "",
        datadict.get("8_keywords", "") or ""
    ]
    keywords_to_insert = []
    for single_keywordcell in keywordcells:
        if len(single_keywordcell) > 0 and single_keywordcell != " ":
            more_keywords = single_keywordcell.replace("'", "").replace("[", "").replace("]", "").replace('"', '')  # remove quotes and brackets
            more_keywordslist = more_keywords.split(",")
            keywords_to_insert += more_keywordslist
    verbose_print(f"\nkeywords_to_insert: {keywords_to_insert}\n", args.verbose)
    for single_keyword in keywords_to_insert:
        keyword = single_keyword.strip(" '")
        if len(keyword) <= 2:
            continue
        try:
            wait_for_obscuring_elements(mydriver, args.verbose)
            keywords_form = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".select2-search__field")))
            keywords_form.click()
            keywords_form.send_keys(keyword)
            #sleep(2)
            wait_for_obscuring_elements(mydriver, args.verbose)
            #keyword_sugg = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-results__option")))
            # find the list element, taking care to match the exact text [suggestion from user sefk]:
            keyword_sugg = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.XPATH, f"//li[contains(@class, 'select2-results__option') and text()='{keyword}']")))
            wait_for_obscuring_elements(mydriver, args.verbose)
            keyword_sugg.click()
        except Exception as e:
            error_msg = f"Problem with keywords: {str(e)}"
            verbose_print(f"\n⚠ There was a problem with the keywords! Please check if one or more are missing in the form and fill them in manually.\n Problem:", args.verbose)
            print(f"\n⚠ {error_msg}")
            print("\nFull traceback:")
            print(traceback.format_exc())
            row_errors.append(error_msg)


    # --- Geographic Coverage

    geographic_coverage_text = datadict["9_geographic_coverage"]
    if len(geographic_coverage_text) != 0 and geographic_coverage_text != " ":
        # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey1.0.5:$dcterms_location_0_0.0.0.0.0.2.0.1"> edit</span>
        #   css-sel: #edit-dcterms_location_0 > span:nth-child(1) > span:nth-child(2)
        geogr_cov_edit = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-dcterms_location_0 > span:nth-child(1) > span:nth-child(2)")))
        verbose_print("edit-button geogr_cov_form found", args.verbose)
        wait_for_obscuring_elements(mydriver, args.verbose)
        geogr_cov_edit.click()
        # form: <input type="text" class="form-control input-sm" style="padding-right: 24px;">
        #   .editable-input > input:nth-child(1)
        geogr_cov_form = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".editable-input > input:nth-child(1)")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        geogr_cov_form.send_keys(geographic_coverage_text)
        geogr_cov_form.submit()


    # --- Time Period

    timeperiod_start_text = datadict["10_time_period1"]
    timeperiod_end_text = datadict["10_time_period2"]
    if len(timeperiod_start_text) != 0 or len(timeperiod_end_text) != 0:
        # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey2.0.2.2"> add value</span>
        #   #groupAttr1 > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)
        time_period_add_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#groupAttr1 > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)")))
        verbose_print("time_period_add_btn found", args.verbose)
        wait_for_obscuring_elements(mydriver, args.verbose)
        time_period_add_btn.click()
        # start: <input type="text" class="form-control" name="startDate" id="startDate" required="" placeholder="YYYY-MM-DD or YYYY-MM or YYYY" title="Enter as YYYY-MM-DD or YYYY-MM or YYYY" value="" data-reactid=".4.0.0.1.1.0.1.0">
        #   #startDate
        time_period_start = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#startDate")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        time_period_start.send_keys(timeperiod_start_text)
        # <input type="text" class="form-control" name="endDate" id="endDate" placeholder="YYYY-MM-DD or YYYY-MM or YYYY" title="Enter as YYYY-MM-DD or YYYY-MM or YYYY" value="" data-reactid=".4.0.0.1.1.1.1.0">
        #   #endDate
        time_period_end = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#endDate")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        time_period_end.send_keys(timeperiod_end_text)
        # <button type="button" class="btn btn-primary save-dates" data-reactid=".4.0.0.1.1.3.0.0">Save &amp; Apply</button>
        #    .save-dates
        save_time_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".save-dates")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        save_time_btn.click()


    # --- Data types

    datatype_to_select = datadict["11_data_types"]
    if len(datatype_to_select) != 0 and datatype_to_select != " ":
        # <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey5.$disco_kindOfData_0.1.0.0.0.2.1"> edit</span>
        #   #disco_kindOfData_0 > span:nth-child(2)
        datatypes_edit_btn = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#disco_kindOfData_0 > span:nth-child(2)")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        datatypes_edit_btn.click()
        wait_for_obscuring_elements(mydriver, args.verbose)
        # <span> geographic information system (GIS) data</span>  # (there is a space character at the beginning of the string!)
        #   .editable-checklist > div:nth-child(8) > label:nth-child(1) > span:nth-child(2)
        datatype_text = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.XPATH, f"//span[contains(text(), '{datatype_to_select}')]")))
        datatype_text.click()
        # <button type="submit" class="btn btn-primary btn-sm editable-submit"><i class="glyphicon glyphicon-ok"></i> save</button>
        #   .editable-submit
        datatypes_save_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".editable-submit")))
        datatypes_save_btn.click()


    # --- Collection Notes

    if len(datadict["12_collection_notes"]) != 0 or len(datadict["12_download_date_original_source"]) != 0:
        # check if there is data in the date field (otherwise set it to empty string):
        downloaddate = f"(Downloaded {datadict['12_download_date_original_source']})" if len(datadict["12_download_date_original_source"]) != 0 else ""
        # the text for collection notes is the note and the download date, if the note cell in the csv file isn't empty (otherwise it's only the date):
        text_for_collectionnotes = datadict["12_collection_notes"] + " " + downloaddate if len(datadict["12_collection_notes"]) != 0 and datadict["12_collection_notes"] != " " else downloaddate
        # css-sel.: #edit-imeta_collectionNotes_0 > span:nth-child(2)
        coll_notes_edit_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-imeta_collectionNotes_0 > span:nth-child(2)")))
        wait_for_obscuring_elements(mydriver, args.verbose)
        coll_notes_edit_btn.click()
        # The WYSIWYG editor is inside an iframe with class "wysihtml5-sandbox"
        #   First, find and switch to the iframe
        wysihtml5_iframe = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "iframe.wysihtml5-sandbox")))
        mydriver.switch_to.frame(wysihtml5_iframe)
        # Now find the body element inside the iframe
        coll_notes_form = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "body")))
        # Click to focus the contenteditable element
        coll_notes_form.click()
        sleep(0.3)
        # Clear any existing content
        coll_notes_form.send_keys(Keys.CONTROL + "a")
        sleep(0.2)
        # Use JavaScript to set the text content (more reliable for contenteditable elements)
        mydriver.execute_script("arguments[0].textContent = arguments[1];", coll_notes_form, text_for_collectionnotes)
        # Trigger input event to ensure the editor recognizes the change
        mydriver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", coll_notes_form)
        sleep(0.3)
        # Switch back to default content before clicking save button (which is outside iframe)
        mydriver.switch_to.default_content()
        wait_for_obscuring_elements(mydriver, args.verbose)
        # css-sel: .editable-submit
        coll_notes_save_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".editable-submit")))
        coll_notes_save_btn.click()


    # --- Upload files

    if len(datadict["path"]) != 0 and datadict["path"] != " ":
        # upload-button: <span data-reactid=".0.3.1.1.0.0.0.0.0.0.1.2.3">Upload Files</span>
        #   a.btn-primary:nth-child(3) > span:nth-child(4)
        wait_for_obscuring_elements(mydriver, args.verbose)
        upload_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-primary:nth-child(3) > span:nth-child(4)")))
        upload_btn.click()
        wait_for_obscuring_elements(mydriver, args.verbose)
        fileupload_field = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".col-md-offset-2 > span:nth-child(1)")))

        filepaths_to_upload = get_paths_uploadfiles(args.folder_path_uploadfiles, datadict["path"])
        if len(filepaths_to_upload) != 2:
            warning_msg = f"The number of files to upload is not 2: {len(filepaths_to_upload)} workspace id {workspace_id}"
            verbose_print(f"⚠ {warning_msg}", args.verbose)
            return
        verbose_print(f"\nFiles that will be uploaded: {[os.path.basename(f) for f in filepaths_to_upload]}\n", args.verbose)
        for singlefile in filepaths_to_upload:
            try:
                drag_and_drop_file(fileupload_field, singlefile)
            except Exception as e:
                error_msg = f"Error uploading file '{os.path.basename(singlefile)}': {str(e)}"
                verbose_print(f"⚠ {error_msg}", args.verbose)
                print(f"⚠ {error_msg}")
                print(f"Full path: {singlefile}")
                print("\nFull traceback:")
                print(traceback.format_exc())
                row_errors.append(error_msg)
                raise  # Re-raise to stop processing

        # when a file is uploaded and its progress bar is complete, a text appears: "File added to queue for upload."
        #   To check that the files are completey uploaded, this text has to be there as often as the number of files:
        filecount = len(filepaths_to_upload)
        verbose_print(f"filecount: {filecount}", args.verbose)
        # wait until the text has appeared as often as there are files:
        #   (to wait longer for uploads to be completed, change the number in WebDriverWait(mydriver, ...) - it is the waiting time in seconds)
        WebDriverWait(mydriver, 2000).until(lambda x: True if len(mydriver.find_elements(By.XPATH, "//span[text()='File added to queue for upload.']")) == filecount else False)
        verbose_print("\nEverything should be uploaded completely now.\n", args.verbose)


        # close-btn: .importFileModal > div:nth-child(3) > button:nth-child(1)
        wait_for_obscuring_elements(mydriver, args.verbose)
        close_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".importFileModal > div:nth-child(3) > button:nth-child(1)")))
        close_btn.click()
        
    return workspace_id


def column_index_to_letter(col_index):
    """
    Convert a 1-based column index to a column letter (e.g., 1 -> A, 2 -> B, 27 -> AA).
    
    Args:
        col_index: 1-based column index
    
    Returns:
        Column letter (e.g., 'A', 'B', 'AA')
    """
    result = ""
    while col_index > 0:
        col_index -= 1
        result = chr(65 + (col_index % 26)) + result
        col_index //= 26
    return result


def get_column_mapping(service, sheet_id, sheet_name, required_columns, verbose=False):
    """
    Read the first row of a Google Sheet and create a mapping from column names to column letters.
    
    Args:
        service: Google Sheets API service object
        sheet_id: Google Sheet ID
        sheet_name: Name of the worksheet/tab
        required_columns: List of required column names to find
        verbose: Whether to print verbose messages
    
    Returns:
        Dictionary mapping column names to column letters (e.g., {'URL': 'G', 'Claimed': 'B'})
        Returns None if a required column is missing
    """
    try:
        # Read the first row (header row)
        range_name = f"{sheet_name}!1:1"
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        if not values or len(values) == 0:
            return None
        
        header_row = values[0]
        
        # Create mapping from column name to column letter
        column_map = {}
        for idx, col_name in enumerate(header_row):
            if col_name and str(col_name).strip():
                col_letter = column_index_to_letter(idx + 1)
                column_map[str(col_name).strip()] = col_letter
                verbose_print(f"  Found column '{col_name}' at {col_letter}", verbose)
        
        # Check for required columns (case-insensitive, partial match)
        missing_columns = []
        found_columns = {}
        
        for required_col in required_columns:
            found = False
            # Try exact match first (case-insensitive)
            for col_name, col_letter in column_map.items():
                if col_name.lower() == required_col.lower():
                    found_columns[required_col] = col_letter
                    found = True
                    break
            
            # Try partial match if exact match not found
            if not found:
                for col_name, col_letter in column_map.items():
                    if required_col.lower() in col_name.lower() or col_name.lower() in required_col.lower():
                        found_columns[required_col] = col_letter
                        found = True
                        break
            
            if not found:
                missing_columns.append(required_col)
        
        if missing_columns:
            error_msg = f"\n✗ ERROR: Required columns not found in Google Sheet '{sheet_name}':\n"
            for col in missing_columns:
                error_msg += f"   - {col}\n"
            error_msg += f"\nAvailable columns in the sheet:\n"
            for col_name, col_letter in sorted(column_map.items()):
                error_msg += f"   - {col_name} ({col_letter})\n"
            print(error_msg)
            raise ValueError(f"Required columns missing from Google Sheet '{sheet_name}'. See error message above.")
        
        return found_columns
        
    except Exception as e:
        print(f"✗ ERROR: Failed to read column headers from Google Sheet: {str(e)}")
        return None


def find_row_by_url(service, sheet_id, sheet_name, url_column_letter, source_url, verbose=False):
    """
    Find the row number in Google Sheet by matching URL in the specified column.
    
    Args:
        service: Google Sheets API service object
        sheet_id: Google Sheet ID
        sheet_name: Name of the worksheet/tab
        url_column_letter: Column letter containing URLs (e.g., "G")
        source_url: URL to search for
        verbose: Whether to print verbose messages
    
    Returns:
        Row number (1-indexed) if found, None otherwise
    """
    try:
        # Read all values from the URL column (starting from row 2 to skip header)
        range_name = f"{sheet_name}!{url_column_letter}2:{url_column_letter}"
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        # Search for matching URL (case-insensitive, handle partial matches)
        source_url_clean = source_url.strip().lower()
        for idx, row in enumerate(values):
            if row and len(row) > 0:
                cell_url = str(row[0]).strip().lower()
                # Check for exact match or if source_url is contained in cell_url
                if source_url_clean == cell_url or source_url_clean in cell_url or cell_url in source_url_clean:
                    # Row number is idx + 2 (idx is 0-based, +1 for header, +1 for 1-indexing)
                    row_num = idx + 2
                    verbose_print(f"  Found matching URL in row {row_num}: {row[0]}", verbose)
                    return row_num
        
        return None
        
    except Exception as e:
        verbose_print(f"  Error searching for URL: {str(e)}", verbose)
        return None


def update_google_sheet(sheet_id, credentials_path, sheet_name, source_url, workspace_id, datadict, username='mkraley', verbose=False):
    """
    Update a Google Sheet with publishing results by finding row via URL match.
    Uses column names from the first row instead of fixed column positions.
    
    Args:
        sheet_id: Google Sheet ID (from URL)
        credentials_path: Path to service account credentials JSON file
        sheet_name: Name of the worksheet/tab
        source_url: Source URL to match against URL column
        workspace_id: Workspace ID for creating download location URL
        datadict: Dictionary containing CSV row data
        username: Username to write in "Claimed" column (default: 'mkraley')
        verbose: Whether to print verbose messages
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    if not GOOGLE_SHEETS_AVAILABLE:
        return False, "Google Sheets API libraries not installed. Install with: pip install google-api-python-client google-auth google-auth-httplib2"
    
    if not sheet_id or not credentials_path:
        return False, "Google Sheet ID and credentials path are required"
    
    if not source_url:
        return False, "Source URL is required to find matching row"
    
    try:
        # Authenticate using service account
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = build('sheets', 'v4', credentials=credentials)
        
        # Define required columns with their search names
        required_columns = [
            'URL',  # Column to match source_url against
            'Claimed',  # or "Claimed (add your name)"
            'Data Added',  # or "Data Added (Y/N/IP)"
            'Dataset Download Possible?',
            'Nominated to EOT / USGWDA',  # or "Nominated"
            'Date Downloaded',
            'Download Location',
            'Dataset Size',
            'File extensions of data uploads',  # or "File extensions"
            'Metadata availability info'  # or "Metadata"
        ]
        
        # Get column mapping from sheet headers
        verbose_print(f"  Reading column headers from sheet '{sheet_name}'...", verbose)
        column_map = get_column_mapping(service, sheet_id, sheet_name, required_columns, verbose)
        
        if not column_map:
            error_msg = "Failed to get column mapping from Google Sheet. Check column names."
            print(f"\n✗ {error_msg}")
            return False, error_msg
        
        # Find row by matching URL
        url_col_letter = column_map.get('URL')
        if not url_col_letter:
            error_msg = "Could not find URL column in sheet"
            print(f"\n✗ {error_msg}")
            return False, error_msg
        
        verbose_print(f"  Searching for URL in column {url_col_letter}: {source_url}", verbose)
        row_number = find_row_by_url(service, sheet_id, sheet_name, url_col_letter, source_url, verbose)
        
        if not row_number:
            error_msg = f"Could not find row with matching URL: {source_url}"
            verbose_print(f"  ⚠ {error_msg}", verbose)
            return False, error_msg
        
        verbose_print(f"  Found matching row: {row_number}", verbose)
        
        # Prepare update requests for all columns
        update_requests = []
        
        # Claimed (add your name)
        claimed_col = column_map.get('Claimed')
        if claimed_col:
            update_requests.append({
                'range': f"{sheet_name}!{claimed_col}{row_number}",
                'values': [[username]]
            })
        
        # Data Added (Y/N/IP)
        data_added_col = column_map.get('Data Added')
        if data_added_col:
            update_requests.append({
                'range': f"{sheet_name}!{data_added_col}{row_number}",
                'values': [['Y']]
            })
        
        # Dataset Download Possible?
        download_possible_col = column_map.get('Dataset Download Possible?')
        if download_possible_col:
            update_requests.append({
                'range': f"{sheet_name}!{download_possible_col}{row_number}",
                'values': [['Y']]
            })
        
        # Nominated to EOT / USGWDA
        nominated_col = column_map.get('Nominated to EOT / USGWDA')
        if nominated_col:
            update_requests.append({
                'range': f"{sheet_name}!{nominated_col}{row_number}",
                'values': [['Y']]
            })
        
        # Date Downloaded
        date_downloaded_col = column_map.get('Date Downloaded')
        if date_downloaded_col:
            download_date = datadict.get("12_download_date_original_source", "").strip()
            if download_date:
                update_requests.append({
                    'range': f"{sheet_name}!{date_downloaded_col}{row_number}",
                    'values': [[download_date]]
                })
        
        # Download Location
        download_location_col = column_map.get('Download Location')
        if download_location_col and workspace_id:
            download_location = f"https://www.datalumos.org/datalumos/project/{workspace_id}/version/V1/view"
            update_requests.append({
                'range': f"{sheet_name}!{download_location_col}{row_number}",
                'values': [[download_location]]
            })
        
        # Dataset Size
        dataset_size_col = column_map.get('Dataset Size')
        if dataset_size_col:
            dataset_size = datadict.get("dataset_size", "").strip()
            if dataset_size:
                update_requests.append({
                    'range': f"{sheet_name}!{dataset_size_col}{row_number}",
                    'values': [[dataset_size]]
                })
        
        # File extensions of data uploads
        file_extensions_col = column_map.get('File extensions of data uploads')
        if file_extensions_col:
            file_extensions = datadict.get("file_extensions", "").strip()
            if file_extensions:
                update_requests.append({
                    'range': f"{sheet_name}!{file_extensions_col}{row_number}",
                    'values': [[file_extensions]]
                })
        
        # Metadata availability info
        metadata_col = column_map.get('Metadata availability info')
        if metadata_col:
            update_requests.append({
                'range': f"{sheet_name}!{metadata_col}{row_number}",
                'values': [['Y']]
            })
        
        if not update_requests:
            return False, "No data to update"
        
        # Batch update
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': update_requests
        }
        
        result = service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body=body
        ).execute()
        
        verbose_print(f"✓ Successfully updated Google Sheet row {row_number} with {len(update_requests)} columns", verbose)
        return True, None
        
    except ValueError:
        # Re-raise ValueError (missing columns) to stop execution
        raise
    except FileNotFoundError:
        error_msg = f"Credentials file not found: {credentials_path}"
        verbose_print(f"⚠ {error_msg}", verbose)
        return False, error_msg
    except HttpError as e:
        error_msg = f"Google Sheets API error: {str(e)}"
        verbose_print(f"⚠ {error_msg}", verbose)
        return False, error_msg
    except Exception as e:
        error_msg = f"Error updating Google Sheet: {str(e)}"
        verbose_print(f"⚠ {error_msg}", verbose)
        verbose_print(traceback.format_exc(), verbose)
        return False, error_msg


def publish_workspace(mydriver, current_row=None, verbose=False):
    """
    Execute the publishing workflow for a workspace.
    
    Args:
        mydriver: WebDriver instance
        current_row: Current row number being processed (for error messages)
        verbose: Whether to print verbose messages
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    verbose_print("\nStarting publish workflow...", verbose)
    
    # Attempt publishing with retry logic (one retry after 5 seconds if error occurs)
    for attempt in range(2):  # Try twice: original attempt + one retry
        if attempt > 0:
            row_info = f"Row {current_row}: " if current_row else ""
            print(f"{row_info}Publish workflow failed, waiting 5 seconds before retry...", verbose)
            sleep(5)
            print(f"{row_info}Retrying publish workflow...", verbose)
        
        try:
            # Step 1: Click "Publish Project" button
            # <button type="submit" class="btn btn-primary btn-sm" ...>Publish Project</button>
            publish_project_btn = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-primary') and contains(., 'Publish Project')]"))
            )
            verbose_print("Found 'Publish Project' button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            publish_project_btn.click()
            
            # Wait for navigation to review/publish page
            try:
                WebDriverWait(mydriver, 30).until(
                    lambda d: 'reviewPublish' in d.current_url
                )
                verbose_print(f"Navigated to review/publish page: {mydriver.current_url}", verbose)
                sleep(1)
            except TimeoutException:
                # If timeout waiting for reviewPublish, check for error message (same logic as line 1404)
                verbose_print("Timeout waiting for reviewPublish page, checking for error message...", verbose)
                error_msg_divs = mydriver.find_elements(By.ID, 'errormsg')
                if len(error_msg_divs) > 0:
                    error_msg_div = error_msg_divs[0]
                    if len(error_msg_div.text) > 0:
                        error_text = error_msg_div.text
                        row_info = f"Row {current_row}: " if current_row else ""
                        error_message = f"{row_info}Error message detected: {error_text}"
                        print(f"✗ {error_message}")
                        raise BatchRestartException(error_message, None)
                # If no error message found, re-raise the timeout exception
                raise
            
            # Step 2: Click "Proceed to Publish" button
            # <button type="submit" class="btn btn-primary btn-sm" ...>Proceed to Publish</button>
            proceed_publish_btn = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-primary') and contains(., 'Proceed to Publish')]"))
            )
            verbose_print("Found 'Proceed to Publish' button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            proceed_publish_btn.click()
            sleep(1)
            
            # Step 3: In the dialog, select options
            # Radio button: <input type="radio" name="disclosure" id="noDisclosure" ...>
            no_disclosure_radio = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.ID, "noDisclosure"))
            )
            verbose_print("Found 'noDisclosure' radio button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            no_disclosure_radio.click()
            sleep(0.5)
            
            # Radio button: <input type="radio" name="sensitive" id="sensitiveNo" ...>
            sensitive_no_radio = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.ID, "sensitiveNo"))
            )
            verbose_print("Found 'sensitiveNo' radio button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            sensitive_no_radio.click()
            sleep(0.5)
            
            # Checkbox: <input type="checkbox" id="depositAgree" ...>
            deposit_agree_checkbox = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.ID, "depositAgree"))
            )
            verbose_print("Found 'depositAgree' checkbox", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            deposit_agree_checkbox.click()
            sleep(0.5)
            
            # Step 4: Click "Publish Data" button
            # <button type="button" class="btn btn-primary" ...>Publish Data</button>
            publish_data_btn = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-primary') and contains(., 'Publish Data')]"))
            )
            verbose_print("Found 'Publish Data' button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            publish_data_btn.click()
            sleep(2)
            
            # Step 5: Click "Back to Project" button
            # <button type="button" class="btn btn-primary" ...>Back to Project</button>
            back_to_project_btn = WebDriverWait(mydriver, 50).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn-primary') and contains(., 'Back to Project')]"))
            )
            verbose_print("Found 'Back to Project' button", verbose)
            wait_for_obscuring_elements(mydriver, verbose)
            back_to_project_btn.click()
            sleep(2)
            
            # Wait for navigation back to workspace
            WebDriverWait(mydriver, 30).until(
                lambda d: '/datalumos/' in d.current_url and 'reviewPublish' not in d.current_url
            )
            verbose_print(f"Returned to workspace: {mydriver.current_url}", verbose)
            
            # Check for error message div
            error_msg_divs = mydriver.find_elements(By.ID, 'errormsg')
            if len(error_msg_divs) > 0:
                error_msg_div = error_msg_divs[0]
                if len(error_msg_div.text) > 0:
                    error_text = error_msg_div.text
                    row_info = f"Row {current_row}: " if current_row else ""
                    error_message = f"{row_info}Error message detected: {error_text}"
                    print(f"✗ {error_message}")
                    raise BatchRestartException(error_message, None)
            
            verbose_print("✓ Publishing workflow completed successfully", verbose)
            return True, None
            
        except Exception as e:
            row_info = f"Row {current_row}: " if current_row else ""
            error_msg = f"{row_info}Error during publishing workflow: {str(e)}"
            verbose_print(f"⚠ {error_msg}", verbose)
            print(f"⚠ {error_msg}")
            print("\nFull traceback:")
            print(traceback.format_exc())
            
            # If this was the first attempt, we'll retry once
            if attempt == 0:
                continue  # Retry once
            else:
                # Second attempt also failed, return error
                return False, error_msg
    
    # Should not reach here, but just in case
    row_info = f"Row {current_row}: " if current_row else ""
    return False, f"{row_info}Publishing workflow failed after retry"


def nominate_url_to_gwda(mydriver, source_url, your_name, institution, email, verbose=False):
    """
    Nominate a URL to the U.S. Government Web & Data Archive (GWDA).
    
    Args:
        mydriver: WebDriver instance
        source_url: The URL to nominate
        your_name: Name to enter in the nomination form
        institution: Institution to enter in the nomination form
        email: Email address to enter in the nomination form
        verbose: Whether to print verbose messages
    
    Returns:
        Tuple of (success: bool, error_message: str or None)
    """
    if not source_url or source_url.strip() == "":
        return False, "Source URL is empty, cannot nominate"
    
    verbose_print(f"\nNominating URL to GWDA: {source_url}", verbose)
    
    try:
        # Navigate to the nomination page
        nomination_url = "https://digital2.library.unt.edu/nomination/GWDA-US-2025/add/"
        verbose_print(f"Navigating to: {nomination_url}", verbose)
        mydriver.get(nomination_url)
        sleep(2)
        
        # Find and fill in the URL input field
        # <input type="text" name="url_value" id="url-value" class="form-control" required="" ...>
        url_input = WebDriverWait(mydriver, 30).until(
            EC.presence_of_element_located((By.ID, "url-value"))
        )
        verbose_print("Found URL input field", verbose)
        url_input.clear()
        url_input.send_keys(source_url)
        verbose_print(f"Entered URL: {source_url}", verbose)
        sleep(0.5)
        
        # Find and fill in the "Your Name" field
        # <input type="text" name="nominator_name" id="your-name-value" class="form-control" ...>
        name_input = WebDriverWait(mydriver, 30).until(
            EC.presence_of_element_located((By.ID, "your-name-value"))
        )
        verbose_print("Found 'Your Name' input field", verbose)
        name_input.clear()
        name_input.send_keys(your_name)
        verbose_print(f"Entered name: {your_name}", verbose)
        sleep(0.5)
        
        # Find and fill in the "Institution" field
        # <input type="text" name="nominator_institution" id="institution-value" class="form-control" ...>
        institution_input = WebDriverWait(mydriver, 30).until(
            EC.presence_of_element_located((By.ID, "institution-value"))
        )
        verbose_print("Found 'Institution' input field", verbose)
        institution_input.clear()
        institution_input.send_keys(institution)
        verbose_print(f"Entered institution: {institution}", verbose)
        sleep(0.5)
        
        # Find and fill in the "Email" field
        # <input type="text" name="nominator_email" id="email-value" class="form-control" ...>
        email_input = WebDriverWait(mydriver, 30).until(
            EC.presence_of_element_located((By.ID, "email-value"))
        )
        verbose_print("Found 'Email' input field", verbose)
        email_input.clear()
        email_input.send_keys(email)
        verbose_print(f"Entered email: {email}", verbose)
        sleep(0.5)
        
        # Find and click the submit button
        # <input type="submit" value="submit" class="btn btn-primary" ...>
        submit_btn = WebDriverWait(mydriver, 30).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "input[type='submit'][value='submit']"))
        )
        verbose_print("Found submit button", verbose)
        wait_for_obscuring_elements(mydriver, verbose)
        submit_btn.click()
        sleep(2)
        
        verbose_print("✓ Successfully nominated URL to GWDA", verbose)
        return True, None
        
    except Exception as e:
        error_msg = f"Error nominating URL to GWDA: {str(e)}"
        verbose_print(f"⚠ {error_msg}", verbose)
        verbose_print(traceback.format_exc(), verbose)
        return False, error_msg


def process_single_row(mydriver, args, current_row, batch_num, total_rows):
    """
    Process a single row from the CSV file.
    
    Args:
        mydriver: WebDriver instance
        args: Parsed command-line arguments
        current_row: Row number to process (1-indexed)
        batch_num: Current row number in overall sequence (1-indexed)
        total_rows: Total number of rows to process
    
    Returns:
        None (errors and warnings are handled internally)
    """
    # Track errors and warnings for this row
    row_errors = []
    row_warnings = []
    workspace_id = None
    source_url = None

    try:
        datadict = read_csv_line(args.csv_file_path, current_row)
        verbose_print(f"\n{datadict}", args.verbose)
        verbose_print("\n----------------------------", args.verbose)
        
        # Get source URL for summary
        source_url = datadict.get("7_original_distribution_url", "")
        
        # Handle only-publish mode: navigate directly to workspace
        if args.publish_mode == 'only-publish':
            # Read workspace ID from CSV (datalumos_id column)
            workspace_id_str = datadict.get('datalumos_id', '').strip()
            if not workspace_id_str:
                error_msg = f"Row {current_row}: No workspace ID found in datalumos_id column. Cannot publish."
                row_errors.append(error_msg)
                if not args.verbose:
                    print(f"[{batch_num}/{total_rows}] Workspace ID: N/A | Source URL: {source_url if source_url else 'N/A'}")
                    print(f"  ✗ ERROR: {error_msg}")
                else:
                    verbose_print(f"✗ {error_msg}", args.verbose)
                return
            
            # Convert workspace ID to integer (handles cases like '123456.0' from CSV)
            try:
                workspace_id = int(float(workspace_id_str))
            except (ValueError, TypeError):
                error_msg = f"Row {current_row}: Invalid workspace ID format: '{workspace_id_str}'. Expected a number."
                row_errors.append(error_msg)
                if not args.verbose:
                    print(f"[{batch_num}/{total_rows}] Workspace ID: N/A | Source URL: {source_url if source_url else 'N/A'}")
                    print(f"  ✗ ERROR: {error_msg}")
                else:
                    verbose_print(f"✗ {error_msg}", args.verbose)
                return
            
            # Navigate directly to workspace
            workspace_url = f"https://www.datalumos.org/datalumos/workspace?goToPath=/datalumos/{workspace_id}&goToLevel=project"
            verbose_print(f"Navigating to workspace: {workspace_url}", args.verbose)
            mydriver.get(workspace_url)
            wait_for_obscuring_elements(mydriver, args.verbose)
            sleep(2)
            verbose_print(f"Processing row {current_row} (only-publish mode), Workspace ID: {workspace_id}\n", args.verbose)
        else:
            # Normal mode: create project and fill forms
            verbose_print(f"Processing row {current_row}, Title: {datadict['4_title']}\n", args.verbose)
            workspace_id = fill_project_forms(mydriver, datadict, args, row_errors, row_warnings)

        # --- Publish Project
        
        publish_success = False
        if args.publish_mode != 'no-publish':
            publish_success, publish_error = publish_workspace(mydriver, current_row, args.verbose)
            if not publish_success:
                row_warnings.append(publish_error)
        else:
            verbose_print("Skipping publish workflow (no-publish mode)", args.verbose)

        # Update Google Sheet with publishing results (if configured)
        if args.google_sheet_id and args.google_credentials and publish_success and workspace_id:
            try:
                gsheet_success, gsheet_error = update_google_sheet(
                    args.google_sheet_id,
                    args.google_credentials,
                    args.google_sheet_name,
                    source_url,
                    workspace_id,
                    datadict,
                    username=args.google_username,
                    verbose=args.verbose
                )
                if not gsheet_success:
                    error_msg = f"Google Sheet update failed: {gsheet_error}"
                    row_errors.append(error_msg)
                    verbose_print(f"⚠ {error_msg}", args.verbose)
            except ValueError:
                # Missing required columns - stop execution
                raise
            except Exception as e:
                error_msg = f"Error updating Google Sheet: {str(e)}"
                verbose_print(f"⚠ {error_msg}", args.verbose)
                print(f"⚠ {error_msg}")
                print("\nFull traceback:")
                print(traceback.format_exc())
                row_errors.append(error_msg)

        # Update CSV with workspace ID
        if workspace_id:
            try:
                update_csv_workspace_id(args.csv_file_path, current_row, workspace_id)
                verbose_print(f"✓ Updated CSV row {current_row} with workspace ID: {workspace_id}", args.verbose)
            except Exception as e:
                error_msg = f"Failed to update CSV with workspace ID: {str(e)}"
                verbose_print(f"⚠ {error_msg}", args.verbose)
                print(f"⚠ {error_msg}")
                print("\nFull traceback:")
                print(traceback.format_exc())
                row_errors.append(error_msg)
        
        # Nominate URL to GWDA (U.S. Government Web & Data Archive)
        if source_url:
            try:
                # Use args.username as default for GWDA email if not provided
                gwda_email = args.gwda_email if args.gwda_email else args.username
                if not gwda_email:
                    row_warnings.append("GWDA nomination skipped: No email provided (neither --GWDA-email nor --username)")
                    verbose_print("⚠ GWDA nomination skipped: No email provided", args.verbose)
                else:
                    gwda_success, gwda_error = nominate_url_to_gwda(
                        mydriver, 
                        source_url, 
                        args.gwda_your_name,
                        args.gwda_institution,
                        gwda_email,
                        args.verbose
                    )
                    if not gwda_success:
                        row_warnings.append(f"GWDA nomination failed: {gwda_error}")
                        verbose_print(f"⚠ GWDA nomination failed: {gwda_error}", args.verbose)
            except Exception as e:
                error_msg = f"Error nominating URL to GWDA: {str(e)}"
                verbose_print(f"⚠ {error_msg}", args.verbose)
                print(f"⚠ {error_msg}")
                print("\nFull traceback:")
                print(traceback.format_exc())
                row_warnings.append(error_msg)
        
        # Print summary line (non-verbose mode) or detailed output (verbose mode)
        if not args.verbose:
            # Summary format: [batch_num/total_rows] Workspace ID: {workspace_id} | Source URL: {source_url}
            workspace_display = workspace_id if workspace_id else "N/A"
            source_url_display = source_url if source_url else "N/A"
            print(f"[{batch_num}/{total_rows}] Workspace ID: {workspace_display} | Source URL: {source_url_display}")
            
            # Print errors/warnings below the summary line if any
            if row_errors:
                for error in row_errors:
                    print(f"  ✗ ERROR: {error}")
            if row_warnings:
                for warning in row_warnings:
                    print(f"  ⚠ WARNING: {warning}")
        else:
            # In verbose mode, print detailed completion message
            if workspace_id:
                verbose_print(f"✓ Completed processing row {current_row}. Workspace ID: {workspace_id}", args.verbose)
            else:
                verbose_print(f"⚠ Completed processing row {current_row}, but workspace ID was not extracted.", args.verbose)
    
    except Exception as e:
        error_msg = f"Error processing row {current_row}: {str(e)}"
        if args.verbose:
            verbose_print(f"\n✗ {error_msg}", args.verbose)
        # Always print full Python traceback for debugging
        print(f"\n✗ {error_msg}")
        print("\nFull traceback:")
        print(traceback.format_exc())
        row_errors.append(error_msg)
        if not args.verbose:
            workspace_display = workspace_id if workspace_id else "N/A"
            source_url_display = source_url if source_url else "N/A"
            print(f"[{batch_num}/{total_rows}] Workspace ID: {workspace_display} | Source URL: {source_url_display}")
            print(f"  ✗ ERROR: {error_msg}")
        
        # Try to update CSV even if there was an error (if we got a workspace ID)
        if workspace_id:
            try:
                update_csv_workspace_id(args.csv_file_path, current_row, workspace_id)
            except:
                pass  # Already logged the error above


def main():
    """Main execution function."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Check CSV writability before starting
    print("\n" + "=" * 80)
    print("Checking CSV File Writability")
    print("=" * 80)
    csv_writable, csv_error = check_csv_writability(args.csv_file_path)
    if not csv_writable:
        print(f"✗ {csv_error}")
        print("\nPlease fix the CSV file issue before proceeding.")
        return
    else:
        print(f"✓ CSV file is writable: {args.csv_file_path}\n")
    
    # Determine which rows to process
    if args.rows:
        # Use specific rows from --rows parameter
        rows_to_process = args.rows
        total_rows = len(rows_to_process)
    else:
        # Use range from --start-row to --end-row
        rows_to_process = list(range(args.start_row, args.end_row + 1))
        total_rows = args.end_row - args.start_row + 1
    
    # Split rows into batches
    batch_size = 5
    batches = [rows_to_process[i:i + batch_size] for i in range(0, len(rows_to_process), batch_size)]
    total_batches = len(batches)
    
    print("If you upload from USB device: MAKE SURE THE USB IS PLUGGED IN!\n")
    print(f"Processing {total_rows} rows in {total_batches} batch(es) of up to {batch_size} rows each.\n")
    
    # Process each batch
    for batch_index, batch_rows in enumerate(batches, start=1):
        print(f"\n{'=' * 80}")
        print(f"Starting Batch {batch_index}/{total_batches} (rows {batch_rows[0]}-{batch_rows[-1]})")
        print(f"{'=' * 80}\n")
        
        # Initialize browser for this batch
        mydriver = None
        try:
            mydriver = initialize_browser(args.browser)
            
            # Automated sign-in
            print("\n" + "-" * 80)
            print("DataLumos Automated Sign-In")
            print("-" * 80)
            signin_success, signin_message = sign_in(mydriver, args.username, args.password)
            if not signin_success:
                print(f"✗ {signin_message}")
                print("Please check the browser and complete login manually if needed.")
                input("Press Enter to continue after manual login...")
            else:
                print(f"✓ {signin_message}\n")
            
            # Process each row in this batch
            batch_restart_needed = False
            remaining_rows = []
            for row_index_in_batch, current_row in enumerate(batch_rows, start=1):
                batch_num = (batch_index - 1) * batch_size + row_index_in_batch
                try:
                    process_single_row(mydriver, args, current_row, batch_num, total_rows)
                except BatchRestartException as e:
                    # Error message already logged, close browser and restart batch with remaining rows
                    print(f"\nBatch restart required. Closing browser...")
                    if mydriver:
                        try:
                            mydriver.quit()
                        except:
                            pass
                    
                    # Get remaining rows from this batch (row_index_in_batch is 1-based, so slice from that index)
                    remaining_rows = batch_rows[row_index_in_batch:]
                    batch_restart_needed = True
                    # Exit the current batch loop
                    break

            # Close browser after batch is complete (if we didn't break due to BatchRestartException)
            if not batch_restart_needed:
                print(f"\nBatch {batch_index}/{total_batches} complete. Closing browser...")
                if mydriver:
                    mydriver.quit()
                print(f"✓ Browser closed after batch {batch_index}\n")
            else:
                # Insert remaining rows as a new batch (using 0-based index)
                if remaining_rows:
                    batches.insert(batch_index, remaining_rows)
                    total_batches = len(batches)
                    print(f"Restarting batch with remaining rows: {remaining_rows}")
                    # Continue outer loop to process the new batch
                    continue
        
        except BatchRestartException as e:
            # This shouldn't happen here since we catch it in the inner loop, but just in case
            error_msg = str(e)
            print(f"\n✗ Batch restart exception: {error_msg}")
            print("\nFull traceback:")
            print(traceback.format_exc())
            if mydriver:
                try:
                    mydriver.quit()
                except:
                    pass
        except Exception as e:
            error_msg = str(e)
            print(f"\n✗ Error during batch {batch_index}: {error_msg}")
            # Always print full traceback for debugging
            print("\nFull traceback:")
            print(traceback.format_exc())
            # Close browser even if there was an error
            if mydriver:
                try:
                    mydriver.quit()
                except:
                    pass
    
    # Final message after all batches
    print("\n" + "=" * 80)
    print("All Batches Complete")
    print("=" * 80)
    if args.verbose:
        print("\nContinue manually (check all the filled in details and publish the project(s)), and maybe check the script output for error messages.\n")
        print("In the Inventory spreadsheet: Add the needed data \n(for the HIFLD data: add the URL in the Download Location field, add 'Y' to the Data Added field, and change the status field to 'Done').\n")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Catch any unhandled exceptions and show full traceback
        error_msg = str(e)
        print(f"\n✗ Unhandled exception: {error_msg}")
        print("\nFull traceback:")
        print(traceback.format_exc())
        raise  # Re-raise so debugger still stops on the exception if needed





