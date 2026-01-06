

"""
Script to automatically fill in the DataLumos fields from a csv file (exported spreadsheet), and upload the files.
Login, checking and publishing is done manually to avoid errors.

The path of the csv file has to be set before starting the script, the path to the folder with the data files too.
Also, the rows to be processed have to be set (start_row and end_row) - counting starts at 1 and doesn't include the column names row.

There is no error handling. But the browser remains open even if the script crashes, so the inputs could be checked and/or completed manually.
"""



from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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


#########################################################
# Command line arguments are now used instead of hardcoded variables
# See parse_arguments() function below
#########################################################

url_datalumos = "https://www.datalumos.org/datalumos/workspace"


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Automate DataLumos form filling and file uploads from CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # With automated login:
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --username "user@example.com" --password "pass123" --folder "C:\\data"
  
  # With manual login (no username/password):
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --folder "C:\\data"
  
  # Using Firefox instead of Chrome:
  python chiara_upload.py --csv "data.csv" --start-row 1 --end-row 5 --browser firefox --username "user@example.com" --password "pass123"
        """
    )
    
    parser.add_argument('--csv', '--csv-file-path', dest='csv_file_path', required=True,
                        help='Path to the CSV file containing the data to upload')
    
    parser.add_argument('--start-row', type=int, required=True,
                        help='Starting row number (counting starts at 1, excluding header row)')
    
    parser.add_argument('--end-row', type=int, required=True,
                        help='Ending row number (to process only one row, set start-row and end-row to the same number)')
    
    parser.add_argument('--folder', '--folder-path-uploadfiles', dest='folder_path_uploadfiles', default='',
                        help='Path to the folder where upload files are located (subfolders for each project should be in here)')
    
    parser.add_argument('--username', default=None,
                        help='Username/email for automated login (if not provided, manual login will be required)')
    
    parser.add_argument('--password', default=None,
                        help='Password for automated login (if not provided, manual login will be required)')
    
    parser.add_argument('--browser', choices=['chrome', 'chromium', 'firefox'], default='chrome',
                        help='Browser to use: chrome/chromium or firefox (default: chrome)')
    
    return parser.parse_args()


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
        email_signin_found = False
        
        all_buttons = driver.find_elements(By.CSS_SELECTOR, "button, a, [role='button']")
        
        for button in all_buttons:
            try:
                text = button.text.strip()
                if 'sign in with email' in text.lower() or ('email' in text.lower() and 'sign' in text.lower()):
                    print(f"Found email sign-in button: '{text}'")
                    button.click()
                    email_signin_found = True
                    break
            except Exception:
                continue
        
        if not email_signin_found:
            return False, "Could not find 'Sign in with Email' button"
        
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
        
        # Navigate to workspace after sign-in
        print("Navigating to workspace...")
        driver.get("https://www.datalumos.org/datalumos/workspace")
        wait_for_verification(driver)
        
        return True, "Successfully signed in"
    
    except Exception as e:
        error_msg = f"Error during sign-in: {str(e)}"
        print(f"✗ {error_msg}")
        return False, error_msg


def wait_for_obscuring_elements(current_driver_obj):
    overlays = current_driver_obj.find_elements(By.ID, "busy")  # caution: find_elements, not find_element
    if len(overlays) != 0:  # there is an overlay
        print(f"... (Waiting for overlay to disappear. Overlay(s): {overlays})")
        for overlay in overlays:
            # Wait until the overlay becomes invisible:
            WebDriverWait(current_driver_obj, 360).until(EC.invisibility_of_element_located(overlay))
            sleep(0.5)

def read_csv_line(csv_file, line_to_process):
    # gets the input from the specified line of the csv file, to put it in the datalumos forms.
    with open(csv_file, "r", newline='') as datafile:
        datareader = csv.DictReader(datafile)
        for i, singlerow in enumerate(datareader):
            if i == (line_to_process - 1):  # -1 because i starts counting at 0
                return singlerow  # is already a dictionary

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
    #print("combinedpath:", combinedpath)
    uploadfiles_names = os.listdir(combinedpath)
    print("\nFiles that will be uploaded:", uploadfiles_names, "\n")
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



def main():
    """Main execution function."""
    # Parse command line arguments
    args = parse_arguments()
    
    # Initialize browser
    mydriver = initialize_browser(args.browser)
    
    try:
        # Automated sign-in
        print("\n" + "=" * 80)
        print("DataLumos Automated Sign-In")
        print("=" * 80)
        signin_success, signin_message = sign_in(mydriver, args.username, args.password)
        if not signin_success:
            print(f"✗ {signin_message}")
            print("Please check the browser and complete login manually if needed.")
            input("Press Enter to continue after manual login...")
        else:
            print(f"✓ {signin_message}\n")

        print("If you upload from USB device: MAKE SURE THE USB IS PLUGGED IN!\n")

        for current_row in range(args.start_row, args.end_row + 1):

            new_project_btn = WebDriverWait(mydriver, 360).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".btn > span:nth-child(3)"))) # .btn > span:nth-child(3)
            #print("button found")
            wait_for_obscuring_elements(mydriver)
            new_project_btn.click()

            datadict = read_csv_line(args.csv_file_path, current_row)
            #print(datadict)
            print("\n----------------------------")
            print(f"Processing row {current_row}, Title: {datadict['4_title']}\n")


            # --- Title

            # <input type="text" class="form-control" name="title" id="title" value="" data-reactid=".2.0.0.1.2.0.$0.$0.$0.$displayPropKey2.0.2.0">
            project_title_form = WebDriverWait(mydriver, 10).until(EC.presence_of_element_located((By.ID, "title")))
            # title with pre-title (if existent):
            pojecttitle = datadict["4_title"] if len(datadict["4_pre_title"]) == 0 else datadict["4_pre_title"] + " " + datadict["4_title"]
            project_title_form.send_keys(pojecttitle)
            # .save-project
            project_title_apply = WebDriverWait(mydriver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".save-project")))
            #print("project_title_apply - found")
            project_title_apply.click()
            # <a role="button" class="btn btn-primary" href="workspace?goToPath=/datalumos/239181&amp;goToLevel=project" data-reactid=".2.0.0.1.2.1.0.0.0">Continue To Project Workspace</a>
            #   CSS-selector: a.btn-primary
            project_title_apply2 = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.LINK_TEXT, "Continue To Project Workspace")))
            #print("Continue To Project Workspace - found")
            project_title_apply2.click()
            
            # Wait for navigation to complete
            wait_for_obscuring_elements(mydriver)
            sleep(1)
            
            # Extract workspace ID from current URL after navigating to workspace
            current_url = mydriver.current_url
            workspace_id = None
            # Look for /datalumos/ followed by digits in the URL
            match = re.search(r'/datalumos/(\d+)', current_url)
            if match:
                workspace_id = match.group(1)
                print(f"✓ Workspace ID: {workspace_id} (from URL: {current_url})")
            else:
                print(f"⚠ Could not extract workspace ID from URL: {current_url}")


            # --- expand everything

            # collapse all: <span data-reactid=".0.3.1.1.0.1.2.0.1.0.1.1"> Collapse All</span>
            #   css-selector: #expand-init > span:nth-child(2)
            collapse_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#expand-init > span:nth-child(2)")))
            wait_for_obscuring_elements(mydriver)
            collapse_btn.click()
            sleep(2)
            # expand all: <span data-reactid=".0.3.1.1.0.1.2.0.1.0.1.1"> Expand All</span>
            #   CSS-selector:    #expand-init > span:nth-child(2)
            expand_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#expand-init > span:nth-child(2)")))
            wait_for_obscuring_elements(mydriver)
            expand_btn.click()
            sleep(2)


            # --- Government agency

            # government add value: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey1.0.2.2"> add value</span>
            #   CSS-selector: #groupAttr0 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)
            agency_investigator = [datadict["5_agency"], datadict["5_agency2"]]
            for singleinput in agency_investigator:
                if len(singleinput) != 0 and singleinput != " ":
                    add_gvmnt_value = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#groupAttr0 > div:nth-child(1) > div:nth-child(1) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)")))
                    #print("add_gvmnt_value found")
                    wait_for_obscuring_elements(mydriver)
                    add_gvmnt_value.click()
                    # <a href="#org" aria-controls="org" role="tab" data-toggle="tab" data-reactid=".2.0.0.1.0.1.0">Organization/Agency</a>
                    #    css-selector: div.modal:nth-child(1) > div:nth-child(1) > div:nth-child(1) > div:nth-child(2) > ul:nth-child(1) > li:nth-child(2) > a:nth-child(1)
                    agency_tab = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.LINK_TEXT, "Organization/Agency")))
                    #print("agency_tab found")
                    wait_for_obscuring_elements(mydriver)
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
                    wait_for_obscuring_elements(mydriver)
                    submit_agency_btn = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".save-org")))
                    submit_agency_btn.click()


            # --- Summary

            summarytext = datadict["6_summary_description"]
            if len(summarytext) != 0 and summarytext != " ":
                # summary edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey2.$dcterms_description_0.1.0.0.0.2.1"> edit</span>
                #   CSS-selector: #edit-dcterms_description_0 > span:nth-child(2)
                edit_summary = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-dcterms_description_0 > span:nth-child(2)")))
                #print("edit_summary found")
                wait_for_obscuring_elements(mydriver)
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
                mydriver.execute_script("arguments[0].textContent = arguments[1];", summary_form, datadict["6_summary_description"])
                # Trigger input event to ensure the editor recognizes the change
                mydriver.execute_script("arguments[0].dispatchEvent(new Event('input', { bubbles: true }));", summary_form)
                sleep(0.3)
                # Switch back to default content before clicking save button (which is outside iframe)
                mydriver.switch_to.default_content()
                wait_for_obscuring_elements(mydriver)
                # save: <i class="glyphicon glyphicon-ok"></i>
                #   .glyphicon-ok
                save_summary_btn = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".glyphicon-ok")))
                wait_for_obscuring_elements(mydriver)
                save_summary_btn.click()
            else:
                print("The summary is mandatory for the DataLumos project! Please fill it in manually.")


            # --- Original Distribution url

            original_url_text = datadict["7_original_distribution_url"]
            if len(original_url_text) != 0 and original_url_text != " ":
                # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$0.$0.0.$displayPropKey4.$imeta_sourceURL_0.1.0.0.0.2.0.1"> edit</span>
                #   css-sel: #edit-imeta_sourceURL_0 > span:nth-child(1) > span:nth-child(2)
                orig_distr_edit = WebDriverWait(mydriver, 100).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-imeta_sourceURL_0 > span:nth-child(1) > span:nth-child(2)")))
                wait_for_obscuring_elements(mydriver)
                orig_distr_edit.click()
                # form: <input type="text" class="form-control input-sm" style="padding-right: 24px;">
                #   css-sel.: .editable-input > input:nth-child(1)
                orig_distr_form = WebDriverWait(mydriver, 100).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".editable-input > input:nth-child(1)")))
                wait_for_obscuring_elements(mydriver)
                orig_distr_form.send_keys(original_url_text)
                # save: <button type="submit" class="btn btn-primary btn-sm editable-submit"><i class="glyphicon glyphicon-ok"></i> save</button>
                #   css-sel: .editable-submit
                orig_distr_form.submit()


            # --- Subject Terms / keywords

            # form: <input class="select2-search__field" type="search" tabindex="0" autocomplete="off" autocorrect="off" autocapitalize="none" spellcheck="false" role="textbox" aria-autocomplete="list" placeholder="" style="width: 0.75em;">
            #   css-sel: .select2-search__field
            # scroll bar: <li class="select2-results__option select2-results__option--highlighted" role="treeitem" aria-selected="false">HIFLD Open</li>
            #    css-sel: .select2-results__option
            keywordcells = [datadict["8_subject_terms1"], datadict["8_subject_terms2"], datadict["8_keywords"]]
            keywords_to_insert = []
            for single_keywordcell in keywordcells:
                if len(single_keywordcell) != 0 and single_keywordcell != " ":
                    more_keywords = single_keywordcell.replace("'", "").replace("[", "").replace("]", "").replace('"', '')  # remove quotes and brackets
                    more_keywordslist = more_keywords.split(",")
                    keywords_to_insert += more_keywordslist
            print("\nkeywords_to_insert:", keywords_to_insert, "\n")
            for single_keyword in keywords_to_insert:
                keyword = single_keyword.strip(" '")
                try:
                    wait_for_obscuring_elements(mydriver)
                    keywords_form = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".select2-search__field")))
                    keywords_form.click()
                    keywords_form.send_keys(keyword)
                    #sleep(2)
                    wait_for_obscuring_elements(mydriver)
                    #keyword_sugg = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select2-results__option")))
                    # find the list element, taking care to match the exact text [suggestion from user sefk]:
                    keyword_sugg = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.XPATH, f"//li[contains(@class, 'select2-results__option') and text()='{keyword}']")))
                    wait_for_obscuring_elements(mydriver)
                    keyword_sugg.click()
                except:
                    print("\nThere was a problem with the keywords! Please check if one ore more are missing in the form and fill them in manually.\n Problem:")
                    print(traceback.format_exc())


            # --- Geographic Coverage

            geographic_coverage_text = datadict["9_geographic_coverage"]
            if len(geographic_coverage_text) != 0 and geographic_coverage_text != " ":
                # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey1.0.5:$dcterms_location_0_0.0.0.0.0.2.0.1"> edit</span>
                #   css-sel: #edit-dcterms_location_0 > span:nth-child(1) > span:nth-child(2)
                geogr_cov_edit = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#edit-dcterms_location_0 > span:nth-child(1) > span:nth-child(2)")))
                #print("edit-button geogr_cov_form found")
                wait_for_obscuring_elements(mydriver)
                geogr_cov_edit.click()
                # form: <input type="text" class="form-control input-sm" style="padding-right: 24px;">
                #   .editable-input > input:nth-child(1)
                geogr_cov_form = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".editable-input > input:nth-child(1)")))
                wait_for_obscuring_elements(mydriver)
                geogr_cov_form.send_keys(geographic_coverage_text)
                geogr_cov_form.submit()


            # --- Time Period

            timeperiod_start_text = datadict["10_time_period1"]
            timeperiod_end_text = datadict["10_time_period2"]
            if len(timeperiod_start_text) != 0 or len(timeperiod_end_text) != 0:
                # edit: <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey2.0.2.2"> add value</span>
                #   #groupAttr1 > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)
                time_period_add_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#groupAttr1 > div:nth-child(1) > div:nth-child(3) > div:nth-child(1) > a:nth-child(3) > span:nth-child(3)")))
                #print("time_period_add_btn found")
                wait_for_obscuring_elements(mydriver)
                time_period_add_btn.click()
                # start: <input type="text" class="form-control" name="startDate" id="startDate" required="" placeholder="YYYY-MM-DD or YYYY-MM or YYYY" title="Enter as YYYY-MM-DD or YYYY-MM or YYYY" value="" data-reactid=".4.0.0.1.1.0.1.0">
                #   #startDate
                time_period_start = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#startDate")))
                wait_for_obscuring_elements(mydriver)
                time_period_start.send_keys(timeperiod_start_text)
                # <input type="text" class="form-control" name="endDate" id="endDate" placeholder="YYYY-MM-DD or YYYY-MM or YYYY" title="Enter as YYYY-MM-DD or YYYY-MM or YYYY" value="" data-reactid=".4.0.0.1.1.1.1.0">
                #   #endDate
                time_period_end = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#endDate")))
                wait_for_obscuring_elements(mydriver)
                time_period_end.send_keys(timeperiod_end_text)
                # <button type="button" class="btn btn-primary save-dates" data-reactid=".4.0.0.1.1.3.0.0">Save &amp; Apply</button>
                #    .save-dates
                save_time_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".save-dates")))
                wait_for_obscuring_elements(mydriver)
                save_time_btn.click()


            # --- Data types

            datatype_to_select = datadict["11_data_types"]
            if len(datatype_to_select) != 0 and datatype_to_select != " ":
                # <span data-reactid=".0.3.1.1.0.1.2.0.2.1:$0.$1.$1.0.$displayPropKey5.$disco_kindOfData_0.1.0.0.0.2.1"> edit</span>
                #   #disco_kindOfData_0 > span:nth-child(2)
                datatypes_edit_btn = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, "#disco_kindOfData_0 > span:nth-child(2)")))
                wait_for_obscuring_elements(mydriver)
                datatypes_edit_btn.click()
                wait_for_obscuring_elements(mydriver)
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
                wait_for_obscuring_elements(mydriver)
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
                wait_for_obscuring_elements(mydriver)
                # css-sel: .editable-submit
                coll_notes_save_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".editable-submit")))
                coll_notes_save_btn.click()


            # --- Upload files

            if len(datadict["path"]) != 0 and datadict["path"] != " ":
                # upload-button: <span data-reactid=".0.3.1.1.0.0.0.0.0.0.1.2.3">Upload Files</span>
                #   a.btn-primary:nth-child(3) > span:nth-child(4)
                wait_for_obscuring_elements(mydriver)
                upload_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-primary:nth-child(3) > span:nth-child(4)")))
                upload_btn.click()
                wait_for_obscuring_elements(mydriver)
                fileupload_field = WebDriverWait(mydriver, 50).until(EC.presence_of_element_located((By.CSS_SELECTOR, ".col-md-offset-2 > span:nth-child(1)")))

                filepaths_to_upload = get_paths_uploadfiles(args.folder_path_uploadfiles, datadict["path"])
                for singlefile in filepaths_to_upload:
                    drag_and_drop_file(fileupload_field, singlefile)

                # when a file is uploaded and its progress bar is complete, a text appears: "File added to queue for upload."
                #   To check that the files are completey uploaded, this text has to be there as often as the number of files:
                filecount = len(filepaths_to_upload)
                #print("filecount:", filecount)
                #sleep(10)
                test2 = mydriver.find_elements(By.XPATH, "//span[text()='File added to queue for upload.']")
                # wait until the text has appeared as often as there are files:
                #   (to wait longer for uploads to be completed, change the number in WebDriverWait(mydriver, ...) - it is the waiting time in seconds)
                WebDriverWait(mydriver, 2000).until(lambda x: True if len(mydriver.find_elements(By.XPATH, "//span[text()='File added to queue for upload.']")) == filecount else False)
                print("\nEverything should be uploaded completely now.\n")


                # close-btn: .importFileModal > div:nth-child(3) > button:nth-child(1)
                wait_for_obscuring_elements(mydriver)
                close_btn = WebDriverWait(mydriver, 50).until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".importFileModal > div:nth-child(3) > button:nth-child(1)")))
                close_btn.click()

            if current_row == args.end_row:
                print("\nContinue manually (check all the filled in details and publish the project(s)), and maybe check the script output for error messages.\n")

                #print("In the Inventory spreadsheet: Add the URL in the Download Location field, add 'Y' to the Data Added field, and change the status field to 'Done'.\n")
                print("In the Inventory spreadsheet: Add the needed data \n(for the HIFLD data: add the URL in the Download Location field, add 'Y' to the Data Added field, and change the status field to 'Done').\n")
                
                if workspace_id:
                    print(f"\nWorkspace ID for this project: {workspace_id}")
    
    except Exception as e:
        print(f"\n✗ Error during execution: {str(e)}")
        print(traceback.format_exc())
    finally:
        # Keep browser open for manual review
        print("\nBrowser will remain open. Close it manually when done.")
        input("Press Enter to close the browser and exit...")
        if 'mydriver' in locals():
            mydriver.quit()


if __name__ == "__main__":
    main()





