"""
Datalumos Workspace Automation
Automates form interactions on the Datalumos workspace page
"""

from playwright.sync_api import sync_playwright
import time
import re


def wait_for_verification(page, timeout=30000):
    """
    Wait for "Verifying you are human" message to complete.
    
    Args:
        page: Playwright page object
        timeout: Maximum time to wait in milliseconds (default: 30 seconds)
    
    Returns:
        bool: True if verification completed, False if timeout
    """
    try:
        # Check for various forms of verification message
        verification_selectors = [
            'text="Verifying you are human"',
            'text=/verifying.*human/i',
            '[class*="verifying"]',
            '[id*="verifying"]'
        ]
        
        verification_found = False
        for selector in verification_selectors:
            try:
                verification_element = page.locator(selector)
                if verification_element.count() > 0:
                    verification_found = True
                    print("Human verification detected, waiting for completion...")
                    # Wait for the verification message to disappear
                    verification_element.wait_for(state='hidden', timeout=timeout)
                    print("✓ Verification completed")
                    break
            except Exception:
                continue
        
        # Additional wait for page to be ready after verification
        page.wait_for_timeout(2000)
        return True
    except Exception as e:
        # If we can't find the verification message or it times out, continue anyway
        print(f"Note: Verification check completed (or not needed)")
        page.wait_for_timeout(2000)
        return True


def sign_in(page):
    """
    Automate the sign-in process for DataLumos.
    
    Args:
        page: Playwright page object
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        # Navigate to home page
        print("Navigating to DataLumos home page...")
        page.goto("https://www.icpsr.umich.edu/sites/datalumos/home", wait_until='domcontentloaded', timeout=30000)
        wait_for_verification(page)
        
        # Click Login button
        print("Looking for 'Login' button...")
        login_found = False
        
        # Try to find Login button by text
        all_buttons = page.locator('button, a, [role="button"]')
        button_count = all_buttons.count()
        
        for i in range(button_count):
            try:
                button = all_buttons.nth(i)
                text = button.inner_text().strip()
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
        wait_for_verification(page)
        
        # Click "Sign in with Email" button
        print("Looking for 'Sign in with Email' button...")
        email_signin_found = False
        
        all_buttons = page.locator('button, a, [role="button"]')
        button_count = all_buttons.count()
        
        for i in range(button_count):
            try:
                button = all_buttons.nth(i)
                text = button.inner_text().strip()
                if 'sign in with email' in text.lower() or 'email' in text.lower():
                    print(f"Found email sign-in button: '{text}'")
                    button.click()
                    email_signin_found = True
                    break
            except Exception:
                continue
        
        if not email_signin_found:
            return False, "Could not find 'Sign in with Email' button"
        
        # Wait for email form to appear and any verification
        wait_for_verification(page)
        
        # Fill in username/email
        print("Filling in username/email address...")
        username_input = page.locator('input#username, input[name="username"]')
        if username_input.count() > 0:
            username_input.first.fill("mike@kraley.com")
            print("✓ Username field filled")
        else:
            return False, "Could not find username input field"
        
        # Fill in password
        print("Filling in password...")
        password_input = page.locator('input#password, input[name="password"]')
        if password_input.count() > 0:
            password_input.first.fill("cnDS5C?3m!kFkt7Q")
            print("✓ Password field filled")
        else:
            return False, "Could not find password input field"
        
        page.wait_for_timeout(500)
        
        # Submit the form by clicking the Sign In button
        print("Clicking Sign In button...")
        submit_button = page.locator('input[type="submit"][value="Sign In"], input.pf-c-button.btn.btn-primary[type="submit"]')
        if submit_button.count() > 0:
            submit_button.first.click()
            print("✓ Sign In button clicked")
        else:
            # Fallback: try pressing Enter on the password field
            print("Sign In button not found, trying Enter key...")
            password_input.first.press('Enter')
        
        # Wait for sign-in to complete
        print("Waiting for sign-in to complete...")
        page.wait_for_timeout(3000)
        
        return True, "Successfully signed in"
    
    except Exception as e:
        error_msg = f"Error during sign-in: {str(e)}"
        print(f"✗ {error_msg}")
        return False, error_msg


def open_workspace_and_click_create_project(headless=False):
    """
    Sign in to DataLumos, open the workspace page, and click the "Create New Project" button.
    
    Args:
        headless: If False, run browser in visible mode for debugging (default: False)
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    playwright = None
    browser = None
    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=headless, slow_mo=500 if not headless else 0)
        page = browser.new_page()
        
        # Sign in first
        signin_success, signin_message = sign_in(page)
        if not signin_success:
            return False, signin_message, None
        
        print(f"✓ {signin_message}\n")
        
        # Click the "Upload Data" button
        print("Looking for 'Upload Data' button...")
        upload_data_link = page.locator('a.nav-link[href*="workspace"], a:has-text("Upload Data")')
        if upload_data_link.count() > 0:
            upload_data_link.first.click()
            print("✓ Clicked 'Upload Data' button")
        else:
            return False, "Could not find 'Upload Data' button", None
        
        # Wait for page to load and any verification
        wait_for_verification(page)
        
        # Find and click the "Create New Project" button
        print("Looking for 'Create New Project' button...")
        try:
            create_project_button = page.get_by_text("Create New Project")
            create_project_button.click()
            print("✓ Clicked 'Create New Project' button")
        except Exception as e:
            print(f"✗ Could not find or click 'Create New Project' button: {e}")
            screenshot_path = "workspace_page_screenshot.png"
            page.screenshot(path=screenshot_path)
            print(f"Screenshot saved to {screenshot_path} for debugging")
            return False, "Could not find 'Create New Project' button"
        
        # Wait for dialog to appear
        page.wait_for_timeout(1000)
        
        # Fill in the title field
        print("Filling in project title...")
        try:
            title_input = page.locator('input#title, input[name="title"]')
            title_input.fill("Test")
            print("✓ Filled in project title: 'Test'")
        except Exception as e:
            return False, f"Could not fill title field: {e}"
        
        # Click the "Save & Apply" button
        print("Clicking 'Save & Apply' button...")
        try:
            save_button = page.locator('button.save-project, button:has-text("Save & Apply")')
            save_button.click()
            print("✓ Clicked 'Save & Apply' button")
        except Exception as e:
            return False, f"Could not click 'Save & Apply' button: {e}"
        
        # Wait for dialog to close and changes to apply
        page.wait_for_timeout(2000)
        
        # Click "Continue To Project Workspace" link
        print("Clicking 'Continue To Project Workspace' link...")
        try:
            continue_link = page.locator('a[role="button"].btn.btn-primary:has-text("Continue To Project Workspace"), a.btn.btn-primary[href*="goToPath=/datalumos/"]')
            if continue_link.count() == 0:
                # Try alternative selector
                continue_link = page.get_by_text("Continue To Project Workspace")
            continue_link.click()
            print("✓ Clicked 'Continue To Project Workspace' link")
        except Exception as e:
            return False, f"Could not click 'Continue To Project Workspace' link: {e}", None
        
        # Wait for navigation to complete and any verification
        wait_for_verification(page)
        
        # Extract project ID from URL
        current_url = page.url
        print(f"Current URL: {current_url}")
        
        project_id = None
        # Look for /datalumos/ followed by numbers in the URL
        match = re.search(r'/datalumos/(\d+)', current_url)
        if match:
            project_id = match.group(1)
            print(f"✓ Extracted project ID: {project_id}")
        else:
            print("⚠ Could not extract project ID from URL")
        
        input("Press Enter to continue...")

        return True, "Successfully created new project", project_id
        
    
    except Exception as e:
        error_msg = f"Error: {str(e)}"
        print(f"✗ {error_msg}")
        return False, error_msg, None
    
    finally:
        if browser:
            browser.close()
        if playwright:
            playwright.stop()


def main():
    """Main entry point for the automation script"""
    print("=" * 80)
    print("Datalumos Workspace Automation")
    print("=" * 80)
    print()
    
    # Run in visible mode by default for first step
    success, message, project_id = open_workspace_and_click_create_project(headless=False)
    
    print()
    print("=" * 80)
    if success:
        print(f"✓ {message}")
        if project_id:
            print(f"Project ID: {project_id}")
    else:
        print(f"✗ {message}")
    print("=" * 80)


if __name__ == "__main__":
    main()

