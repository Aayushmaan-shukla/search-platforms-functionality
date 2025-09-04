import csv
import time
import random
import re
import json
import argparse
import sys
import subprocess
import platform
try:
    import winreg
except ImportError:
    winreg = None
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
import pandas as pd
import os
from datetime import datetime
import requests

class AmazonMobileScraper:
    def __init__(self, headless=False):
        self.driver = None
        self.wait = None
        self.results = []  # Changed to list for atomic entries
        self.headless = headless
        self.progress_file = "scraping_progress.json"
        self.screenshot_dir = "screenshots"
        self.proxy_api_key = "wvm4z69kf54pc9rod7ck"
        self.proxy_base_url = "https://proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
        self.current_proxy = None
        self.rows_processed = 0
        self.session_change_threshold = 5
        self.backup_threshold = 100  # Create backup every 100 rows
        self.error_retry_count = 0
        self.max_retries = 2
        self.setup_driver()
        self.load_progress()
        
    def get_proxy(self):
        """Get a new proxy from proxyscrape"""
        try:
            url = f"{self.proxy_base_url}&apikey={self.proxy_api_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                proxy = response.text.strip()
                if proxy and ':' in proxy:
                    print(f"New proxy obtained: {proxy[:20]}...")
                    return proxy
                else:
                    print("Invalid proxy format received")
            else:
                print(f"Proxy API returned status code: {response.status_code}")
        except requests.exceptions.Timeout:
            print("Timeout getting proxy from API")
        except requests.exceptions.ConnectionError:
            print("Connection error getting proxy from API")
        except Exception as e:
            print(f"Error getting proxy: {e}")
        
        print("Falling back to no proxy")
        return None
    
    def get_chrome_version_major(self):
        """Detect installed Chrome major version (best-effort)."""
        candidates = [
            "chrome",
            "google-chrome",
            "C:/Program Files/Google/Chrome/Application/chrome.exe",
            "C:/Program Files (x86)/Google/Chrome/Application/chrome.exe",
        ]
        for binary in candidates:
            try:
                out = subprocess.check_output([binary, "--version"], stderr=subprocess.STDOUT, universal_newlines=True, timeout=3)
                m = re.search(r"(\d+)\.\d+\.\d+\.\d+", out)
                if m:
                    return int(m.group(1))
            except Exception:
                pass

        if platform.system().lower().startswith('win') and winreg is not None:
            reg_paths = [
                (winreg.HKEY_CURRENT_USER, r"Software\\Google\\Chrome\\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"Software\\Google\\Chrome\\BLBeacon"),
                (winreg.HKEY_LOCAL_MACHINE, r"Software\\WOW6432Node\\Google\\Chrome\\BLBeacon"),
            ]
            for hive, path in reg_paths:
                try:
                    with winreg.OpenKey(hive, path) as key:
                        version, _ = winreg.QueryValueEx(key, "version")
                        m = re.match(r"(\d+)", version)
                        if m:
                            return int(m.group(1))
                except OSError:
                    continue
        return None
    
    def setup_driver(self, use_proxy=True):
        """Setup Chrome driver with proper headless support"""
        try:
            if self.driver:
                self.driver.quit()
            
            # For headless mode, prefer regular Chrome driver over undetected_chromedriver
            # as undetected_chromedriver has issues with proper headless mode
            if self.headless:
                print("Headless mode requested - using regular Chrome driver for better headless support")
                self.setup_regular_chrome(use_proxy)
                return
            
            # For non-headless mode, try undetected Chrome first
            chrome_options = uc.ChromeOptions()
            
            # Add proxy if available
            if use_proxy and self.current_proxy:
                chrome_options.add_argument(f'--proxy-server={self.current_proxy}')
                print(f"Using proxy: {self.current_proxy[:20]}...")
            
            # Additional options for better undetection
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Random user agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Create undetected Chrome driver (pin to detected version if available), with retry
            version_main = self.get_chrome_version_major()
            try:
                if version_main:
                    self.driver = uc.Chrome(options=chrome_options, version_main=version_main)
                else:
                    self.driver = uc.Chrome(options=chrome_options)
            except Exception as inner_e:
                msg = str(inner_e)
                retry_version = None
                m_current = re.search(r"Current browser version is (\d+)", msg)
                m_only = re.search(r"only supports Chrome version (\d+)", msg)
                if m_current:
                    retry_version = int(m_current.group(1))
                elif m_only:
                    retry_version = int(m_only.group(1))
                if retry_version:
                    print(f"Retrying with Chrome version: {retry_version}")
                    self.driver = uc.Chrome(options=chrome_options, version_main=retry_version)
                else:
                    raise inner_e

            # Verify driver was created successfully
            if not self.driver:
                raise Exception("Driver creation failed - driver is None")

            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            
            print("Undetected Chrome driver setup completed successfully")
            
        except Exception as e:
            print(f"Error setting up Chrome driver: {e}")
            # Fallback to regular Chrome if undetected fails
            print("Falling back to regular Chrome driver...")
            self.setup_regular_chrome(use_proxy)
    
    def setup_regular_chrome(self, use_proxy=True):
        """Setup regular Chrome driver with enforced headless mode"""
        try:
            chrome_options = Options()
            
            # Essential Chrome options
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            if self.headless:
                print("Setting up ENFORCED headless mode...")
                # Multiple headless configurations for maximum compatibility
                chrome_options.add_argument("--headless=new")  # New headless mode for Chrome 109+
                chrome_options.add_argument("--headless")      # Legacy headless mode
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--disable-software-rasterizer")
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-features=TranslateUI")
                chrome_options.add_argument("--disable-ipc-flooding-protection")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument("--no-first-run")
                chrome_options.add_argument("--disable-default-apps")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-background-networking")
                chrome_options.add_argument("--disable-component-update")
                chrome_options.add_argument("--disable-client-side-phishing-detection")
                chrome_options.add_argument("--disable-sync")
                chrome_options.add_argument("--disable-translate")
                chrome_options.add_argument("--hide-scrollbars")
                chrome_options.add_argument("--mute-audio")
                chrome_options.add_argument("--disable-logging")
                chrome_options.add_argument("--disable-plugins")
                chrome_options.add_argument("--disable-extensions")
                chrome_options.add_argument("--disable-images")
                
                # Force headless property
                try:
                    chrome_options.headless = True
                except Exception:
                    pass
                    
                print("Headless mode configured with multiple fallbacks")
            
            if use_proxy and self.current_proxy:
                chrome_options.add_argument(f'--proxy-server={self.current_proxy}')
                print(f"Using proxy: {self.current_proxy[:20]}...")
            
            # User agent for better compatibility
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Create regular Chrome driver
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Verify driver was created successfully
            if not self.driver:
                raise Exception("Regular Chrome driver creation failed - driver is None")
                
            # Additional stealth configurations
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": random.choice(user_agents)
            })
            
            self.wait = WebDriverWait(self.driver, 15)
            
            if self.headless:
                print("Regular Chrome driver setup completed successfully in HEADLESS mode")
            else:
                print("Regular Chrome driver setup completed successfully")
            
        except Exception as e:
            print(f"Error setting up regular Chrome driver: {e}")
            raise
    
    def change_session(self, change_proxy=False):
        """Change Chrome session every 5 rows, optionally change proxy on errors"""
        try:
            print("Changing Chrome session...")
            
            # Only get new proxy if explicitly requested (on errors)
            if change_proxy:
                new_proxy = self.get_proxy()
                if new_proxy:
                    self.current_proxy = new_proxy
                    print(f"Switched to new proxy: {new_proxy[:20]}...")
            
            # Setup new driver
            self.setup_driver(use_proxy=True)
            
            # Validate driver was created successfully
            if not self.driver:
                raise Exception("Failed to create driver in change_session")
            
            # Reset row counter
            self.rows_processed = 0
            
            print("Chrome session changed successfully")
            
        except Exception as e:
            print(f"Error changing Chrome session: {e}")
            # Try without proxy if proxy fails
            try:
                print("Trying without proxy...")
                self.current_proxy = None
                self.setup_driver(use_proxy=False)
                if not self.driver:
                    raise Exception("Failed to create driver without proxy")
                self.rows_processed = 0
                print("Chrome session changed without proxy")
            except Exception as e2:
                print(f"Failed to change session: {e2}")
                raise
    
    def load_progress(self):
        """Load progress from previous scraping session"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    progress_data = json.load(f)
                    self.results = progress_data.get('results', [])
                    self.last_processed_index = progress_data.get('last_processed_index', -1)
                    print(f"Loaded progress: {len(self.results)} entries already processed")
                    print(f"Resuming from index: {self.last_processed_index + 1}")
                return True
        except Exception as e:
            print(f"Error loading progress: {e}")
        
        self.last_processed_index = -1
        return False
    
    def save_progress(self):
        """Save current progress to file"""
        try:
            progress_data = {
                'results': self.results,
                'last_processed_index': self.last_processed_index,
                'timestamp': datetime.now().isoformat()
            }
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
            print(f"Progress saved: {len(self.results)} entries processed")
        except Exception as e:
            print(f"Error saving progress: {e}")
    
    def create_backup(self):
        """Create backup file and delete previous backup"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"amazon_mobile_results_backup_{timestamp}.json"
            
            # Save current results to backup
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            print(f"Backup created: {backup_file}")
            
            # Find and delete previous backup files
            backup_files = [f for f in os.listdir('.') if f.startswith('amazon_mobile_results_backup_') and f.endswith('.json')]
            backup_files.sort()
            
            # Keep only the latest backup, delete others
            if len(backup_files) > 1:
                for old_backup in backup_files[:-1]:
                    try:
                        os.remove(old_backup)
                        print(f"Deleted old backup: {old_backup}")
                    except Exception as e:
                        print(f"Error deleting old backup {old_backup}: {e}")
            
            return backup_file
            
        except Exception as e:
            print(f"Error creating backup: {e}")
            return None
    
    def handle_error_with_backup(self, error_msg, search_query, index):
        """Handle errors by creating backup and managing retries"""
        print(f"Error occurred: {error_msg}")
        
        # Create backup on error
        backup_file = self.create_backup()
        if backup_file:
            print(f"Backup created due to error: {backup_file}")
        
        # Take screenshot for debugging
        self.take_screenshot("error_occurred", search_query, index)
        
        # Increment error retry count
        self.error_retry_count += 1
        
        if self.error_retry_count >= self.max_retries:
            print(f"Max retries ({self.max_retries}) reached. Skipping this entry.")
            self.error_retry_count = 0  # Reset for next entry
            return False
        else:
            print(f"Retry attempt {self.error_retry_count}/{self.max_retries}")
            # Change session and proxy on error
            self.change_session(change_proxy=True)
            return True
    
    def take_screenshot(self, reason, search_query, index):
        """Take screenshot for debugging purposes"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{reason}_{index}_{timestamp}.png"
            if not self.driver:
                print("No active driver; skipping screenshot")
                return None
            # Ensure screenshots directory exists
            os.makedirs(self.screenshot_dir, exist_ok=True)
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            print(f"Error taking screenshot: {e}")
            return None
    
    def search_amazon(self, search_query):
        """Search for products on Amazon.in"""
        try:
            if not self.driver:
                print("Driver not initialized; reinitializing...")
                self.setup_driver(use_proxy=True)
                if not self.driver:
                    return False
                    
            # Navigate to Amazon.in
            self.driver.get("https://www.amazon.in")
            time.sleep(random.uniform(2, 4))
            
            # Find and fill the search box
            search_box = self.wait.until(
                EC.presence_of_element_located((By.ID, "twotabsearchtextbox"))
            )
            
            # Clear and enter search query
            search_box.clear()
            search_box.send_keys(search_query)
            time.sleep(random.uniform(1, 2))
            
            # Submit search
            search_box.send_keys(Keys.RETURN)
            time.sleep(random.uniform(3, 5))
            
            return True
            
        except Exception as e:
            print(f"Error searching Amazon: {e}")
            # Check if it's a proxy/connection error
            if any(error_type in str(e).lower() for error_type in ['timeout', 'connection', 'pool', 'http']):
                print("Detected connection/proxy error, switching IP...")
                self.change_session(change_proxy=True)  # Change proxy on error
                return False
            return False
    
    def handle_continue_shopping(self):
        """Handle 'Continue shopping' button if it appears"""
        try:
            # Look for various possible button texts
            button_texts = [
                "Continue shopping",
                "Continue Shopping", 
                "Continue",
                "Keep shopping",
                "Keep Shopping"
            ]
            
            for text in button_texts:
                try:
                    button = self.driver.find_element(By.XPATH, f"//button[contains(text(), '{text}')]")
                    button.click()
                    print(f"Clicked '{text}' button")
                    time.sleep(random.uniform(2, 3))
                    return True
                except NoSuchElementException:
                    continue
                    
            return False
            
        except Exception as e:
            print(f"Error handling continue shopping button: {e}")
            return False
    
    def extract_product_links(self, max_products=5):
        """Extract product links and text from Amazon search results"""
        products = []
        
        try:
            # Look for product links with the specified class pattern
            # Amazon uses various class patterns for product links
            link_selectors = [
                "a[class*='a-link-normal'][class*='s-line-clamp']",
                "a[class*='a-link-normal'][class*='s-link-style']",
                "a[class*='a-link-normal'][class*='a-text-normal']",
                "h2 a",
                ".s-result-item a[href*='/dp/']",
                ".s-result-item a[href*='/gp/product/']"
            ]
            
            links = []
            for selector in link_selectors:
                try:
                    links = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if links:
                        print(f"Found {len(links)} links with selector: {selector}")
                        break
                except:
                    continue
            
            if not links:
                print("No product links found")
                return products
            
            # Extract first max_products links
            for i, link in enumerate(links[:max_products]):
                try:
                    href = link.get_attribute('href')
                    text = link.text.strip()
                    
                    # If no text in link, try to find text in child elements
                    if not text:
                        # Look for text in h2 or span elements
                        h2 = link.find_element(By.TAG_NAME, 'h2') if link.find_elements(By.TAG_NAME, 'h2') else None
                        if h2:
                            text = h2.text.strip()
                        else:
                            span = link.find_element(By.TAG_NAME, 'span') if link.find_elements(By.TAG_NAME, 'span') else None
                            if span:
                                text = span.text.strip()
                    
                    if href and text:
                        products.append({
                            'url': href,
                            'product_name_via_url': text
                        })
                        print(f"Amazon Product {i+1}: {text[:50]}...")
                        
                except Exception as e:
                    print(f"Error extracting Amazon product {i+1}: {e}")
                    continue
            
        except Exception as e:
            print(f"Error extracting Amazon product links: {e}")
        
        return products
    
    def scrape_permutations(self, csv_file_path):
        """Scrape Amazon for each permutation in the CSV file"""
        
        # Read CSV file
        try:
            df = pd.read_csv(csv_file_path)
            print(f"Loaded {len(df)} rows from CSV")
            print(f"Processing all {len(df)} rows (including duplicates)")
            
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return
        
        try:
            for index, row in df.iterrows():
                # Skip if already processed
                if index <= self.last_processed_index:
                    continue
                
                try:
                    # Check if we need to change session (every 5 rows) - WITHOUT changing proxy
                    if self.rows_processed >= self.session_change_threshold:
                        print(f"\n--- Changing Chrome session after {self.rows_processed} rows (keeping same proxy) ---")
                        self.change_session(change_proxy=False)  # Don't change proxy, just new session
                    
                    # Create search query for each row
                    product_name = row['product_name']
                    colour = row['colour']
                    ram_rom = row['ram_rom']
                    model_id = str(row['model_id'])
                    
                    search_query = f"{product_name} {colour} {ram_rom}"
                    print(f"\n--- Processing {index + 1}/{len(df)}: {search_query} ---")
                    
                    # Search on Amazon
                    if self.search_amazon(search_query):
                        # Handle continue shopping button if it appears
                        self.handle_continue_shopping()
                        
                        # Extract product links
                        products = self.extract_product_links(max_products=5)
                        
                        if products:
                            # Check for blank/null values and take screenshots if needed
                            for i, product in enumerate(products):
                                if not product['url'] or not product['product_name_via_url']:
                                    print(f"Warning: Blank/null values found for product {i+1}")
                                    self.take_screenshot("blank_values", search_query, index)
                                    break
                            
                            # Create separate entry for each CSV row (atomic structure)
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'amazon_links': products
                            }
                            
                            # Add to results list (each row is a separate entry)
                            self.results.append(entry)
                            
                            print(f"Extracted {len(products)} Amazon products for entry {index + 1}")
                        else:
                            print("No Amazon products found")
                            # Take screenshot for no products found
                            self.take_screenshot("no_products", search_query, index)
                            
                            # Still create an entry even if no products found
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'amazon_links': []
                            }
                            self.results.append(entry)
                        
                        # Update progress and row counter
                        self.last_processed_index = index
                        self.rows_processed += 1
                        self.save_progress()
                        
                        # Create backup every 100 rows
                        if self.rows_processed % self.backup_threshold == 0:
                            print(f"\n--- Creating backup after {self.rows_processed} rows ---")
                            self.create_backup()
                        
                        # Random delay between searches
                        delay = random.uniform(3, 7)
                        print(f"Waiting {delay:.1f} seconds before next search...")
                        time.sleep(delay)
                        
                    else:
                        print("Failed to search Amazon")
                        # Handle search failure with backup and retry
                        if self.handle_error_with_backup("Search failed", search_query, index):
                            continue  # Retry
                        else:
                            # Max retries reached, create empty entry and continue
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'amazon_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.save_progress()
                            continue
                
                except Exception as e:
                    error_msg = f"Error processing row {index + 1}: {e}"
                    print(error_msg)
                    
                    # Check if it's a connection-related error
                    if any(error_type in str(e).lower() for error_type in ['timeout', 'connection', 'pool', 'http', 'network']):
                        if self.handle_error_with_backup(f"Connection error: {e}", search_query, index):
                            continue  # Retry
                        else:
                            # Max retries reached, create empty entry and continue
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'amazon_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.save_progress()
                            continue
                    else:
                        # Other errors - create backup and continue
                        self.handle_error_with_backup(error_msg, search_query, index)
                        continue
                    
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Saving progress...")
            self.save_progress()
            print("Progress saved. You can resume later by running the script again.")
            raise
    
    def save_results(self, output_file="amazon_mobile_results.json"):
        """Save Amazon results to JSON file"""
        
        if self.results:
            try:
                # Results are already in list format
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, indent=2, ensure_ascii=False)
                
                print(f"Amazon results saved to {output_file}")
                print(f"Total entries processed: {len(self.results)}")
                
                # Also save as CSV for compatibility
                csv_file = output_file.replace('.json', '.csv')
                self.save_results_csv(csv_file)
                
            except Exception as e:
                print(f"Error saving Amazon results: {e}")
        else:
            print("No Amazon results to save")
    
    def save_results_csv(self, output_file="amazon_mobile_results.csv"):
        """Save results in CSV format for compatibility"""
        try:
            csv_data = []
            for entry in self.results:
                model_id = entry['model_id']
                product_name = entry['product_name']
                colour = entry['colour']
                ram_rom = entry['ram_rom']
                
                for link in entry['amazon_links']:
                    csv_data.append({
                        'model_id': model_id,
                        'product_name': product_name,
                        'colour': colour,
                        'ram_rom': ram_rom,
                        'url': link['url'],
                        'product_name_via_url': link['product_name_via_url']
                    })
            
            if csv_data:
                df = pd.DataFrame(csv_data)
                df.to_csv(output_file, index=False, encoding='utf-8')
                print(f"CSV results also saved to {output_file}")
        
        except Exception as e:
            print(f"Error saving CSV results: {e}")
    
    def close(self):
        """Close the browser"""
        if self.driver:
            self.driver.quit()

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Amazon Mobile Phone Scraper')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = AmazonMobileScraper(headless=args.headless)
    
    try:
        # CSV file path
        csv_file = "expanded_permutations.csv"
        
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return
        
        # Start Amazon scraping (process all entries)
        print("=== STARTING AMAZON SCRAPING ===")
        print(f"Headless mode: {'Enabled' if args.headless else 'Disabled'}")
        scraper.scrape_permutations(csv_file)
        
        # Save results
        scraper.save_results()
        
    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()
