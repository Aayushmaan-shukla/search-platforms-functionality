import csv
import time
import random
import re
import json
import argparse
import sys
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, WebDriverException
import pandas as pd
import os
from datetime import datetime
import requests
import gc
import psutil
from contextlib import contextmanager

class CromaMobileScraper:
    def __init__(self, headless=False):
        self.driver = None
        self.wait = None
        self.results = []  # Changed to list for atomic entries
        self.headless = headless
        self.progress_file = "croma_scraping_progress.json"
        self.screenshot_dir = "screenshots"
        self.proxy_api_key = "wvm4z69kf54pc9rod7ck"
        self.proxy_base_url = "https://proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
        self.current_proxy = None
        self.rows_processed = 0
        self.session_change_threshold = 5
        self.backup_threshold = 100  # Create backup every 100 rows
        self.error_retry_count = 0
        self.max_retries = 5  # Increased to 5 retries as requested
        self.global_retry_count = 0
        self.max_global_retries = 3  # Maximum global retries before stopping
        self.open_files_count = 0
        self.max_open_files = 50  # Limit open files
        self.setup_driver()
        self.load_progress()
    
    @contextmanager
    def safe_file_operation(self, file_path, mode='r', encoding='utf-8'):
        """Context manager for safe file operations with proper cleanup"""
        file_handle = None
        try:
            file_handle = open(file_path, mode, encoding=encoding)
            self.open_files_count += 1
            yield file_handle
        except Exception as e:
            print(f"Error in file operation {file_path}: {e}")
            raise
        finally:
            if file_handle:
                try:
                    file_handle.close()
                    self.open_files_count -= 1
                except Exception as e:
                    print(f"Error closing file {file_path}: {e}")
    
    def cleanup_resources(self):
        """Clean up system resources and close open files"""
        try:
            # Force garbage collection
            gc.collect()
            
            # Check and limit open files
            if self.open_files_count > self.max_open_files:
                print(f"Warning: Too many open files ({self.open_files_count}). Forcing cleanup...")
                # Force garbage collection multiple times
                for _ in range(3):
                    gc.collect()
                    time.sleep(0.1)
                
            # Check system resources
            try:
                process = psutil.Process()
                open_files = process.num_fds() if hasattr(process, 'num_fds') else 0
                if open_files > 100:  # Unix-like systems
                    print(f"Warning: High number of open file descriptors: {open_files}")
            except:
                pass  # Ignore if psutil doesn't work on this system
                
        except Exception as e:
            print(f"Error during resource cleanup: {e}")
        
    def get_proxy(self):
        """Get a new proxy from proxyscrape with retry logic"""
        for attempt in range(3):  # Try 3 times
            try:
                url = f"{self.proxy_base_url}&apikey={self.proxy_api_key}"
                with requests.Session() as session:
                    response = session.get(url, timeout=10)
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
                print(f"Timeout getting proxy from API (attempt {attempt + 1}/3)")
            except requests.exceptions.ConnectionError:
                print(f"Connection error getting proxy from API (attempt {attempt + 1}/3)")
            except requests.exceptions.TooManyRedirects:
                print(f"Too many redirects getting proxy (attempt {attempt + 1}/3)")
            except Exception as e:
                print(f"Error getting proxy (attempt {attempt + 1}/3): {e}")
            
            if attempt < 2:  # Don't sleep on last attempt
                time.sleep(random.uniform(2, 5))
        
        print("Falling back to no proxy after all attempts failed")
        return None
    
    def setup_driver(self, use_proxy=True):
        """Setup undetected Chrome driver with proxy support and proper cleanup"""
        try:
            # Properly close existing driver
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    print(f"Error closing existing driver: {e}")
                finally:
                    self.driver = None
                    self.wait = None
            
            # Clean up resources before creating new driver
            self.cleanup_resources()
            
            chrome_options = uc.ChromeOptions()
            
            if self.headless:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")
            
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
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")
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
            
            # Create undetected Chrome driver
            self.driver = uc.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            
            print("Chrome driver setup completed successfully")
            
        except Exception as e:
            print(f"Error setting up Chrome driver: {e}")
            # Fallback to regular Chrome if undetected fails
            if "undetected" in str(e).lower():
                print("Falling back to regular Chrome driver...")
                self.setup_regular_chrome(use_proxy)
    
    def setup_regular_chrome(self, use_proxy=True):
        """Fallback to regular Chrome driver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            if self.headless:
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")
            
            if use_proxy and self.current_proxy:
                chrome_options.add_argument(f'--proxy-server={self.current_proxy}')
            
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            
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
                self.rows_processed = 0
                print("Chrome session changed without proxy")
            except Exception as e2:
                print(f"Failed to change session: {e2}")
    
    def load_progress(self):
        """Load progress from previous scraping session with safe file handling"""
        try:
            if os.path.exists(self.progress_file):
                with self.safe_file_operation(self.progress_file, 'r', 'utf-8') as f:
                    progress_data = json.load(f)
                    self.results = progress_data.get('results', [])
                    self.last_processed_index = progress_data.get('last_processed_index', -1)
                    print(f"Loaded progress: {len(self.results)} entries already processed")
                    print(f"Resuming from index: {self.last_processed_index + 1}")
                return True
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading progress: {e}")
        except Exception as e:
            print(f"Unexpected error loading progress: {e}")
        
        self.last_processed_index = -1
        return False
    
    def save_progress(self):
        """Save current progress to file with safe file handling"""
        try:
            progress_data = {
                'results': self.results,
                'last_processed_index': self.last_processed_index,
                'timestamp': datetime.now().isoformat()
            }
            with self.safe_file_operation(self.progress_file, 'w', 'utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
            print(f"Progress saved: {len(self.results)} entries processed")
        except (OSError, IOError) as e:
            print(f"File system error saving progress: {e}")
        except Exception as e:
            print(f"Unexpected error saving progress: {e}")
    
    def create_backup(self):
        """Create backup file and delete previous backup with safe file handling"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"croma_mobile_results_backup_{timestamp}.json"
            
            # Save current results to backup
            with self.safe_file_operation(backup_file, 'w', 'utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            print(f"Backup created: {backup_file}")
            
            # Find and delete previous backup files
            try:
                backup_files = [f for f in os.listdir('.') if f.startswith('croma_mobile_results_backup_') and f.endswith('.json')]
                backup_files.sort()
                
                # Keep only the latest backup, delete others
                if len(backup_files) > 1:
                    for old_backup in backup_files[:-1]:
                        try:
                            os.remove(old_backup)
                            print(f"Deleted old backup: {old_backup}")
                        except (OSError, IOError) as e:
                            print(f"Error deleting old backup {old_backup}: {e}")
            except (OSError, IOError) as e:
                print(f"Error managing backup files: {e}")
            
            return backup_file
            
        except (OSError, IOError) as e:
            print(f"File system error creating backup: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error creating backup: {e}")
            return None
    
    def handle_error_with_backup(self, error_msg, search_query, index):
        """Handle errors by creating backup and managing retries with exponential backoff"""
        print(f"Error occurred: {error_msg}")
        
        # Create backup on error
        backup_file = self.create_backup()
        if backup_file:
            print(f"Backup created due to error: {backup_file}")
        
        # Take screenshot for debugging
        self.take_screenshot("error_occurred", search_query, index)
        
        # Increment error retry count
        self.error_retry_count += 1
        self.global_retry_count += 1
        
        # Check if we've exceeded global retry limit
        if self.global_retry_count >= self.max_global_retries:
            print(f"Global retry limit ({self.max_global_retries}) reached. Stopping scraper.")
            return False
        
        if self.error_retry_count >= self.max_retries:
            print(f"Max retries ({self.max_retries}) reached for this entry. Skipping.")
            self.error_retry_count = 0  # Reset for next entry
            return False
        else:
            print(f"Retry attempt {self.error_retry_count}/{self.max_retries}")
            
            # Exponential backoff delay
            delay = min(30, 2 ** self.error_retry_count)
            print(f"Waiting {delay} seconds before retry...")
            time.sleep(delay)
            
            # Change session and proxy on error
            self.change_session(change_proxy=True)
            return True
    
    def take_screenshot(self, reason, search_query, index):
        """Take screenshot for debugging purposes"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{reason}_{index}_{timestamp}.png"
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            print(f"Error taking screenshot: {e}")
            return None

    def search_croma(self, search_query):
        """Search for products on Croma.com with comprehensive error handling"""
        for attempt in range(3):  # Try up to 3 times
            try:
                # Navigate to Croma.com
                self.driver.get("https://www.croma.com")
                time.sleep(random.uniform(2, 4))
                
                # Find and fill the search box
                search_box = self.wait.until(
                    EC.presence_of_element_located((By.ID, "searchV2"))
                )
                
                # Clear and enter search query
                search_box.clear()
                search_box.send_keys(search_query)
                time.sleep(random.uniform(1, 2))
                
                # Submit search
                search_box.send_keys(Keys.RETURN)
                time.sleep(random.uniform(3, 5))
                
                return True
                
            except TimeoutException as e:
                print(f"Timeout error searching Croma (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(random.uniform(5, 10))
                    continue
            except WebDriverException as e:
                print(f"WebDriver error searching Croma (attempt {attempt + 1}/3): {e}")
                if any(error_type in str(e).lower() for error_type in ['timeout', 'connection', 'pool', 'http', 'network']):
                    print("Detected connection/proxy error, switching IP...")
                    self.change_session(change_proxy=True)
                if attempt < 2:
                    time.sleep(random.uniform(5, 10))
                    continue
            except Exception as e:
                print(f"Unexpected error searching Croma (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(random.uniform(3, 7))
                    continue
        
        print("All search attempts failed")
        return False
    
    # def handle_continue_shopping(self):
    #     """Handle 'Continue shopping' button if it appears"""
    #     try:
    #         # Look for various possible button texts
    #         button_texts = [
    #             "Continue shopping",
    #             "Continue Shopping", 
    #             "Continue",
    #             "Keep shopping",
    #             "Keep Shopping"
    #         ]
            
    #         for text in button_texts:
    #             try:
    #                 button = self.driver.find_element(By.XPATH, f"//button[contains(text(), '{text}')]")
    #                 button.click()
    #                 print(f"Clicked '{text}' button")
    #                 time.sleep(random.uniform(2, 3))
    #                 return True
    #             except NoSuchElementException:
    #                 continue
                    
    #         return False
            
    #     except Exception as e:
    #         print(f"Error handling continue shopping button: {e}")
    #         return False
    
    def extract_product_links(self, max_products=5):
        """Extract product links and text from Croma search results"""
        products = []
        
        try:
            # Look for product links with the specified class pattern
            # Croma uses various class patterns for product links
            link_selectors = [
                # "a[class*='a-link-normal'][class*='s-line-clamp']",
                # "a[class*='a-link-normal'][class*='s-link-style']",
                # "a[class*='a-link-normal'][class*='a-text-normal']",
                # "h2 a",
                # ".s-result-item a[href*='/dp/']",
                # ".s-result-item a[href*='/gp/product/']"
                "li.product-item > div[data-testid='product-img'] > a",
                "li.product-item h3.product-title > a"
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
                        print(f"Croma Product {i+1}: {text[:50]}...")
                        
                except Exception as e:
                    print(f"Error extracting Croma product {i+1}: {e}")
                    continue
            
        except Exception as e:
            print(f"Error extracting Croma product links: {e}")
        
        return products
    
    def scrape_permutations(self, csv_file_path):
        """Scrape Croma for each permutation in the CSV file"""
        
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
                
                # Check global retry limit
                if self.global_retry_count >= self.max_global_retries:
                    print(f"Global retry limit reached. Stopping scraper at index {index}")
                    break
                
                try:
                    # Clean up resources periodically
                    if index % 10 == 0:
                        self.cleanup_resources()
                    
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
                    
                    # Search on Croma
                    if self.search_croma(search_query):
                        # Handle continue shopping button if it appears
                        # self.handle_continue_shopping()
                        
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
                                'croma_links': products
                            }
                            
                            # Add to results list (each row is a separate entry)
                            self.results.append(entry)
                            
                            print(f"Extracted {len(products)} Croma products for entry {index + 1}")
                        else:
                            print("No Croma products found")
                            # Take screenshot for no products found
                            self.take_screenshot("no_products", search_query, index)
                            
                            # Still create an entry even if no products found
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'croma_links': []
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
                        print("Failed to search Croma")
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
                                'croma_links': []
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
                    if any(error_type in str(e).lower() for error_type in ['timeout', 'connection', 'pool', 'http', 'network', 'max retries', 'too many open files']):
                        if self.handle_error_with_backup(f"Connection error: {e}", search_query, index):
                            continue  # Retry
                        else:
                            # Max retries reached, create empty entry and continue
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'croma_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.error_retry_count = 0  # Reset for next entry
                            self.save_progress()
                            continue
                    else:
                        # Other errors - create backup and continue
                        if self.handle_error_with_backup(error_msg, search_query, index):
                            continue  # Retry
                        else:
                            # Max retries reached, create empty entry and continue
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'croma_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.error_retry_count = 0  # Reset for next entry
                            self.save_progress()
                            continue
                    
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Saving progress...")
            self.save_progress()
            print("Progress saved. You can resume later by running the script again.")
            raise
    
    def save_results(self, output_file="croma_mobile_results.json"):
        """Save Croma results to JSON file with safe file handling"""
        
        if self.results:
            try:
                # Results are already in list format
                with self.safe_file_operation(output_file, 'w', 'utf-8') as f:
                    json.dump(self.results, f, indent=2, ensure_ascii=False)
                
                print(f"Croma results saved to {output_file}")
                print(f"Total entries processed: {len(self.results)}")
                
                # Also save as CSV for compatibility
                csv_file = output_file.replace('.json', '.csv')
                self.save_results_csv(csv_file)
                
            except (OSError, IOError) as e:
                print(f"File system error saving Croma results: {e}")
            except Exception as e:
                print(f"Unexpected error saving Croma results: {e}")
        else:
            print("No Croma results to save")
    
    def save_results_csv(self, output_file="croma_mobile_results.csv"):
        """Save results in CSV format for compatibility with safe file handling"""
        try:
            csv_data = []
            for entry in self.results:
                model_id = entry['model_id']
                product_name = entry['product_name']
                colour = entry['colour']
                ram_rom = entry['ram_rom']
                
                for link in entry['croma_links']:
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
                with self.safe_file_operation(output_file, 'w', 'utf-8') as f:
                    df.to_csv(f, index=False, encoding='utf-8')
                print(f"CSV results also saved to {output_file}")
        
        except (OSError, IOError) as e:
            print(f"File system error saving CSV results: {e}")
        except Exception as e:
            print(f"Unexpected error saving CSV results: {e}")
    
    def close(self):
        """Close the browser and cleanup all resources"""
        try:
            if self.driver:
                try:
                    self.driver.quit()
                except Exception as e:
                    print(f"Error closing driver: {e}")
                finally:
                    self.driver = None
                    self.wait = None
            
            # Final cleanup
            self.cleanup_resources()
            
            # Save final progress
            self.save_progress()
            
        except Exception as e:
            print(f"Error during cleanup: {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Croma Mobile Phone Scraper')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = CromaMobileScraper(headless=args.headless)
    
    try:
        # CSV file path
        csv_file = "expanded_permutations.csv"
        
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return
        
        # Start Croma scraping (process all entries)
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