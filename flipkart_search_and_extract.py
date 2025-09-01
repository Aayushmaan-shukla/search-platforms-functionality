import os
import csv
import json
import time
import importlib.util
import argparse
import signal
import sys
import requests
import random
import pandas as pd
from typing import List, Dict, Optional
from collections import defaultdict
from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import logging
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


# CSV_PATH = os.path.join(os.path.dirname(__file__), 'expanded_permutations.csv')
# OUTPUT_JSON_PATH = os.path.join(os.path.dirname(__file__), 'flipkart_product_links_and_names.json')
# OUTPUT_CSV_PATH = os.path.join(os.path.dirname(__file__), 'flipkart_product_links_and_names.csv')
# PROGRESS_FILE = os.path.join(os.path.dirname(__file__), 'flipkart_progress.json')
# TEMP_OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'flipkart_temp_output.json')
# BACKUP_DIR = os.path.join(os.path.dirname(__file__), 'backups')
# BACKUP_INTERVAL = 100  # Create backup every 100 rows
# MAX_RETRIES = 2  # Maximum retries before creating backup on error

# # Proxy configuration
# PROXYSCRAPE_API_KEY = "wvm4z69kf54pc9rod7ck"
# PROXYSCRAPE_API_URL = "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all&apikey=" + PROXYSCRAPE_API_KEY

# # Chrome session management
# CHROME_SESSION_RENEWAL_INTERVAL = 5  # Renew Chrome session after every 5 phones

# # Global variables for interrupt handling
# current_progress = 0
# total_queries = 0
# all_records = []
# driver = None
# current_proxy = None
# proxy_list = []
# proxy_rotation_count = 0
# use_proxy_mode = False  # Flag to track if we need to use proxies due to errors
# backup_count = 0  # Track backup count for cleanup
# last_backup_file = None  # Track last backup file for cleanup

# def get_proxy_list() -> List[str]:
#     """Fetch fresh proxy list from ProxyScrape API"""
#     try:
#         logging.info("Fetching fresh proxy list from ProxyScrape...")
#         response = requests.get(PROXYSCRAPE_API_URL, timeout=30)
#         response.raise_for_status()
        
#         # Parse proxy list (one proxy per line)
#         proxies = [proxy.strip() for proxy in response.text.strip().split('\n') if proxy.strip()]
        
#         if proxies:
#             logging.info(f"Successfully fetched {len(proxies)} proxies")
#             return proxies
#         else:
#             logging.warning("No proxies returned from API")
#             return []
            
#     except requests.exceptions.RequestException as e:
#         logging.error(f"Failed to fetch proxy list: {e}")
#         return []
#     except Exception as e:
#         logging.error(f"Unexpected error fetching proxies: {e}")
#         return []

# def get_next_proxy() -> Optional[str]:
#     """Get next proxy from the list, refresh if needed"""
#     global proxy_list, current_proxy, proxy_rotation_count
    
#     if not proxy_list:
#         proxy_list = get_proxy_list()
#         if not proxy_list:
#             logging.warning("No proxies available, continuing without proxy")
#             return None
    
#     # Rotate to next proxy
#     current_proxy = random.choice(proxy_list)
#     proxy_rotation_count += 1
    
#     logging.info(f"Rotating to proxy #{proxy_rotation_count}: {current_proxy}")
#     return current_proxy

# def should_renew_chrome_session(phone_count: int) -> bool:
#     """Check if Chrome session should be renewed based on phone count"""
#     return phone_count % CHROME_SESSION_RENEWAL_INTERVAL == 0

# def renew_chrome_session(driver, headless: bool = False, use_proxy: bool = False) -> uc.Chrome:
#     """Close current Chrome session and create a new one"""
#     global current_proxy
    
#     try:
#         logging.info("Renewing Chrome session...")
#         if driver:
#             driver.quit()
#             time.sleep(2)
#     except Exception as e:
#         logging.warning(f"Error closing previous driver: {e}")
    
#     # Only use proxy if explicitly requested (due to network errors)
#     if use_proxy:
#         new_proxy = get_next_proxy()
#         logging.info("Using proxy for new session due to previous network errors")
#     else:
#         new_proxy = None
#         logging.info("Using host IP for new session (no proxy)")
    
#     # Create new driver with or without proxy
#     new_driver = get_driver(headless=headless, proxy=new_proxy)
#     logging.info("Chrome session renewed successfully")
    
#     return new_driver

# def handle_proxy_error(driver, error: Exception, context: str = "") -> bool:
#     """Handle proxy-related errors and rotate proxy if needed"""
#     global use_proxy_mode
    
#     error_str = str(error).lower()
    
#     # Check for specific errors that warrant proxy rotation
#     should_rotate_proxy = any(keyword in error_str for keyword in [
#         'max retries reached',
#         'http connection pool',
#         'connection error',
#         'timeout',
#         'proxy',
#         'network error',
#         'connection refused',
#         'connection reset',
#         'ssl error',
#         'certificate error'
#     ])
    
#     if should_rotate_proxy:
#         logging.warning(f'Network/proxy error detected in {context}, switching to proxy mode...')
#         use_proxy_mode = True  # Set flag to use proxies going forward
        
#         try:
#             new_proxy = get_next_proxy()
#             if new_proxy:
#                 logging.info(f'Rotated to new proxy: {new_proxy}')
#                 return True
#             else:
#                 logging.warning('No new proxy available')
#                 return False
#         except Exception as proxy_error:
#             logging.error(f'Failed to rotate proxy: {proxy_error}')
#             return False
    
#     return False

# def create_driver_with_proxy(headless: bool = False) -> uc.Chrome:
#     """Create a new driver with proxy when network errors occur"""
#     global use_proxy_mode
    
#     if use_proxy_mode:
#         new_proxy = get_next_proxy()
#         logging.info(f'Creating new driver with proxy due to network errors: {new_proxy}')
#         return get_driver(headless=headless, proxy=new_proxy)
#     else:
#         logging.info('Creating new driver with host IP (no proxy)')
#         return get_driver(headless=headless, proxy=None)

# def signal_handler(signum, frame):
#     """Handle keyboard interrupt (Ctrl+C) gracefully"""
#     global current_progress, all_records, driver
#     print(f"\n\nKeyboard interrupt detected! Saving progress and creating backup...")
    
#     if all_records:
#         save_progress(current_progress, all_records)
#         # Create emergency backup
#         backup_path = create_backup(all_records, current_progress)
#         print(f"Progress saved! Completed {current_progress}/{total_queries} queries.")
#         print(f"Emergency backup created: {backup_path}")
#         print(f"Temporary data saved to {TEMP_OUTPUT_FILE}")
    
#     if driver:
#         try:
#             driver.quit()
#         except Exception:
#             pass
    
#     print("Script terminated safely. You can resume later by running the same command.")
#     sys.exit(0)

# def create_backup_dir():
#     """Create backup directory if it doesn't exist"""
#     if not os.path.exists(BACKUP_DIR):
#         os.makedirs(BACKUP_DIR)
#         logging.info(f"Created backup directory: {BACKUP_DIR}")

# def create_backup(records: List[Dict], completed_count: int) -> str:
#     """Create backup file and return the backup file path"""
#     global backup_count, last_backup_file
    
#     create_backup_dir()
    
#     timestamp = time.strftime("%Y%m%d_%H%M%S")
#     backup_filename = f"flipkart_backup_{completed_count}_{timestamp}.json"
#     backup_path = os.path.join(BACKUP_DIR, backup_filename)
    
#     # Save backup with atomic structure (each CSV row = separate JSON entry)
#     atomic_records = []
#     for record in records:
#         atomic_records.append({
#             'model_id': record['model_id'],
#             'product_name': record['product_name'],
#             'color': record['colour'],  # Rename to 'color' as requested
#             'variant': record['ram_rom'],  # Rename to 'variant' as requested
#             'url': record['url'],
#             'product_name_via_url': record['product_name_via_url']
#         })
    
#     with open(backup_path, 'w', encoding='utf-8') as f:
#         json.dump(atomic_records, f, ensure_ascii=False, indent=2)
    
#     backup_count += 1
#     logging.info(f"Backup created: {backup_path} ({len(atomic_records)} records)")
    
#     # Clean up previous backup if it exists
#     if last_backup_file and os.path.exists(last_backup_file):
#         try:
#             os.remove(last_backup_file)
#             logging.info(f"Previous backup cleaned up: {last_backup_file}")
#         except Exception as e:
#             logging.warning(f"Could not clean up previous backup {last_backup_file}: {e}")
    
#     last_backup_file = backup_path
#     return backup_path

# def save_progress(completed_count: int, records: List[Dict]):
#     """Save current progress and data to temporary files"""
#     # Save progress
#     progress_data = {
#         'completed_count': completed_count,
#         'total_queries': total_queries,
#         'timestamp': time.time()
#     }
    
#     with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
#         json.dump(progress_data, f, indent=2)
    
#     # Save temporary data
#     with open(TEMP_OUTPUT_FILE, 'w', encoding='utf-8') as f:
#         json.dump(records, f, ensure_ascii=False, indent=2)
    
#     # Create backup every 100 rows
#     if completed_count > 0 and completed_count % BACKUP_INTERVAL == 0:
#         create_backup(records, completed_count)

# def load_progress() -> tuple[int, List[Dict]]:
#     """Load previous progress and data if available"""
#     completed_count = 0
#     records = []
    
#     if os.path.exists(PROGRESS_FILE) and os.path.exists(TEMP_OUTPUT_FILE):
#         try:
#             # Load progress
#             with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
#                 progress_data = json.load(f)
#                 completed_count = progress_data.get('completed_count', 0)
            
#             # Load temporary data
#             with open(TEMP_OUTPUT_FILE, 'r', encoding='utf-8') as f:
#                 records = json.load(f)
            
#             print(f"Resuming from previous run: {completed_count} queries already completed")
#             print(f"Loaded {len(records)} existing records")
            
#         except Exception as e:
#             print(f"Warning: Could not load previous progress: {e}")
#             completed_count = 0
#             records = []
    
#     return completed_count, records

# def cleanup_temp_files():
#     """Clean up temporary files after successful completion"""
#     try:
#         if os.path.exists(PROGRESS_FILE):
#             os.remove(PROGRESS_FILE)
#         if os.path.exists(TEMP_OUTPUT_FILE):
#             os.remove(TEMP_OUTPUT_FILE)
#         print("Temporary files cleaned up")
#     except Exception as e:
#         print(f"Warning: Could not clean up temporary files: {e}")

# def generate_extraction_report(records: List[Dict]) -> Dict[str, int]:
#     """Generate a report on extraction success/failure rates"""
#     total_records = len(records)
#     successful_extractions = 0
#     failed_urls = 0
#     failed_names = 0
#     complete_failures = 0
    
#     for record in records:
#         has_url = record.get('url') is not None
#         has_name = record.get('product_name_via_url') is not None and record.get('product_name_via_url', '').strip()
        
#         if has_url and has_name:
#             successful_extractions += 1
#         elif not has_url and not has_name:
#             complete_failures += 1
#         elif not has_url:
#             failed_urls += 1
#         elif not has_name:
#             failed_names += 1
    
#     report = {
#         'total_records': total_records,
#         'successful_extractions': successful_extractions,
#         'failed_urls': failed_urls,
#         'failed_names': failed_names,
#         'complete_failures': complete_failures,
#         'success_rate': (successful_extractions / total_records * 100) if total_records > 0 else 0
#     }
    
#     return report


# def read_queries_from_csv(csv_path: str, limit: Optional[int] = None) -> List[Dict]:
#     queries: List[Dict] = []
#     with open(csv_path, 'r', encoding='utf-8', newline='') as f:
#         reader = csv.DictReader(f)
#         for row in reader:
#             model_id = row.get('model_id', '').strip()
#             product_name = row.get('product_name', '').strip()
#             colour = row.get('colour', '').strip()
#             ram_rom = row.get('ram_rom', '').strip()
            
#             if not product_name or not model_id:
#                 continue
                
#             query = ' '.join([p for p in [product_name, colour, ram_rom] if p])
#             queries.append({
#                 'model_id': model_id,
#                 'product_name': product_name,
#                 'colour': colour,
#                 'ram_rom': ram_rom,
#                 'query': query
#             })
            
#             if limit and len(queries) >= limit:
#                 break
#     return queries


# def get_driver(headless: bool = False, proxy: Optional[str] = None) -> uc.Chrome:
#     logging.debug('Initializing undetected_chromedriver (headless=%s, proxy=%s)', headless, proxy)
#     options = uc.ChromeOptions()
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument('--disable-gpu')
#     options.add_argument('--start-maximized')
    
#     # Add proxy if provided, otherwise use host IP
#     if proxy:
#         options.add_argument(f'--proxy-server={proxy}')
#         logging.info(f'Using proxy: {proxy}')
#     else:
#         logging.info('Using host IP address (no proxy)')
    
#     if headless:
#         # Use new headless mode for Chrome 109+
#         options.add_argument('--headless=new')
    
#     driver = uc.Chrome(options=options)
#     driver.set_page_load_timeout(45)
#     return driver


# def close_login_modal_if_present(driver):
#     try:
#         # If class="_3skCyB" overlay appears, close via <span role="button" class="_30XB9F">
#         logging.debug('Checking for login modal (_3skCyB)')
#         WebDriverWait(driver, 5).until(
#             EC.presence_of_element_located((By.CSS_SELECTOR, 'div._3skCyB'))
#         )
#         btn = WebDriverWait(driver, 5).until(
#             EC.element_to_be_clickable((By.CSS_SELECTOR, 'span._30XB9F[role="button"]'))
#         )
#         btn.click()
#         logging.info('Closed login modal (_3skCyB) via span._30XB9F')
#         time.sleep(0.5)
#     except Exception:
#         pass


# def take_debug_screenshot(driver, filename: str):
#     """Take a screenshot for debugging purposes"""
#     try:
#         # Create screenshots directory if it doesn't exist
#         screenshots_dir = os.path.join(os.path.dirname(__file__), 'debug_screenshots')
#         os.makedirs(screenshots_dir, exist_ok=True)
        
#         # Generate unique filename with timestamp
#         timestamp = time.strftime("%Y%m%d_%H%M%S")
#         screenshot_path = os.path.join(screenshots_dir, f"{filename}_{timestamp}.png")
        
#         driver.save_screenshot(screenshot_path)
#         logging.info('Debug screenshot saved: %s', screenshot_path)
        
#         # Also save the current page source for debugging
#         page_source_path = os.path.join(screenshots_dir, f"{filename}_{timestamp}.html")
#         with open(page_source_path, 'w', encoding='utf-8') as f:
#             f.write(driver.page_source)
#         logging.info('Page source saved: %s', page_source_path)
        
#     except Exception as e:
#         logging.error('Failed to take debug screenshot: %s', str(e))

# def perform_search_and_extract_links(driver, query: str, max_retries: int = MAX_RETRIES, headless: bool = False) -> List[str]:
#     for attempt in range(max_retries):
#         try:
#             logging.info('Search attempt %d/%d for query: %s', attempt + 1, max_retries, query)
            
#             logging.info('Navigating to Flipkart homepage')
#             driver.get('https://www.flipkart.com')
#             time.sleep(2)
#             close_login_modal_if_present(driver)

#             # Find search input (handle multiple possible classes)
#             search_input = None
#             selectors = [
#                 'input.Pke_EE',
#                 'input.zDPmFV',
#                 'form.header-form-search input[name="q"]',
#                 'input[title="Search for products, brands and more"]'
#             ]
#             for sel in selectors:
#                 try:
#                     search_input = WebDriverWait(driver, 5).until(
#                         EC.presence_of_element_located((By.CSS_SELECTOR, sel))
#                     )
#                     if search_input:
#                         logging.debug('Found search input by selector: %s', sel)
#                         break
#                 except TimeoutException:
#                     continue
#             if search_input is None:
#                 raise TimeoutException('Search input not found with known selectors')

#             search_input.clear()
#             search_input.send_keys(query)
#             search_input.submit()
#             logging.info('Submitted search query: %s', query)

#             # Wait for results to appear (container or product anchors)
#             try:
#                 WebDriverWait(driver, 12).until(
#                     EC.presence_of_element_located((By.CSS_SELECTOR, 'div._75nlfW'))
#                 )
#                 logging.debug('Results container (_75nlfW) detected')
#             except TimeoutException:
#                 WebDriverWait(driver, 12).until(
#                     EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/p/"]'))
#                 )
#                 logging.debug('Results anchors (/p/) detected via fallback selector')
            
#             # Close any modal that might have appeared after navigation
#             close_login_modal_if_present(driver)
#             time.sleep(2)

#             # Extract only the necessary elements instead of saving entire HTML
#             links = extract_product_links_from_page(driver.page_source, n=5)
            
#             if links and len(links) > 0:
#                 logging.info('Successfully extracted %d product links from search results', len(links))
#                 return links
#             else:
#                 logging.warning('Attempt %d: No links found for query: %s', attempt + 1, query)
#                 if attempt < max_retries - 1:
#                     take_debug_screenshot(driver, f"no_links_found_{hash(query) % 10000}")
#                     time.sleep(3)
#                 else:
#                     logging.error('All search attempts failed for query: %s', query)
#                     take_debug_screenshot(driver, f"search_failed_{hash(query) % 10000}")
#                     return []
                    
#         except Exception as e:
#             logging.error('Search attempt %d failed for query %s: %s', attempt + 1, query, str(e))
            
#             # Handle proxy errors and rotate if needed
#             proxy_rotated = handle_proxy_error(driver, e, f"search attempt {attempt + 1}")
            
#             # If proxy was rotated, create new driver with proxy
#             if proxy_rotated and use_proxy_mode:
#                 try:
#                     logging.info('Creating new driver with proxy due to network errors...')
#                     driver.quit()
#                     driver = create_driver_with_proxy(headless=headless)
#                 except Exception as driver_error:
#                     logging.error(f'Failed to create new driver with proxy: {driver_error}')
            
#             if attempt < max_retries - 1:
#                 logging.info('Waiting 3 seconds before retry...')
#                 time.sleep(3)
#                 # Take screenshot for debugging
#                 take_debug_screenshot(driver, f"search_error_{hash(query) % 10000}")
#             else:
#                 logging.error('All search attempts failed for query: %s', query)
#                 take_debug_screenshot(driver, f"search_failed_{hash(query) % 10000}")
#                 return []
    
#     return []


# def extract_product_links_from_page(html: str, n: int = 5) -> List[str]:
#     """Extract product links from HTML without saving the entire page"""
#     soup = BeautifulSoup(html, 'html.parser')
#     links: List[str] = []
    
#     # Preferred: within the known container
#     container = soup.find('div', class_='_75nlfW')
#     if container:
#         anchors = container.find_all('a', class_='wjcEIp')
#         for a in anchors:
#             href = a.get('href')
#             if href:
#                 if href.startswith('/'):
#                     href = 'https://www.flipkart.com' + href
#                 links.append(href)
#             if len(links) >= n:
#                 return links[:n]
    
#     # Fallback 1: any anchors with class commonly used for product cards
#     if len(links) < n:
#         for cls in ['wjcEIp', 'IRpwTa', 'CGtC98']:
#             anchors = soup.find_all('a', class_=cls)
#             for a in anchors:
#                 href = a.get('href')
#                 if href:
#                     if href.startswith('/'):
#                         href = 'https://www.flipkart.com' + href
#                     if '/p/' in href and href not in links:
#                         links.append(href)
#                 if len(links) >= n:
#                     return links[:n]
    
#     # Fallback 2: any anchor with product pattern
#     if len(links) < n:
#         anchors = soup.select('a[href*="/p/"]')
#         for a in anchors:
#             href = a.get('href')
#             if href:
#                 if href.startswith('/'):
#                     href = 'https://www.flipkart.com' + href
#                 if href not in links:
#                     links.append(href)
#             if len(links) >= n:
#                 break
    
#     return links


# def _load_flipkart_helper():
#     here = os.path.dirname(__file__)
#     helper_path = os.path.join(here, 'enhanced_flipkart_scraper_comprehensive.py')
#     spec = importlib.util.spec_from_file_location('flipkart_helper', helper_path)
#     if spec is None or spec.loader is None:
#         raise RuntimeError('Unable to load enhanced_flipkart_scraper_comprehensive.py')
#     module = importlib.util.module_from_spec(spec)
#     spec.loader.exec_module(module)
#     return module


# def extract_product_name_via_existing_helper(driver, url: str, max_retries: int = MAX_RETRIES) -> Optional[str]:
#     module = _load_flipkart_helper()
#     # Use minimal helper as requested
#     extract_product_name_via_url_minimal = getattr(module, 'extract_product_name_via_url_minimal')
    
#     for attempt in range(max_retries):
#         try:
#             logging.debug('Product name extraction attempt %d/%d for URL: %s', attempt + 1, max_retries, url)
            
#             driver.get(url)
#             time.sleep(2)
#             close_login_modal_if_present(driver)
            
#             # Minimal path returns product name directly
#             product_name = extract_product_name_via_url_minimal(driver)
            
#             if product_name and product_name.strip():
#                 logging.debug('Successfully extracted product name: %s', product_name)
#                 return product_name
#             else:
#                 logging.warning('Attempt %d: Got empty/null product name for URL: %s', attempt + 1, url)
#                 if attempt < max_retries - 1:
#                     time.sleep(1)
#                     continue
#                 else:
#                     logging.error('All product name extraction attempts failed for URL: %s', url)
#                     return None
                    
#         except Exception as e:
#             logging.error('Product name extraction attempt %d failed for URL %s: %s', attempt + 1, url, str(e))
            
#             # Handle proxy errors and rotate if needed
#             handle_proxy_error(driver, e, f"product extraction attempt {attempt + 1}")
            
#             if attempt < max_retries - 1:
#                 logging.info('Waiting 1 second before retry...')
#                 time.sleep(1)
#             else:
#                 logging.error('All product name extraction attempts failed for URL: %s', url)
#                 return None
    
#     return None


# def visit_links_and_collect_names(driver, links: List[str], max_retries: int = MAX_RETRIES) -> List[Dict[str, str]]:
#     collected: List[Dict[str, str]] = []
#     for link in links:
#         name = None
#         url = link
        
#         # Retry mechanism for failed extractions
#         for attempt in range(max_retries):
#             try:
#                 logging.info('Visiting product link (attempt %d/%d): %s', attempt + 1, max_retries, link)
#                 name = extract_product_name_via_existing_helper(driver, link)
                
#                 if name and name.strip():
#                     logging.info('Successfully extracted name: %s', name)
#                     break
#                 else:
#                     logging.warning('Attempt %d: Got empty/null name for URL: %s', attempt + 1, link)
                    
#             except Exception as e:
#                 logging.error('Attempt %d failed for URL %s: %s', attempt + 1, link, str(e))
                
#                 if attempt < max_retries - 1:
#                     logging.info('Waiting 2 seconds before retry...')
#                     time.sleep(2)
#                 else:
#                     logging.error('All retry attempts failed for URL: %s', link)
        
#         # If still no name after all retries, take screenshot and log
#         if not name or not name.strip():
#             logging.error('Failed to extract product name after %d attempts for URL: %s', max_retries, link)
#             take_debug_screenshot(driver, f"failed_extraction_{hash(link) % 10000}")
#             # Set URL to None to indicate failure
#             url = None
        
#         # Additional validation and logging for null cases
#         if not url or not name:
#             logging.warning('Incomplete data - URL: %s, Name: %s - Taking debug screenshot', url, name)
#             take_debug_screenshot(driver, f"incomplete_data_{hash(str(link)) % 10000}")
            
#             # Log the current page state for debugging
#             try:
#                 current_url = driver.current_url
#                 page_title = driver.title
#                 logging.debug('Current page URL: %s, Title: %s', current_url, page_title)
#             except Exception as e:
#                 logging.debug('Could not get current page info: %s', str(e))
        
#         collected.append({'url': url, 'product_name_via_url': name})
#         logging.info('Final result - URL: %s, Name: %s', url, name)
    
#     return collected


# def save_outputs(records: List[Dict], output_json_path: str, output_csv_path: str):
#     # Create atomic JSON structure - each CSV row becomes a separate JSON entry
#     atomic_json = []
#     for record in records:
#         atomic_json.append({
#             'model_id': record['model_id'],
#             'product_name': record['product_name'],
#             'color': record['colour'],  # Rename to 'color' as requested
#             'variant': record['ram_rom'],  # Rename to 'variant' as requested
#             'url': record['url'],
#             'product_name_via_url': record['product_name_via_url']
#         })
    
#     # Save atomic JSON
#     with open(output_json_path, 'w', encoding='utf-8') as jf:
#         json.dump(atomic_json, jf, ensure_ascii=False, indent=2)
    
#     # Save CSV with updated field names
#     with open(output_csv_path, 'w', encoding='utf-8', newline='') as cf:
#         writer = csv.DictWriter(cf, fieldnames=['model_id', 'product_name', 'color', 'variant', 'url', 'product_name_via_url'])
#         writer.writeheader()
#         for record in records:
#             writer.writerow({
#                 'model_id': record['model_id'],
#                 'product_name': record['product_name'],
#                 'color': record['colour'],
#                 'variant': record['ram_rom'],
#                 'url': record['url'],
#                 'product_name_via_url': record['product_name_via_url']
#             })


# def main():
#     global current_progress, total_queries, all_records, driver
    
#     parser = argparse.ArgumentParser(description='Flipkart search and extract tool')
#     parser.add_argument('--limit', type=int, default=None, help='Limit number of CSV rows to process')
#     parser.add_argument('--headless', action='store_true', help='Run Chrome in headless mode')
#     parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
#     parser.add_argument('--resume', action='store_true', help='Resume from previous run (auto-detected)')
#     args = parser.parse_args()

#     logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
#                         format='%(asctime)s %(levelname)s: %(message)s')

#     # Set up signal handler for graceful interruption
#     signal.signal(signal.SIGINT, signal_handler)
    
#     queries = read_queries_from_csv(CSV_PATH, limit=args.limit)
#     if not queries:
#         print('No queries found in CSV')
#         return
    
#     total_queries = len(queries)
    
#     # Initialize proxy list
#     print("Initializing proxy list...")
#     proxy_list = get_proxy_list()
#     if proxy_list:
#         print(f"Loaded {len(proxy_list)} proxies")
#     else:
#         print("Warning: No proxies available, continuing without proxy rotation")
    
#     # Load previous progress if available
#     completed_count, all_records = load_progress()
    
#     if completed_count > 0:
#         # Skip already completed queries
#         queries = queries[completed_count:]
#         print(f"Skipping {completed_count} already completed queries")
#         print(f"Remaining queries: {len(queries)}")
    
#     if not queries:
#         print("All queries already completed!")
#         # Final save and cleanup
#         save_outputs(all_records, OUTPUT_JSON_PATH, OUTPUT_CSV_PATH)
#         cleanup_temp_files()
#         return
    
#     # Initialize driver with host IP (no proxy initially)
#     driver = get_driver(headless=args.headless, proxy=None)
#     print("🚀 Starting with host IP address (no proxy)")
    
#     try:
#         for idx, query_data in enumerate(queries, start=completed_count + 1):
#             query = query_data['query']
#             current_progress = idx
            
#             # Show current mode (host IP vs proxy)
#             mode_status = "🔒 PROXY MODE" if use_proxy_mode else "🌐 HOST IP MODE"
#             print(f'[{idx}/{total_queries}] {mode_status} - Running Flipkart search for: {query}')
            
#             # Check if Chrome session should be renewed
#             if should_renew_chrome_session(idx):
#                 print(f'   Renewing Chrome session after {CHROME_SESSION_RENEWAL_INTERVAL} phones...')
#                 # Use proxy only if we've encountered network errors
#                 driver = renew_chrome_session(driver, headless=args.headless, use_proxy=use_proxy_mode)
            
#             try:
#                 # Extract links directly without saving HTML
#                 links = perform_search_and_extract_links(driver, query, headless=args.headless)
#                 print(f'   Found {len(links)} links')
                
#                 if links:
#                     results = visit_links_and_collect_names(driver, links)
#                     for r in results:
#                         r.update({
#                             'model_id': query_data['model_id'],
#                             'product_name': query_data['product_name'],
#                             'colour': query_data['colour'],
#                             'ram_rom': query_data['ram_rom']
#                         })
#                     all_records.extend(results)
                    
#                     # Save progress after each successful query
#                     save_progress(idx, all_records)
#                     print(f'   Progress saved: {idx}/{total_queries} completed')
#                 else:
#                     print(f'   No links found for query: {query}')
                    
#             except Exception as query_error:
#                 logging.error(f'Error processing query {idx}: {query_error}')
#                 # Create backup on query error after MAX_RETRIES attempts
#                 if all_records:
#                     backup_path = create_backup(all_records, idx)
#                     print(f'   Error backup created: {backup_path}')
                
#                 # Check if we should continue or stop
#                 error_str = str(query_error).lower()
#                 critical_errors = ['connection error', 'http connection pool', 'timeout', 'proxy', 'network error']
                
#                 if any(keyword in error_str for keyword in critical_errors):
#                     print(f'   Critical network error detected. Creating final backup and stopping...')
#                     if all_records:
#                         final_backup = create_backup(all_records, idx)
#                         print(f'   Final backup created: {final_backup}')
#                     break
#                 else:
#                     print(f'   Non-critical error, continuing with next query...')
        
#         # Generate and display extraction report
#         extraction_report = generate_extraction_report(all_records)
#         print("\n" + "="*60)
#         print("EXTRACTION REPORT")
#         print("="*60)
#         print(f"Total Records: {extraction_report['total_records']}")
#         print(f"Successful Extractions: {extraction_report['successful_extractions']}")
#         print(f"Failed URLs: {extraction_report['failed_urls']}")
#         print(f"Failed Names: {extraction_report['failed_names']}")
#         print(f"Complete Failures: {extraction_report['complete_failures']}")
#         print(f"Success Rate: {extraction_report['success_rate']:.2f}%")
#         print(f"Proxy Rotations: {proxy_rotation_count}")
#         print(f"Chrome Sessions Renewed: {proxy_rotation_count // CHROME_SESSION_RENEWAL_INTERVAL}")
#         print("="*60)
        
#         # Save the report to a separate file
#         report_file = os.path.join(os.path.dirname(__file__), 'extraction_report.json')
#         with open(report_file, 'w', encoding='utf-8') as f:
#             json.dump(extraction_report, f, indent=2)
#         print(f"Extraction report saved to: {report_file}")
        
#         # Final save and cleanup
#         save_outputs(all_records, OUTPUT_JSON_PATH, OUTPUT_CSV_PATH)
        
#         # Create final backup
#         if all_records:
#             final_backup = create_backup(all_records, total_queries)
#             print(f'Final backup created: {final_backup}')
        
#         cleanup_temp_files()
#         print(f'Saved outputs to {OUTPUT_JSON_PATH} and {OUTPUT_CSV_PATH}')
#         print(f'Successfully completed all {total_queries} queries!')
#         print(f'Total backups created: {backup_count}')
        
#     except KeyboardInterrupt:
#         # This should be handled by signal_handler, but just in case
#         pass
#     except Exception as e:
#         print(f"Error occurred: {e}")
#         # Save progress and create backup on error
#         if all_records:
#             save_progress(current_progress, all_records)
#             backup_path = create_backup(all_records, current_progress)
#             print(f"Progress saved due to error. Completed {current_progress}/{total_queries} queries.")
#             print(f"Emergency backup created: {backup_path}")
#     finally:
#         if driver:
#             try:
#                 driver.quit()
#             except Exception:
#                 pass


# if __name__ == '__main__':
#     main()


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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
import pandas as pd
import os
from datetime import datetime
import requests

class FlipkartMobileScraper:
    def __init__(self, headless=False):
        self.driver = None
        self.wait = None
        self.results = []  # Changed to list for atomic entries
        self.headless = headless
        self.progress_file = "flipkart_scraping_progress.json"
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
    
    def setup_driver(self, use_proxy=True):
        """Setup undetected Chrome driver with proxy support"""
        try:
            if self.driver:
                self.driver.quit()
            
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
            backup_file = f"flipkart_mobile_results_backup_{timestamp}.json"
            
            # Save current results to backup
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            print(f"Backup created: {backup_file}")
            
            # Find and delete previous backup files
            backup_files = [f for f in os.listdir('.') if f.startswith('flipkart_mobile_results_backup_') and f.endswith('.json')]
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
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            print(f"Error taking screenshot: {e}")
            return None

    def is_valid_product_url(self, url):
        """Check if the URL is a valid Flipkart product URL"""
        try:
            # Basic URL validation
            if not url or not isinstance(url, str):
                return False
            
            # Must be a Flipkart URL
            if not url.startswith('https://www.flipkart.com'):
                return False
            
            # Must contain product identifier '/p/'
            if '/p/' not in url:
                return False
            
            # Should not be just the homepage
            if url in ['https://www.flipkart.com', 'https://www.flipkart.com/']:
                return False
            
            # Should not contain search or category URLs
            invalid_patterns = [
                '/search?',
                '/q=',
                '?q=',
                '/category/',
                '/categories/',
                '/offers',
                '/deals',
                '/compare',
                '/account/',
                '/cart',
                '/checkout'
            ]
            
            for pattern in invalid_patterns:
                if pattern in url.lower():
                    return False
            
            # Additional check: product URLs typically have format like /product-name/p/itm[alphanumeric]
            # The URL should have both product name and product ID
            url_parts = url.split('/p/')
            if len(url_parts) != 2:
                return False
            
            # The part after /p/ should have some content (product ID)
            product_id_part = url_parts[1].split('?')[0]  # Remove query parameters
            if len(product_id_part) < 5:  # Product IDs are typically longer
                return False
            
            return True
            
        except Exception as e:
            print(f"Error validating URL {url}: {e}")
            return False

    def search_flipkart(self, search_query):
        """Search for products on Flipkart.com"""
        try:
            # Navigate to Flipkart.com
            self.driver.get("https://www.flipkart.com")
            time.sleep(random.uniform(2, 4))
            
            # Find and fill the search box
            search_box = self.wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "Pke_EE"))
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
            print(f"Error searching Flipkart: {e}")
            # Check if it's a proxy/connection error
            if any(error_type in str(e).lower() for error_type in ['timeout', 'connection', 'pool', 'http']):
                print("Detected connection/proxy error, switching IP...")
                self.change_session(change_proxy=True)  # Change proxy on error
                return False
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
    
    def extract_clean_product_name(self, link_element):
        """Extract clean product name from link element, filtering out UI text"""
        try:
            # First try to get text from the main link
            text = link_element.text.strip()
            
            # If no text in main link, try specific selectors for product names
            if not text:
                # Look for product name in common Flipkart selectors
                selectors = [
                    'div.KzDlHZ',  # Product name container
                    'div._4rR01T',  # Alternative product name
                    'a.IRpwTa',    # Product link text
                    'div.col-7-12', # Product details column
                    'h2',           # Generic heading
                    'span',         # Generic span
                    'div'           # Fallback div
                ]
                
                for selector in selectors:
                    try:
                        element = link_element.find_element(By.CSS_SELECTOR, selector)
                        if element and element.text.strip():
                            text = element.text.strip()
                            break
                    except:
                        continue
            
            if text:
                # Clean the text by removing unwanted UI elements
                text = self.clean_product_text(text)
                
                # Final validation - ensure we have actual product name
                if len(text) > 5 and not self.is_ui_text(text):
                    return text
            
            return None
            
        except Exception as e:
            print(f"Error extracting product name: {e}")
            return None
    
    def clean_product_text(self, text):
        """Clean product text by removing unwanted UI elements"""
        if not text:
            return ""
        
        # Remove common UI text patterns
        unwanted_patterns = [
            r"Add to Compare\s*",
            r"Compare\s*",
            r"₹[\d,]+\s*",  # Remove prices
            r"EMI\s*",
            r"No Cost EMI\s*",
            r"Free delivery\s*",
            r"Delivery\s*",
            r"Bank Offer\s*",
            r"Exchange Offer\s*",
            r"Special Price\s*",
            r"Save\s*",
            r"Extra\s*₹[\d,]+\s*off\s*",
            r"\d+%\s*off\s*",
            r"Bestseller\s*",
            r"Top Rated\s*",
            r"Sponsored\s*",
            r"Ad\s*",
            r"^\s*\n+",  # Remove leading newlines
            r"\n+\s*$",  # Remove trailing newlines
        ]
        
        for pattern in unwanted_patterns:
            text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.MULTILINE)
        
        # Clean up multiple spaces and newlines
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n+', ' ', text)
        
        return text.strip()
    
    def is_ui_text(self, text):
        """Check if text is likely UI text rather than product name"""
        if not text or len(text.strip()) < 5:
            return True
        
        text_lower = text.lower().strip()
        
        # Common UI text patterns that should be filtered out
        ui_patterns = [
            "add to compare",
            "compare",
            "sponsored",
            "bestseller",
            "top rated",
            "free delivery",
            "bank offer",
            "exchange offer",
            "special price",
            "no cost emi",
            "emi available",
            "save extra",
            "% off",
            "₹",
            "rs.",
            "rupees"
        ]
        
        # Check if text contains only UI elements
        for pattern in ui_patterns:
            if text_lower == pattern or text_lower.startswith(pattern):
                return True
        
        # If text is very short and doesn't look like a product name
        if len(text) < 10 and not any(char.isalpha() for char in text):
            return True
        
        return False

    def extract_product_links(self, max_products=5):
        """Extract product links and text from Flipkart search results"""
        products = []
        
        try:
            # Look for product links with the specified class pattern
            # Flipkart uses various class patterns for product links
            link_selectors = [
                # Primary product card links (most important) - only product links
                "a.CGtC98[href*='/p/']",
                # Alternative product link classes found in Flipkart - only product links
                "a.wjcEIp[href*='/p/']", 
                "a.IRpwTa[href*='/p/']",
                # Generic product page links within product containers
                "div._75nlfW a[href*='/p/']",
                "div._1AtVbE a[href*='/p/']",
                # More specific product link patterns
                "a[href*='/p/itm']",
                # Fallback generic product links (least priority)
                "a[href*='/p/']"
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
            for i, link in enumerate(links[:max_products * 2]):  # Get more links to filter better
                try:
                    href = link.get_attribute('href')
                    text = self.extract_clean_product_name(link)
                    
                    # Skip if we already have enough products
                    if len(products) >= max_products:
                        break
                    
                    if href and text:
                        # Ensure absolute URL for Flipkart
                        if href.startswith('/'):
                            href = f"https://www.flipkart.com{href}"
                        
                        # Validate that this is actually a product URL
                        if self.is_valid_product_url(href):
                            # Additional check: avoid duplicate URLs
                            existing_urls = [p['url'] for p in products]
                            if href not in existing_urls:
                                products.append({
                                    'url': href,
                                    'product_name_via_url': text
                                })
                                print(f"Flipkart Product {len(products)}: {text[:50]}...")
                            else:
                                print(f"Skipping duplicate URL: {href[:50]}...")
                        else:
                            print(f"Skipping non-product URL: {href[:50]}...")
                        
                except Exception as e:
                    print(f"Error extracting Flipkart product {i+1}: {e}")
                    continue
            
        except Exception as e:
            print(f"Error extracting Flipkart product links: {e}")
        
        print(f"Successfully filtered and extracted {len(products)} valid product links")
        return products
    
    def scrape_permutations(self, csv_file_path):
        """Scrape Flipkart for each permutation in the CSV file"""
        
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
                    
                    # Search on Flipkart
                    if self.search_flipkart(search_query):
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
                                'flipkart_links': products
                            }
                            
                            # Add to results list (each row is a separate entry)
                            self.results.append(entry)
                            
                            print(f"Extracted {len(products)} Flipkart products for entry {index + 1}")
                        else:
                            print("No Flipkart products found")
                            # Take screenshot for no products found
                            self.take_screenshot("no_products", search_query, index)
                            
                            # Still create an entry even if no products found
                            entry = {
                                'product_name': product_name,
                                'colour': colour,
                                'ram_rom': ram_rom,
                                'model_id': model_id,
                                'flipkart_links': []
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
                        print("Failed to search Flipkart")
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
                                'flipkart_links': []
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
                                'flipkart_links': []
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
    
    def save_results(self, output_file="flipkart_mobile_results.json"):
        """Save Flipkart results to JSON file"""
        
        if self.results:
            try:
                # Results are already in list format
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(self.results, f, indent=2, ensure_ascii=False)
                
                print(f"Flipkart results saved to {output_file}")
                print(f"Total entries processed: {len(self.results)}")
                
                # Also save as CSV for compatibility
                csv_file = output_file.replace('.json', '.csv')
                self.save_results_csv(csv_file)
                
            except Exception as e:
                print(f"Error saving Flipkart results: {e}")
        else:
            print("No Flipkart results to save")
    
    def save_results_csv(self, output_file="flipkart_mobile_results.csv"):
        """Save results in CSV format for compatibility"""
        try:
            csv_data = []
            for entry in self.results:
                model_id = entry['model_id']
                product_name = entry['product_name']
                colour = entry['colour']
                ram_rom = entry['ram_rom']
                
                for link in entry['flipkart_links']:
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
    parser = argparse.ArgumentParser(description='Flipkart Mobile Phone Scraper')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = FlipkartMobileScraper(headless=args.headless)
    
    try:
        # CSV file path
        csv_file = "expanded_permutations.csv"
        
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return
        
        # Start Flipkart scraping (process all entries)
        print("=== STARTING FLIPKART SCRAPING ===")
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