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
from typing import List, Dict, Optional
from collections import defaultdict

from bs4 import BeautifulSoup
import undetected_chromedriver as uc
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException


CSV_PATH = os.path.join(os.path.dirname(__file__), 'expanded_permutations.csv')
OUTPUT_JSON_PATH = os.path.join(os.path.dirname(__file__), 'flipkart_product_links_and_names.json')
OUTPUT_CSV_PATH = os.path.join(os.path.dirname(__file__), 'flipkart_product_links_and_names.csv')
PROGRESS_FILE = os.path.join(os.path.dirname(__file__), 'flipkart_progress.json')
TEMP_OUTPUT_FILE = os.path.join(os.path.dirname(__file__), 'flipkart_temp_output.json')

# Proxy configuration
PROXYSCRAPE_API_KEY = "wvm4z69kf54pc9rod7ck"
PROXYSCRAPE_API_URL = "https://api.proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all&apikey=" + PROXYSCRAPE_API_KEY

# Chrome session management
CHROME_SESSION_RENEWAL_INTERVAL = 5  # Renew Chrome session after every 5 phones

# Global variables for interrupt handling
current_progress = 0
total_queries = 0
all_records = []
driver = None
current_proxy = None
proxy_list = []
proxy_rotation_count = 0
use_proxy_mode = False  # Flag to track if we need to use proxies due to errors

def get_proxy_list() -> List[str]:
    """Fetch fresh proxy list from ProxyScrape API"""
    try:
        logging.info("Fetching fresh proxy list from ProxyScrape...")
        response = requests.get(PROXYSCRAPE_API_URL, timeout=30)
        response.raise_for_status()
        
        # Parse proxy list (one proxy per line)
        proxies = [proxy.strip() for proxy in response.text.strip().split('\n') if proxy.strip()]
        
        if proxies:
            logging.info(f"Successfully fetched {len(proxies)} proxies")
            return proxies
        else:
            logging.warning("No proxies returned from API")
            return []
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch proxy list: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error fetching proxies: {e}")
        return []

def get_next_proxy() -> Optional[str]:
    """Get next proxy from the list, refresh if needed"""
    global proxy_list, current_proxy, proxy_rotation_count
    
    if not proxy_list:
        proxy_list = get_proxy_list()
        if not proxy_list:
            logging.warning("No proxies available, continuing without proxy")
            return None
    
    # Rotate to next proxy
    current_proxy = random.choice(proxy_list)
    proxy_rotation_count += 1
    
    logging.info(f"Rotating to proxy #{proxy_rotation_count}: {current_proxy}")
    return current_proxy

def should_renew_chrome_session(phone_count: int) -> bool:
    """Check if Chrome session should be renewed based on phone count"""
    return phone_count % CHROME_SESSION_RENEWAL_INTERVAL == 0

def renew_chrome_session(driver, headless: bool = False, use_proxy: bool = False) -> uc.Chrome:
    """Close current Chrome session and create a new one"""
    global current_proxy
    
    try:
        logging.info("Renewing Chrome session...")
        if driver:
            driver.quit()
            time.sleep(2)
    except Exception as e:
        logging.warning(f"Error closing previous driver: {e}")
    
    # Only use proxy if explicitly requested (due to network errors)
    if use_proxy:
        new_proxy = get_next_proxy()
        logging.info("Using proxy for new session due to previous network errors")
    else:
        new_proxy = None
        logging.info("Using host IP for new session (no proxy)")
    
    # Create new driver with or without proxy
    new_driver = get_driver(headless=headless, proxy=new_proxy)
    logging.info("Chrome session renewed successfully")
    
    return new_driver

def handle_proxy_error(driver, error: Exception, context: str = "") -> bool:
    """Handle proxy-related errors and rotate proxy if needed"""
    global use_proxy_mode
    
    error_str = str(error).lower()
    
    # Check for specific errors that warrant proxy rotation
    should_rotate_proxy = any(keyword in error_str for keyword in [
        'max retries reached',
        'http connection pool',
        'connection error',
        'timeout',
        'proxy',
        'network error',
        'connection refused',
        'connection reset',
        'ssl error',
        'certificate error'
    ])
    
    if should_rotate_proxy:
        logging.warning(f'Network/proxy error detected in {context}, switching to proxy mode...')
        use_proxy_mode = True  # Set flag to use proxies going forward
        
        try:
            new_proxy = get_next_proxy()
            if new_proxy:
                logging.info(f'Rotated to new proxy: {new_proxy}')
                return True
            else:
                logging.warning('No new proxy available')
                return False
        except Exception as proxy_error:
            logging.error(f'Failed to rotate proxy: {proxy_error}')
            return False
    
    return False

def create_driver_with_proxy(headless: bool = False) -> uc.Chrome:
    """Create a new driver with proxy when network errors occur"""
    global use_proxy_mode
    
    if use_proxy_mode:
        new_proxy = get_next_proxy()
        logging.info(f'Creating new driver with proxy due to network errors: {new_proxy}')
        return get_driver(headless=headless, proxy=new_proxy)
    else:
        logging.info('Creating new driver with host IP (no proxy)')
        return get_driver(headless=headless, proxy=None)

def signal_handler(signum, frame):
    """Handle keyboard interrupt (Ctrl+C) gracefully"""
    global current_progress, all_records, driver
    print(f"\n\nKeyboard interrupt detected! Saving progress...")
    
    if all_records:
        save_progress(current_progress, all_records)
        print(f"Progress saved! Completed {current_progress}/{total_queries} queries.")
        print(f"Temporary data saved to {TEMP_OUTPUT_FILE}")
    
    if driver:
        try:
            driver.quit()
        except Exception:
            pass
    
    print("Script terminated safely. You can resume later by running the same command.")
    sys.exit(0)

def save_progress(completed_count: int, records: List[Dict]):
    """Save current progress and data to temporary files"""
    # Save progress
    progress_data = {
        'completed_count': completed_count,
        'total_queries': total_queries,
        'timestamp': time.time()
    }
    
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress_data, f, indent=2)
    
    # Save temporary data
    with open(TEMP_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

def load_progress() -> tuple[int, List[Dict]]:
    """Load previous progress and data if available"""
    completed_count = 0
    records = []
    
    if os.path.exists(PROGRESS_FILE) and os.path.exists(TEMP_OUTPUT_FILE):
        try:
            # Load progress
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                completed_count = progress_data.get('completed_count', 0)
            
            # Load temporary data
            with open(TEMP_OUTPUT_FILE, 'r', encoding='utf-8') as f:
                records = json.load(f)
            
            print(f"Resuming from previous run: {completed_count} queries already completed")
            print(f"Loaded {len(records)} existing records")
            
        except Exception as e:
            print(f"Warning: Could not load previous progress: {e}")
            completed_count = 0
            records = []
    
    return completed_count, records

def cleanup_temp_files():
    """Clean up temporary files after successful completion"""
    try:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
        if os.path.exists(TEMP_OUTPUT_FILE):
            os.remove(TEMP_OUTPUT_FILE)
        print("Temporary files cleaned up")
    except Exception as e:
        print(f"Warning: Could not clean up temporary files: {e}")

def generate_extraction_report(records: List[Dict]) -> Dict[str, int]:
    """Generate a report on extraction success/failure rates"""
    total_records = len(records)
    successful_extractions = 0
    failed_urls = 0
    failed_names = 0
    complete_failures = 0
    
    for record in records:
        has_url = record.get('url') is not None
        has_name = record.get('product_name_via_url') is not None and record.get('product_name_via_url', '').strip()
        
        if has_url and has_name:
            successful_extractions += 1
        elif not has_url and not has_name:
            complete_failures += 1
        elif not has_url:
            failed_urls += 1
        elif not has_name:
            failed_names += 1
    
    report = {
        'total_records': total_records,
        'successful_extractions': successful_extractions,
        'failed_urls': failed_urls,
        'failed_names': failed_names,
        'complete_failures': complete_failures,
        'success_rate': (successful_extractions / total_records * 100) if total_records > 0 else 0
    }
    
    return report


def read_queries_from_csv(csv_path: str, limit: Optional[int] = None) -> List[Dict]:
    queries: List[Dict] = []
    with open(csv_path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            model_id = row.get('model_id', '').strip()
            product_name = row.get('product_name', '').strip()
            colour = row.get('colour', '').strip()
            ram_rom = row.get('ram_rom', '').strip()
            
            if not product_name or not model_id:
                continue
                
            query = ' '.join([p for p in [product_name, colour, ram_rom] if p])
            queries.append({
                'model_id': model_id,
                'product_name': product_name,
                'colour': colour,
                'ram_rom': ram_rom,
                'query': query
            })
            
            if limit and len(queries) >= limit:
                break
    return queries


def get_driver(headless: bool = False, proxy: Optional[str] = None) -> uc.Chrome:
    logging.debug('Initializing undetected_chromedriver (headless=%s, proxy=%s)', headless, proxy)
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--start-maximized')
    
    # Add proxy if provided, otherwise use host IP
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')
        logging.info(f'Using proxy: {proxy}')
    else:
        logging.info('Using host IP address (no proxy)')
    
    if headless:
        # Use new headless mode for Chrome 109+
        options.add_argument('--headless=new')
    
    driver = uc.Chrome(options=options)
    driver.set_page_load_timeout(45)
    return driver


def close_login_modal_if_present(driver):
    try:
        # If class="_3skCyB" overlay appears, close via <span role="button" class="_30XB9F">
        logging.debug('Checking for login modal (_3skCyB)')
        WebDriverWait(driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'div._3skCyB'))
        )
        btn = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'span._30XB9F[role="button"]'))
        )
        btn.click()
        logging.info('Closed login modal (_3skCyB) via span._30XB9F')
        time.sleep(0.5)
    except Exception:
        pass


def take_debug_screenshot(driver, filename: str):
    """Take a screenshot for debugging purposes"""
    try:
        # Create screenshots directory if it doesn't exist
        screenshots_dir = os.path.join(os.path.dirname(__file__), 'debug_screenshots')
        os.makedirs(screenshots_dir, exist_ok=True)
        
        # Generate unique filename with timestamp
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(screenshots_dir, f"{filename}_{timestamp}.png")
        
        driver.save_screenshot(screenshot_path)
        logging.info('Debug screenshot saved: %s', screenshot_path)
        
        # Also save the current page source for debugging
        page_source_path = os.path.join(screenshots_dir, f"{filename}_{timestamp}.html")
        with open(page_source_path, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logging.info('Page source saved: %s', page_source_path)
        
    except Exception as e:
        logging.error('Failed to take debug screenshot: %s', str(e))

def perform_search_and_extract_links(driver, query: str, max_retries: int = 3, headless: bool = False) -> List[str]:
    for attempt in range(max_retries):
        try:
            logging.info('Search attempt %d/%d for query: %s', attempt + 1, max_retries, query)
            
            logging.info('Navigating to Flipkart homepage')
            driver.get('https://www.flipkart.com')
            time.sleep(2)
            close_login_modal_if_present(driver)

            # Find search input (handle multiple possible classes)
            search_input = None
            selectors = [
                'input.Pke_EE',
                'input.zDPmFV',
                'form.header-form-search input[name="q"]',
                'input[title="Search for products, brands and more"]'
            ]
            for sel in selectors:
                try:
                    search_input = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, sel))
                    )
                    if search_input:
                        logging.debug('Found search input by selector: %s', sel)
                        break
                except TimeoutException:
                    continue
            if search_input is None:
                raise TimeoutException('Search input not found with known selectors')

            search_input.clear()
            search_input.send_keys(query)
            search_input.submit()
            logging.info('Submitted search query: %s', query)

            # Wait for results to appear (container or product anchors)
            try:
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'div._75nlfW'))
                )
                logging.debug('Results container (_75nlfW) detected')
            except TimeoutException:
                WebDriverWait(driver, 12).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/p/"]'))
                )
                logging.debug('Results anchors (/p/) detected via fallback selector')
            
            # Close any modal that might have appeared after navigation
            close_login_modal_if_present(driver)
            time.sleep(2)

            # Extract only the necessary elements instead of saving entire HTML
            links = extract_product_links_from_page(driver.page_source, n=5)
            
            if links and len(links) > 0:
                logging.info('Successfully extracted %d product links from search results', len(links))
                return links
            else:
                logging.warning('Attempt %d: No links found for query: %s', attempt + 1, query)
                if attempt < max_retries - 1:
                    take_debug_screenshot(driver, f"no_links_found_{hash(query) % 10000}")
                    time.sleep(3)
                else:
                    logging.error('All search attempts failed for query: %s', query)
                    take_debug_screenshot(driver, f"search_failed_{hash(query) % 10000}")
                    return []
                    
        except Exception as e:
            logging.error('Search attempt %d failed for query %s: %s', attempt + 1, query, str(e))
            
            # Handle proxy errors and rotate if needed
            proxy_rotated = handle_proxy_error(driver, e, f"search attempt {attempt + 1}")
            
            # If proxy was rotated, create new driver with proxy
            if proxy_rotated and use_proxy_mode:
                try:
                    logging.info('Creating new driver with proxy due to network errors...')
                    driver.quit()
                    driver = create_driver_with_proxy(headless=headless)
                except Exception as driver_error:
                    logging.error(f'Failed to create new driver with proxy: {driver_error}')
            
            if attempt < max_retries - 1:
                logging.info('Waiting 3 seconds before retry...')
                time.sleep(3)
                # Take screenshot for debugging
                take_debug_screenshot(driver, f"search_error_{hash(query) % 10000}")
            else:
                logging.error('All search attempts failed for query: %s', query)
                take_debug_screenshot(driver, f"search_failed_{hash(query) % 10000}")
                return []
    
    return []


def extract_product_links_from_page(html: str, n: int = 5) -> List[str]:
    """Extract product links from HTML without saving the entire page"""
    soup = BeautifulSoup(html, 'html.parser')
    links: List[str] = []
    
    # Preferred: within the known container
    container = soup.find('div', class_='_75nlfW')
    if container:
        anchors = container.find_all('a', class_='wjcEIp')
        for a in anchors:
            href = a.get('href')
            if href:
                if href.startswith('/'):
                    href = 'https://www.flipkart.com' + href
                links.append(href)
            if len(links) >= n:
                return links[:n]
    
    # Fallback 1: any anchors with class commonly used for product cards
    if len(links) < n:
        for cls in ['wjcEIp', 'IRpwTa', 'CGtC98']:
            anchors = soup.find_all('a', class_=cls)
            for a in anchors:
                href = a.get('href')
                if href:
                    if href.startswith('/'):
                        href = 'https://www.flipkart.com' + href
                    if '/p/' in href and href not in links:
                        links.append(href)
                if len(links) >= n:
                    return links[:n]
    
    # Fallback 2: any anchor with product pattern
    if len(links) < n:
        anchors = soup.select('a[href*="/p/"]')
        for a in anchors:
            href = a.get('href')
            if href:
                if href.startswith('/'):
                    href = 'https://www.flipkart.com' + href
                if href not in links:
                    links.append(href)
            if len(links) >= n:
                break
    
    return links


def _load_flipkart_helper():
    here = os.path.dirname(__file__)
    helper_path = os.path.join(here, 'enhanced_flipkart_scraper_comprehensive.py')
    spec = importlib.util.spec_from_file_location('flipkart_helper', helper_path)
    if spec is None or spec.loader is None:
        raise RuntimeError('Unable to load enhanced_flipkart_scraper_comprehensive.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def extract_product_name_via_existing_helper(driver, url: str, max_retries: int = 2) -> Optional[str]:
    module = _load_flipkart_helper()
    # Use minimal helper as requested
    extract_product_name_via_url_minimal = getattr(module, 'extract_product_name_via_url_minimal')
    
    for attempt in range(max_retries):
        try:
            logging.debug('Product name extraction attempt %d/%d for URL: %s', attempt + 1, max_retries, url)
            
            driver.get(url)
            time.sleep(2)
            close_login_modal_if_present(driver)
            
            # Minimal path returns product name directly
            product_name = extract_product_name_via_url_minimal(driver)
            
            if product_name and product_name.strip():
                logging.debug('Successfully extracted product name: %s', product_name)
                return product_name
            else:
                logging.warning('Attempt %d: Got empty/null product name for URL: %s', attempt + 1, url)
                if attempt < max_retries - 1:
                    time.sleep(1)
                    continue
                else:
                    logging.error('All product name extraction attempts failed for URL: %s', url)
                    return None
                    
        except Exception as e:
            logging.error('Product name extraction attempt %d failed for URL %s: %s', attempt + 1, url, str(e))
            
            # Handle proxy errors and rotate if needed
            handle_proxy_error(driver, e, f"product extraction attempt {attempt + 1}")
            
            if attempt < max_retries - 1:
                logging.info('Waiting 1 second before retry...')
                time.sleep(1)
            else:
                logging.error('All product name extraction attempts failed for URL: %s', url)
                return None
    
    return None


def visit_links_and_collect_names(driver, links: List[str], max_retries: int = 3) -> List[Dict[str, str]]:
    collected: List[Dict[str, str]] = []
    for link in links:
        name = None
        url = link
        
        # Retry mechanism for failed extractions
        for attempt in range(max_retries):
            try:
                logging.info('Visiting product link (attempt %d/%d): %s', attempt + 1, max_retries, link)
                name = extract_product_name_via_existing_helper(driver, link)
                
                if name and name.strip():
                    logging.info('Successfully extracted name: %s', name)
                    break
                else:
                    logging.warning('Attempt %d: Got empty/null name for URL: %s', attempt + 1, link)
                    
            except Exception as e:
                logging.error('Attempt %d failed for URL %s: %s', attempt + 1, link, str(e))
                
                if attempt < max_retries - 1:
                    logging.info('Waiting 2 seconds before retry...')
                    time.sleep(2)
                else:
                    logging.error('All retry attempts failed for URL: %s', link)
        
        # If still no name after all retries, take screenshot and log
        if not name or not name.strip():
            logging.error('Failed to extract product name after %d attempts for URL: %s', max_retries, link)
            take_debug_screenshot(driver, f"failed_extraction_{hash(link) % 10000}")
            # Set URL to None to indicate failure
            url = None
        
        # Additional validation and logging for null cases
        if not url or not name:
            logging.warning('Incomplete data - URL: %s, Name: %s - Taking debug screenshot', url, name)
            take_debug_screenshot(driver, f"incomplete_data_{hash(str(link)) % 10000}")
            
            # Log the current page state for debugging
            try:
                current_url = driver.current_url
                page_title = driver.title
                logging.debug('Current page URL: %s, Title: %s', current_url, page_title)
            except Exception as e:
                logging.debug('Could not get current page info: %s', str(e))
        
        collected.append({'url': url, 'product_name_via_url': name})
        logging.info('Final result - URL: %s, Name: %s', url, name)
    
    return collected


def save_outputs(records: List[Dict], output_json_path: str, output_csv_path: str):
    # Group by model_id, product_name, colour, and ram_rom
    grouped_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(list))))
    
    for record in records:
        model_id = record['model_id']
        product_name = record['product_name']
        colour = record['colour']
        ram_rom = record['ram_rom']
        
        grouped_data[model_id][product_name][colour][ram_rom].append({
            'url': record['url'],
            'product_name_via_url': record['product_name_via_url']
        })
    
    # Convert to final JSON structure matching the required format
    final_json = []
    for model_id, products in grouped_data.items():
        for product_name, colours in products.items():
            for colour, ram_roms in colours.items():
                for ram_rom, flipkart_names in ram_roms.items():
                    final_json.append({
                        'model_id': model_id,
                        'product_name': product_name,
                        'colour': colour,
                        'ram_rom': ram_rom,
                        'flipkart_names': flipkart_names
                    })
    
    # Save JSON
    with open(output_json_path, 'w', encoding='utf-8') as jf:
        json.dump(final_json, jf, ensure_ascii=False, indent=2)
    
    # Save CSV
    with open(output_csv_path, 'w', encoding='utf-8', newline='') as cf:
        writer = csv.DictWriter(cf, fieldnames=['model_id', 'product_name', 'colour', 'ram_rom', 'url', 'product_name_via_url'])
        writer.writeheader()
        for record in records:
            writer.writerow({
                'model_id': record['model_id'],
                'product_name': record['product_name'],
                'colour': record['colour'],
                'ram_rom': record['ram_rom'],
                'url': record['url'],
                'product_name_via_url': record['product_name_via_url']
            })


def main():
    global current_progress, total_queries, all_records, driver
    
    parser = argparse.ArgumentParser(description='Flipkart search and extract tool')
    parser.add_argument('--limit', type=int, default=None, help='Limit number of CSV rows to process')
    parser.add_argument('--headless', action='store_true', help='Run Chrome in headless mode')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    parser.add_argument('--resume', action='store_true', help='Resume from previous run (auto-detected)')
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format='%(asctime)s %(levelname)s: %(message)s')

    # Set up signal handler for graceful interruption
    signal.signal(signal.SIGINT, signal_handler)
    
    queries = read_queries_from_csv(CSV_PATH, limit=args.limit)
    if not queries:
        print('No queries found in CSV')
        return
    
    total_queries = len(queries)
    
    # Initialize proxy list
    print("Initializing proxy list...")
    proxy_list = get_proxy_list()
    if proxy_list:
        print(f"Loaded {len(proxy_list)} proxies")
    else:
        print("Warning: No proxies available, continuing without proxy rotation")
    
    # Load previous progress if available
    completed_count, all_records = load_progress()
    
    if completed_count > 0:
        # Skip already completed queries
        queries = queries[completed_count:]
        print(f"Skipping {completed_count} already completed queries")
        print(f"Remaining queries: {len(queries)}")
    
    if not queries:
        print("All queries already completed!")
        # Final save and cleanup
        save_outputs(all_records, OUTPUT_JSON_PATH, OUTPUT_CSV_PATH)
        cleanup_temp_files()
        return
    
    # Initialize driver with host IP (no proxy initially)
    driver = get_driver(headless=args.headless, proxy=None)
    print("🚀 Starting with host IP address (no proxy)")
    
    try:
        for idx, query_data in enumerate(queries, start=completed_count + 1):
            query = query_data['query']
            current_progress = idx
            
            # Show current mode (host IP vs proxy)
            mode_status = "🔒 PROXY MODE" if use_proxy_mode else "🌐 HOST IP MODE"
            print(f'[{idx}/{total_queries}] {mode_status} - Running Flipkart search for: {query}')
            
            # Check if Chrome session should be renewed
            if should_renew_chrome_session(idx):
                print(f'   Renewing Chrome session after {CHROME_SESSION_RENEWAL_INTERVAL} phones...')
                # Use proxy only if we've encountered network errors
                driver = renew_chrome_session(driver, headless=args.headless, use_proxy=use_proxy_mode)
            
            # Extract links directly without saving HTML
            links = perform_search_and_extract_links(driver, query, headless=args.headless)
            print(f'   Found {len(links)} links')
            
            if links:
                results = visit_links_and_collect_names(driver, links)
                for r in results:
                    r.update({
                        'model_id': query_data['model_id'],
                        'product_name': query_data['product_name'],
                        'colour': query_data['colour'],
                        'ram_rom': query_data['ram_rom']
                    })
                all_records.extend(results)
                
                # Save progress after each successful query
                save_progress(idx, all_records)
                print(f'   Progress saved: {idx}/{total_queries} completed')
            else:
                print(f'   No links found for query: {query}')
        
        # Generate and display extraction report
        extraction_report = generate_extraction_report(all_records)
        print("\n" + "="*60)
        print("EXTRACTION REPORT")
        print("="*60)
        print(f"Total Records: {extraction_report['total_records']}")
        print(f"Successful Extractions: {extraction_report['successful_extractions']}")
        print(f"Failed URLs: {extraction_report['failed_urls']}")
        print(f"Failed Names: {extraction_report['failed_names']}")
        print(f"Complete Failures: {extraction_report['complete_failures']}")
        print(f"Success Rate: {extraction_report['success_rate']:.2f}%")
        print(f"Proxy Rotations: {proxy_rotation_count}")
        print(f"Chrome Sessions Renewed: {proxy_rotation_count // CHROME_SESSION_RENEWAL_INTERVAL}")
        print("="*60)
        
        # Save the report to a separate file
        report_file = os.path.join(os.path.dirname(__file__), 'extraction_report.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(extraction_report, f, indent=2)
        print(f"Extraction report saved to: {report_file}")
        
        # Final save and cleanup
        save_outputs(all_records, OUTPUT_JSON_PATH, OUTPUT_CSV_PATH)
        cleanup_temp_files()
        print(f'Saved outputs to {OUTPUT_JSON_PATH} and {OUTPUT_CSV_PATH}')
        print(f'Successfully completed all {total_queries} queries!')
        
    except KeyboardInterrupt:
        # This should be handled by signal_handler, but just in case
        pass
    except Exception as e:
        print(f"Error occurred: {e}")
        # Save progress even on error
        if all_records:
            save_progress(current_progress, all_records)
            print(f"Progress saved due to error. Completed {current_progress}/{total_queries} queries.")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


if __name__ == '__main__':
    main()


