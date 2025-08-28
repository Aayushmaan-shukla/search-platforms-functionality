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

class CromaMobileScrapper:  
    def __init__(self, headless=False):
        self.driver = None
        self.wait = None
        self.results = {}
        self.headless = headless
        self.progress_file = "scraping_progress.json"
        self.screenshot_dir = "screenshots"
        self.proxy_api_key = "wvm4z69kf54pc9rod7ck"
        self.proxy_base_url = "https://proxyscrape.com/v2/?request=get&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all"
        self.current_proxy = None
        self.rows_processed = 0
        self.session_change_threshold = 5
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
                    self.results = progress_data.get('results', {})
                    self.last_processed_index = progress_data.get('last_processed_index', -1)
                    print(f"Loaded progress: {len(self.results)} models already processed")
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
            print(f"Progress saved: {len(self.results)} models processed")
        except Exception as e:
            print(f"Error saving progress: {e}")
    
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
        """Search for products on Croma.com"""
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
            
        except Exception as e:
            print(f"Error searching Croma: {e}")
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
    
    def extract_product_links(self, max_products=5):
        """Extract product links and text from Croma search results"""
        products = []
        
        try:
            # Look for product links with the specified class pattern
            # Croma uses various class patterns for product links
            # link_selectors = [
            #     "a[class*='a-link-normal'][class*='s-line-clamp']",
            #     "a[class*='a-link-normal'][class*='s-line-style']",
            #     "a[class*='a-link-normal'][class*='a-text-normal']",
            #     "h2 a",
            #     ".s-result-item a[href*='/dp/']",
            #     ".s-result-item a[href*='/gp/product/']"
            # ]
            
            link_selectors = [
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
                            
                            # Store results grouped by model_id (overwrite if same model_id)
                            # This ensures we get the latest search results for each model
                            self.results[model_id] = {
                                'product_name': product_name,
                                'model_id': model_id,
                                'Croma_links': products
                            }
                            
                            print(f"Extracted {len(products)} Croma products for model {model_id}")
                        else:
                            print("No Croma products found")
                            # Take screenshot for no products found
                            self.take_screenshot("no_products", search_query, index)
                        
                        # Update progress and row counter
                        self.last_processed_index = index
                        self.rows_processed += 1
                        self.save_progress()
                        
                        # Random delay between searches
                        delay = random.uniform(3, 7)
                        print(f"Waiting {delay:.1f} seconds before next search...")
                        time.sleep(delay)
                        
                    else:
                        print("Failed to search Croma")
                        # Take screenshot for search failure
                        self.take_screenshot("search_failed", search_query, index)
                
                except Exception as e:
                    print(f"Error processing row {index + 1}: {e}")
                    # Take screenshot for processing error
                    self.take_screenshot("processing_error", search_query, index)
                    continue
                    
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Saving progress...")
            self.save_progress()
            print("Progress saved. You can resume later by running the script again.")
            raise
    
    def save_results(self, output_file="Croma_mobile_results.json"):
        """Save Croma results to JSON file"""
        
        if self.results:
            try:
                # Convert results to list format as requested
                results_list = list(self.results.values())
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(results_list, f, indent=2, ensure_ascii=False)
                
                print(f"Croma results saved to {output_file}")
                print(f"Total models processed: {len(self.results)}")
                
                # Also save as CSV for compatibility
                csv_file = output_file.replace('.json', '.csv')
                self.save_results_csv(csv_file)
                
            except Exception as e:
                print(f"Error saving Croma results: {e}")
        else:
            print("No Croma results to save")
    
    def save_results_csv(self, output_file="Croma_mobile_results.csv"):
        """Save results in CSV format for compatibility"""
        try:
            csv_data = []
            for model_data in self.results.values():
                model_id = model_data['model_id']
                product_name = model_data['product_name']
                
                for link in model_data['Croma_links']:
                    csv_data.append({
                        'model_id': model_id,
                        'product_name': product_name,
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
    parser = argparse.ArgumentParser(description='Croma Mobile Phone Scraper')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = CromaMobileScrapper(headless=args.headless)
    
    try:
        # CSV file path
        csv_file = "expanded_permutations.csv"
        
        # Check if file exists
        if not os.path.exists(csv_file):
            print(f"CSV file not found: {csv_file}")
            return
        
        # Start Croma scraping (process all entries)
        print("=== STARTING Croma SCRAPING ===")
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