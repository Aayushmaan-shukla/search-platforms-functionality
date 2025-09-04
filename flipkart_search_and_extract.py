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
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException, WebDriverException
import pandas as pd
import os
from datetime import datetime
import requests
import logging
import gc
import psutil
import threading
import os
import signal
from contextlib import contextmanager

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
        self.session_change_threshold = 3  # Reduced from 5 to 3 for better stability
        self.backup_threshold = 100  # Create backup every 100 rows
        self.error_retry_count = 0
        self.max_retries = 5  # Increased to 5 retries as requested
        self.max_connection_retries = 3  # Specific retries for connection errors
        self.open_files_count = 0
        self.max_open_files = 50  # Limit to prevent "too many open files" error
        
        # Thread management for Docker
        self.thread_cleanup_threshold = 100  # Cleanup threads every 100 scrapes
        self.initial_thread_count = threading.active_count()
        self.thread_cleanup_count = 0
        self.is_docker_env = self.detect_docker_environment()
        
        # Setup logging
        self.setup_logging()
        
        # Create screenshots directory
        os.makedirs(self.screenshot_dir, exist_ok=True)
        
        self.setup_driver()
        self.load_progress()
    
    def detect_docker_environment(self):
        """Detect if running inside Docker container"""
        try:
            # Check for Docker-specific files and environment variables
            docker_indicators = [
                os.path.exists('/.dockerenv'),
                os.path.exists('/proc/1/cgroup') and 'docker' in open('/proc/1/cgroup').read(),
                os.environ.get('DOCKER_CONTAINER') == 'true',
                os.environ.get('container') == 'docker'
            ]
            return any(docker_indicators)
        except Exception:
            return False
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('flipkart_scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Docker environment detected: {self.is_docker_env}")
        self.logger.info(f"Initial thread count: {self.initial_thread_count}")
    
    @contextmanager
    def managed_file_handle(self, file_path, mode='r'):
        """Context manager to ensure file handles are properly closed"""
        file_handle = None
        try:
            file_handle = open(file_path, mode, encoding='utf-8')
            self.open_files_count += 1
            yield file_handle
        finally:
            if file_handle:
                file_handle.close()
                self.open_files_count -= 1
    
    def check_system_resources(self):
        """Check system resources and clean up if needed"""
        try:
            # Check open file descriptors
            process = psutil.Process()
            open_files = len(process.open_files())
            
            if open_files > self.max_open_files:
                self.logger.warning(f"Too many open files detected: {open_files}. Cleaning up...")
                gc.collect()  # Force garbage collection
                time.sleep(1)
                
            # Check memory usage
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                self.logger.warning(f"High memory usage: {memory_percent}%. Cleaning up...")
                gc.collect()
                time.sleep(1)
            
            # Check thread count (especially important in Docker)
            current_thread_count = threading.active_count()
            if self.is_docker_env and current_thread_count > self.initial_thread_count + 20:
                self.logger.warning(f"High thread count detected: {current_thread_count}. Initial: {self.initial_thread_count}")
                
        except Exception as e:
            self.logger.error(f"Error checking system resources: {e}")
    
    def cleanup_threads(self):
        """Clean up threads to prevent exhaustion in Docker environment"""
        if not self.is_docker_env:
            return  # Only cleanup in Docker environment
            
        try:
            current_thread_count = threading.active_count()
            self.logger.info(f"Thread cleanup triggered. Current threads: {current_thread_count}, Initial: {self.initial_thread_count}")
            
            # Get list of all active threads for monitoring
            active_threads = threading.enumerate()
            scraper_threads = [t for t in active_threads if 'scraper' in t.name.lower() or 'chrome' in t.name.lower()]
            
            self.logger.info(f"Active scraper-related threads: {len(scraper_threads)}")
            
            # Force garbage collection to clean up any dead threads
            gc.collect()
            
            # Wait a bit for threads to finish naturally
            time.sleep(3)
            
            # Check if we can reduce thread count
            new_thread_count = threading.active_count()
            if new_thread_count < current_thread_count:
                self.logger.info(f"Thread cleanup successful. Reduced from {current_thread_count} to {new_thread_count}")
            else:
                self.logger.warning(f"Thread cleanup had no effect. Still {new_thread_count} threads")
                
                # If thread count is still high, try more aggressive cleanup
                if new_thread_count > self.initial_thread_count + 15:
                    self.logger.warning("High thread count persists. Attempting aggressive cleanup...")
                    self.aggressive_thread_cleanup()
            
            # Reset thread cleanup counter
            self.thread_cleanup_count = 0
            
        except Exception as e:
            self.logger.error(f"Error during thread cleanup: {e}")
    
    def aggressive_thread_cleanup(self):
        """More aggressive thread cleanup for persistent high thread counts"""
        try:
            self.logger.info("Performing aggressive thread cleanup...")
            
            # Force multiple garbage collections
            for _ in range(3):
                gc.collect()
                time.sleep(1)
            
            # Log final thread count
            final_thread_count = threading.active_count()
            self.logger.info(f"Aggressive cleanup completed. Final thread count: {final_thread_count}")
            
        except Exception as e:
            self.logger.error(f"Error during aggressive thread cleanup: {e}")
    
    def should_cleanup_threads(self):
        """Check if thread cleanup should be performed"""
        return (self.is_docker_env and 
                self.thread_cleanup_count >= self.thread_cleanup_threshold)
    
    def check_chrome_health(self):
        """Check if Chrome driver is responsive"""
        try:
            if not self.driver:
                return False
            
            # Try a simple operation to check if Chrome is responsive
            self.driver.current_url
            return True
        except Exception as e:
            self.logger.warning(f"Chrome health check failed: {e}")
            return False
    
    def force_chrome_restart(self):
        """Force restart Chrome when it becomes unresponsive"""
        try:
            self.logger.warning("Forcing Chrome restart due to unresponsiveness...")
            
            # Kill any existing Chrome processes
            if self.driver:
                try:
                    self.driver.quit()
                except:
                    pass
            
            # Wait for processes to clean up
            time.sleep(5)
            
            # Force garbage collection
            gc.collect()
            
            # Kill any remaining Chrome processes (Docker-specific)
            if self.is_docker_env:
                try:
                    import subprocess
                    subprocess.run(['pkill', '-f', 'chrome'], check=False)
                    subprocess.run(['pkill', '-f', 'chromedriver'], check=False)
                    time.sleep(2)
                except:
                    pass
            
            # Create new driver
            self.setup_driver(use_proxy=True)
            
            self.logger.info("Chrome restart completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to restart Chrome: {e}")
            return False
    
    def monitor_chrome_processes(self):
        """Monitor Chrome processes and clean up if needed"""
        if not self.is_docker_env:
            return
            
        try:
            import subprocess
            result = subprocess.run(['pgrep', '-f', 'chrome'], capture_output=True, text=True)
            chrome_processes = len(result.stdout.strip().split('\n')) if result.stdout.strip() else 0
            
            if chrome_processes > 10:  # Too many Chrome processes
                self.logger.warning(f"Too many Chrome processes detected: {chrome_processes}")
                subprocess.run(['pkill', '-f', 'chrome'], check=False)
                time.sleep(2)
                gc.collect()
                
        except Exception as e:
            self.logger.error(f"Error monitoring Chrome processes: {e}")
    
    def safe_driver_quit(self):
        """Safely quit driver with proper error handling and thread cleanup"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                self.wait = None
                self.logger.info("Driver closed successfully")
        except Exception as e:
            self.logger.error(f"Error closing driver: {e}")
        finally:
            # Force cleanup including threads
            gc.collect()
            time.sleep(1)
            
            # If in Docker, also cleanup threads
            if self.is_docker_env:
                self.cleanup_threads()
        
    def get_proxy(self):
        """Get a new proxy from proxyscrape with retry logic"""
        for attempt in range(self.max_connection_retries):
            try:
                url = f"{self.proxy_base_url}&apikey={self.proxy_api_key}"
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    proxy = response.text.strip()
                    if proxy and ':' in proxy:
                        self.logger.info(f"New proxy obtained: {proxy[:20]}...")
                        return proxy
                    else:
                        self.logger.warning("Invalid proxy format received")
                else:
                    self.logger.warning(f"Proxy API returned status code: {response.status_code}")
            except requests.exceptions.Timeout:
                self.logger.warning(f"Timeout getting proxy from API (attempt {attempt + 1}/{self.max_connection_retries})")
            except requests.exceptions.ConnectionError:
                self.logger.warning(f"Connection error getting proxy from API (attempt {attempt + 1}/{self.max_connection_retries})")
            except Exception as e:
                self.logger.error(f"Error getting proxy (attempt {attempt + 1}/{self.max_connection_retries}): {e}")
            
            if attempt < self.max_connection_retries - 1:
                time.sleep(2)  # Wait before retry
        
        self.logger.info("Falling back to no proxy after all retries failed")
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
        """Setup undetected Chrome driver with proxy support"""
        try:
            if self.driver:
                self.driver.quit()
            
            # Try to get the correct Chrome version first
            chrome_major_version = self.get_chrome_version_major()
            self.logger.info(f"Detected Chrome major version: {chrome_major_version}")
            
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
            chrome_options.add_argument("--disable-web-security")
            chrome_options.add_argument("--allow-running-insecure-content")
            chrome_options.add_argument("--disable-features=VizDisplayCompositor")
            
            # Random user agent matching detected Chrome version
            if chrome_major_version:
                user_agent = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_major_version}.0.0.0 Safari/537.36"
            else:
                user_agents = [
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36"
                ]
                user_agent = random.choice(user_agents)
            
            chrome_options.add_argument(f"--user-agent={user_agent}")
            
            # Create undetected Chrome driver with version matching
            try:
                if chrome_major_version:
                    self.driver = uc.Chrome(options=chrome_options, version_main=chrome_major_version)
                else:
                    self.driver = uc.Chrome(options=chrome_options)
            except Exception as version_error:
                self.logger.warning(f"Failed to create driver with version {chrome_major_version}: {version_error}")
                # Try without version specification
                self.driver = uc.Chrome(options=chrome_options)
                
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            
            print("Chrome driver setup completed successfully")
            
        except Exception as e:
            print(f"Error setting up Chrome driver: {e}")
            # Fallback to regular Chrome if undetected fails
            self.logger.warning("Falling back to regular Chrome driver...")
            self.setup_regular_chrome(use_proxy)
    
    def setup_regular_chrome(self, use_proxy=True):
        """Fallback to regular Chrome driver"""
        try:
            chrome_options = Options()
            
            # Essential Chrome options
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--max_old_space_size=4096")  # Limit memory usage
            
            if self.headless:
                print("Configuring headless mode...")
                # Multiple headless arguments for maximum compatibility
                chrome_options.add_argument("--headless")
                chrome_options.add_argument("--headless=new")  # For newer Chrome versions
                chrome_options.add_argument("--disable-gpu")
                chrome_options.add_argument("--window-size=1920,1080")
                chrome_options.add_argument("--no-first-run")
                chrome_options.add_argument("--disable-default-apps")
                chrome_options.add_argument("--disable-popup-blocking")
                chrome_options.add_argument("--disable-background-timer-throttling")
                chrome_options.add_argument("--disable-backgrounding-occluded-windows")
                chrome_options.add_argument("--disable-renderer-backgrounding")
                chrome_options.add_argument("--disable-background-networking")
                chrome_options.add_argument("--disable-component-update")
                chrome_options.add_argument("--disable-client-side-phishing-detection")
                chrome_options.add_argument("--disable-sync")
                chrome_options.add_argument("--disable-translate")
                chrome_options.add_argument("--hide-scrollbars")
                chrome_options.add_argument("--mute-audio")
                chrome_options.add_argument("--disable-ipc-flooding-protection")
                chrome_options.add_argument("--disable-hang-monitor")
                chrome_options.add_argument("--disable-prompt-on-repost")
                chrome_options.add_argument("--disable-domain-reliability")
                chrome_options.add_argument("--disable-features=TranslateUI")
                chrome_options.add_argument("--disable-features=BlinkGenPropertyTrees")
                
                # Set headless property explicitly
                try:
                    chrome_options.headless = True
                except Exception:
                    pass
            
            if use_proxy and self.current_proxy:
                chrome_options.add_argument(f'--proxy-server={self.current_proxy}')
                print(f"Using proxy: {self.current_proxy[:20]}...")
            
            # Random user agent
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
            ]
            chrome_options.add_argument(f"--user-agent={random.choice(user_agents)}")
            
            # Additional stealth options
            chrome_options.add_argument("--disable-extensions")
            chrome_options.add_argument("--disable-plugins")
            chrome_options.add_argument("--disable-images")
            chrome_options.add_argument("--disable-javascript")
            
            # Create Chrome driver
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Anti-detection scripts
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.wait = WebDriverWait(self.driver, 15)
            
            print("Regular Chrome driver setup completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error setting up regular Chrome driver: {e}")
            raise
    
    def change_session(self, change_proxy=False):
        """Change Chrome session every 5 rows, optionally change proxy on errors"""
        try:
            self.logger.info("Changing Chrome session...")
            
            # Only get new proxy if explicitly requested (on errors)
            if change_proxy:
                new_proxy = self.get_proxy()
                if new_proxy:
                    self.current_proxy = new_proxy
                    self.logger.info(f"Switched to new proxy: {new_proxy[:20]}...")
            
            # Setup new driver (will automatically use headless if configured)
            self.setup_driver(use_proxy=True)
            
            # Reset row counter
            self.rows_processed = 0
            
            print("Chrome session changed successfully")
            
        except Exception as e:
            self.logger.error(f"Error changing Chrome session: {e}")
            # Try without proxy if proxy fails
            try:
                self.logger.info("Trying without proxy...")
                self.current_proxy = None
                self.setup_driver(use_proxy=False)
                self.rows_processed = 0
                print("Chrome session changed without proxy")
            except Exception as e2:
                self.logger.error(f"Failed to change session: {e2}")
                raise
    
    def load_progress(self):
        """Load progress from previous scraping session with proper file handling"""
        try:
            if os.path.exists(self.progress_file):
                with self.managed_file_handle(self.progress_file, 'r') as f:
                    progress_data = json.load(f)
                    self.results = progress_data.get('results', [])
                    self.last_processed_index = progress_data.get('last_processed_index', -1)
                    self.logger.info(f"Loaded progress: {len(self.results)} entries already processed")
                    self.logger.info(f"Resuming from index: {self.last_processed_index + 1}")
                return True
        except Exception as e:
            self.logger.error(f"Error loading progress: {e}")
        
        self.last_processed_index = -1
        return False
    
    def save_progress(self):
        """Save current progress to file with proper error handling"""
        try:
            progress_data = {
                'results': self.results,
                'last_processed_index': self.last_processed_index,
                'timestamp': datetime.now().isoformat()
            }
            with self.managed_file_handle(self.progress_file, 'w') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Progress saved: {len(self.results)} entries processed")
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")
            # Try to save to backup file if main file fails
            try:
                backup_file = f"{self.progress_file}.backup"
                with self.managed_file_handle(backup_file, 'w') as f:
                    json.dump(progress_data, f, indent=2, ensure_ascii=False)
                self.logger.info(f"Progress saved to backup file: {backup_file}")
            except Exception as backup_error:
                self.logger.error(f"Failed to save backup progress: {backup_error}")
    
    def create_backup(self):
        """Create backup file and delete previous backup with proper file handling"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"flipkart_mobile_results_backup_{timestamp}.json"
            
            # Save current results to backup
            with self.managed_file_handle(backup_file, 'w') as f:
                json.dump(self.results, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Backup created: {backup_file}")
            
            # Find and delete previous backup files
            backup_files = [f for f in os.listdir('.') if f.startswith('flipkart_mobile_results_backup_') and f.endswith('.json')]
            backup_files.sort()
            
            # Keep only the latest backup, delete others
            if len(backup_files) > 1:
                for old_backup in backup_files[:-1]:
                    try:
                        os.remove(old_backup)
                        self.logger.info(f"Deleted old backup: {old_backup}")
                    except Exception as e:
                        self.logger.error(f"Error deleting old backup {old_backup}: {e}")
            
            return backup_file
            
        except Exception as e:
            self.logger.error(f"Error creating backup: {e}")
            return None
    
    def handle_error_with_backup(self, error_msg, search_query, index):
        """Handle errors by creating backup and managing retries with comprehensive error handling"""
        self.logger.error(f"Error occurred: {error_msg}")
        
        # Check if it's a critical error that requires immediate attention
        critical_errors = ['max retries reached', 'too many open files', 'connection error', 'http connection pool', 'timeout', 'network error']
        is_critical = any(error_type in error_msg.lower() for error_type in critical_errors)
        
        if is_critical:
            self.logger.warning(f"Critical error detected: {error_msg}")
            # Force cleanup on critical errors
            self.check_system_resources()
            gc.collect()
        
        # Create backup on error
        backup_file = self.create_backup()
        if backup_file:
            self.logger.info(f"Backup created due to error: {backup_file}")
        
        # Take screenshot for debugging
        self.take_screenshot("error_occurred", search_query, index)
        
        # Increment error retry count
        self.error_retry_count += 1
        
        if self.error_retry_count >= self.max_retries:
            self.logger.error(f"Max retries ({self.max_retries}) reached. Skipping this entry.")
            self.error_retry_count = 0  # Reset for next entry
            return False
        else:
            self.logger.info(f"Retry attempt {self.error_retry_count}/{self.max_retries}")
            
            # For critical errors, change session and proxy
            if is_critical:
                try:
                    self.change_session(change_proxy=True)
                except Exception as session_error:
                    self.logger.error(f"Failed to change session on critical error: {session_error}")
                    # If session change fails, try to continue with current session
                    pass
            
            return True
    
    def take_screenshot(self, reason, search_query, index):
        """Take screenshot for debugging purposes with error handling"""
        try:
            # Check Chrome health before taking screenshot
            if not self.check_chrome_health():
                self.logger.warning("Chrome is unresponsive, skipping screenshot")
                return None
                
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.screenshot_dir}/{reason}_{index}_{timestamp}.png"
            self.driver.save_screenshot(filename)
            print(f"Screenshot saved: {filename}")
            return filename
        except Exception as e:
            self.logger.error(f"Error taking screenshot: {e}")
            # If screenshot fails due to timeout, try to restart Chrome
            if 'timeout' in str(e).lower() or 'connection' in str(e).lower():
                self.logger.warning("Screenshot failed due to timeout, attempting Chrome restart...")
                self.force_chrome_restart()
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
                    # Check if we need to change session (every 3 rows) - WITHOUT changing proxy
                    if self.rows_processed >= self.session_change_threshold:
                        self.logger.info(f"\n--- Changing Chrome session after {self.rows_processed} rows (keeping same proxy) ---")
                        self.change_session(change_proxy=False)  # Don't change proxy, just new session
                    
                    # Monitor Chrome processes in Docker
                    if self.is_docker_env and self.rows_processed % 10 == 0:
                        self.monitor_chrome_processes()
                    
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
                        self.thread_cleanup_count += 1  # Increment thread cleanup counter
                        self.save_progress()
                        
                        # Create backup every 100 rows
                        if self.rows_processed % self.backup_threshold == 0:
                            self.logger.info(f"\n--- Creating backup after {self.rows_processed} rows ---")
                            self.create_backup()
                        
                        # Thread cleanup every 100 scrapes (Docker-specific)
                        if self.should_cleanup_threads():
                            self.logger.info(f"\n--- Thread cleanup after {self.thread_cleanup_count} scrapes ---")
                            self.cleanup_threads()
                        
                        # Random delay between searches
                        delay = random.uniform(3, 7)
                        self.logger.info(f"Waiting {delay:.1f} seconds before next search...")
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
                            self.thread_cleanup_count += 1  # Increment thread cleanup counter
                            self.save_progress()
                            continue
                
                except Exception as e:
                    error_msg = f"Error processing row {index + 1}: {e}"
                    self.logger.error(error_msg)
                    
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
                                'flipkart_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.thread_cleanup_count += 1  # Increment thread cleanup counter
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
                                'flipkart_links': []
                            }
                            self.results.append(entry)
                            self.last_processed_index = index
                            self.rows_processed += 1
                            self.thread_cleanup_count += 1  # Increment thread cleanup counter
                            self.save_progress()
                            continue
                    
        except KeyboardInterrupt:
            self.logger.info("\nScraping interrupted by user. Saving progress...")
            self.save_progress()
            self.logger.info("Progress saved. You can resume later by running the script again.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error in scraping process: {e}")
            self.save_progress()
            raise
    
    def save_results(self, output_file="flipkart_mobile_results.json"):
        """Save Flipkart results to JSON file with proper error handling"""
        
        if self.results:
            try:
                # Results are already in list format
                with self.managed_file_handle(output_file, 'w') as f:
                    json.dump(self.results, f, indent=2, ensure_ascii=False)
                
                self.logger.info(f"Flipkart results saved to {output_file}")
                self.logger.info(f"Total entries processed: {len(self.results)}")
                
                # Also save as CSV for compatibility
                csv_file = output_file.replace('.json', '.csv')
                self.save_results_csv(csv_file)
                
            except Exception as e:
                self.logger.error(f"Error saving Flipkart results: {e}")
                # Try to save to backup file
                try:
                    backup_file = f"{output_file}.backup"
                    with self.managed_file_handle(backup_file, 'w') as f:
                        json.dump(self.results, f, indent=2, ensure_ascii=False)
                    self.logger.info(f"Results saved to backup file: {backup_file}")
                except Exception as backup_error:
                    self.logger.error(f"Failed to save backup results: {backup_error}")
        else:
            self.logger.info("No Flipkart results to save")
    
    def save_results_csv(self, output_file="flipkart_mobile_results.csv"):
        """Save results in CSV format for compatibility with proper error handling"""
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
                with self.managed_file_handle(output_file, 'w') as f:
                    df.to_csv(f, index=False, encoding='utf-8')
                self.logger.info(f"CSV results also saved to {output_file}")
        
        except Exception as e:
            self.logger.error(f"Error saving CSV results: {e}")
    
    def close(self):
        """Close the browser and cleanup resources"""
        try:
            self.safe_driver_quit()
            
            # Final thread cleanup if in Docker
            if self.is_docker_env:
                self.logger.info("Performing final thread cleanup...")
                self.cleanup_threads()
            
            # Final cleanup
            gc.collect()
            
            # Log final thread count
            final_thread_count = threading.active_count()
            self.logger.info(f"Scraper closed successfully. Final thread count: {final_thread_count} (Initial: {self.initial_thread_count})")
            
        except Exception as e:
            self.logger.error(f"Error closing scraper: {e}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Flipkart Mobile Phone Scraper')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--log-level', default='INFO', help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    args = parser.parse_args()
    
    # Initialize scraper
    scraper = FlipkartMobileScraper(headless=args.headless)
    
    try:
        # CSV file path
        csv_file = "flipkart.csv"
        
        # Check if file exists
        if not os.path.exists(csv_file):
            scraper.logger.error(f"CSV file not found: {csv_file}")
            return
        
        # Start Flipkart scraping (process all entries)
        scraper.logger.info("=== STARTING FLIPKART SCRAPING ===")
        scraper.logger.info(f"Headless mode: {'Enabled' if args.headless else 'Disabled'}")
        scraper.logger.info(f"Max retries: {scraper.max_retries}")
        scraper.logger.info(f"Max connection retries: {scraper.max_connection_retries}")
        scraper.logger.info(f"Docker environment: {scraper.is_docker_env}")
        scraper.logger.info(f"Thread cleanup threshold: {scraper.thread_cleanup_threshold} scrapes")
        scraper.logger.info(f"Initial thread count: {scraper.initial_thread_count}")
        
        scraper.scrape_permutations(csv_file)
        
        # Save results
        scraper.save_results()
        
    except KeyboardInterrupt:
        scraper.logger.info("\nScraping interrupted by user")
    except Exception as e:
        scraper.logger.error(f"Unexpected error: {e}")
    finally:
        scraper.close()

if __name__ == "__main__":
    main()