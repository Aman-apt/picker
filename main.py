

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import os
import sys
from concurrent.futures import ThreadPoolExecutor
import time

@dataclass
class Business:
    """holds business data"""
    name: str = None
    address: str = None
    website: str = None
    phone_number: str = None
    reviews_count: int = None
    reviews_average: float = None
    latitude: float = None
    longitude: float = None

@dataclass
class BusinessList:
    """holds list of Business objects and saves to Excel/CSV"""
    business_list: list[Business] = field(default_factory=list)
    save_at = 'output'

    def dataframe(self):
        """transform business_list to pandas dataframe"""
        return pd.json_normalize(
            (asdict(business) for business in self.business_list), sep="_"
        )

    def save_to_excel(self, filename):
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        self.dataframe().to_excel(f"{self.save_at}/{filename}.xlsx", index=False)

    def save_to_csv(self, filename):
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        self.dataframe().to_csv(f"{self.save_at}/{filename}.csv", index=False)

def extract_coordinates_from_url(url: str) -> tuple[float, float]:
    """helper function to extract coordinates from url"""
    try:
        coordinates = url.split('/@')[-1].split('/')[0]
        return float(coordinates.split(',')[0]), float(coordinates.split(',')[1])
    except:
        return None, None

def scrape_business_details(page, listing, name_attribute='aria-label'):
    """Optimized business details scraping with reduced waits"""
    try:
        listing.click()
        # Reduced wait time - page usually loads faster
        page.wait_for_timeout(2000)

        # XPath selectors
        address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
        website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
        phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
        review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
        reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'
        
        business = Business()
        
        # Get name from listing attribute
        business.name = listing.get_attribute(name_attribute) or ""
        
        # Use is_visible() with timeout for faster checks
        try:
            if page.locator(address_xpath).first.is_visible(timeout=500):
                business.address = page.locator(address_xpath).first.inner_text()
        except:
            business.address = ""
            
        try:
            if page.locator(website_xpath).first.is_visible(timeout=500):
                business.website = page.locator(website_xpath).first.inner_text()
        except:
            business.website = ""
            
        try:
            if page.locator(phone_number_xpath).first.is_visible(timeout=500):
                business.phone_number = page.locator(phone_number_xpath).first.inner_text()
        except:
            business.phone_number = ""
            
        try:
            if page.locator(review_count_xpath).first.is_visible(timeout=500):
                review_text = page.locator(review_count_xpath).first.inner_text()
                business.reviews_count = int(review_text.split()[0].replace(',', '').strip())
        except:
            business.reviews_count = None
            
        try:
            if page.locator(reviews_average_xpath).first.is_visible(timeout=500):
                avg_text = page.locator(reviews_average_xpath).first.get_attribute(name_attribute)
                business.reviews_average = float(avg_text.split()[0].replace(',', '.').strip())
        except:
            business.reviews_average = None
        
        # Extract coordinates
        business.latitude, business.longitude = extract_coordinates_from_url(page.url)
        
        return business
    except Exception as e:
        print(f'Error scraping business: {e}')
        return None

def scroll_and_load_listings(page, total):
    """Optimized scrolling with better detection"""
    print("Loading listings...")
    
    # Wait for initial listings to load
    page.wait_for_selector('//a[contains(@href, "https://www.google.com/maps/place")]', timeout=10000)
    
    previously_counted = 0
    same_count_iterations = 0
    
    while True:
        current_count = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').count()
        
        # Check if we've reached our target
        if current_count >= total:
            print(f"Reached target: {total} listings")
            break
        
        # Check if we're stuck at the same count
        if current_count == previously_counted:
            same_count_iterations += 1
            # If stuck for 2 iterations, we've likely reached the end
            if same_count_iterations >= 2:
                print(f"Reached end of available listings: {current_count}")
                break
        else:
            same_count_iterations = 0
            previously_counted = current_count
            print(f"Loaded: {current_count} listings")
        
        # Scroll down
        page.mouse.wheel(0, 10000)
        page.wait_for_timeout(2000)  # Reduced from 5000ms
    
    # Get all listings
    listings = page.locator('//a[contains(@href, "https://www.google.com/maps/place")]').all()[:total]
    listings = [listing.locator("xpath=..") for listing in listings]
    print(f"Total listings to scrape: {len(listings)}")
    
    return listings

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str, help="Search query")
    parser.add_argument("-t", "--total", type=int, help="Maximum number of results")
    parser.add_argument("-o", "--output", type=str, default="google_maps_results", help="Output filename")
    parser.add_argument("--headless", action="store_true", help="Run in headless mode")
    args = parser.parse_args()
    
    # Determine total
    total = args.total if args.total else 1_000_000
    
    # Get search queries
    search_list = []
    if args.search:
        search_list = [args.search]
    else:
        input_file_path = os.path.join(os.getcwd(), 'input.txt')
        if os.path.exists(input_file_path):
            with open(input_file_path, 'r') as file:
                search_list = [line.strip() for line in file.readlines() if line.strip()]
        
        if not search_list:
            print('Error: You must either pass the -s search argument, or add searches to input.txt')
            sys.exit()
    
    # Start scraping
    start_time = time.time()
    
    with sync_playwright() as p:
        # Launch browser with optimized settings
        browser = p.chromium.launch(
            headless=args.headless,
            args=['--disable-blink-features=AutomationControlled']
        )
        
        # Create context with optimized settings
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        # Set default timeout
        page.set_default_timeout(30000)
        
        page.goto("https://www.google.com/maps", timeout=60000)
        page.wait_for_timeout(3000)
        
        all_businesses = BusinessList()
        
        for search_index, search_query in enumerate(search_list):
            print(f"\n{'='*50}")
            print(f"Search {search_index + 1}/{len(search_list)}: {search_query}")
            print(f"{'='*50}")
            
            # Clear and enter search
            search_box = page.locator('//input[@id="searchboxinput"]')
            search_box.clear()
            search_box.fill(search_query)
            page.wait_for_timeout(1000)
            page.keyboard.press("Enter")
            page.wait_for_timeout(3000)
            
            # Scroll and load all listings
            listings = scroll_and_load_listings(page, total)
            
            # Scrape each listing
            print(f"\nScraping {len(listings)} businesses...")
            for idx, listing in enumerate(listings, 1):
                business = scrape_business_details(page, listing)
                if business:
                    all_businesses.business_list.append(business)
                
                if idx % 10 == 0:
                    print(f"Scraped: {idx}/{len(listings)}")
            
            print(f"Completed: {len(listings)} businesses scraped")
        
        browser.close()
    
    # Save results
    elapsed_time = time.time() - start_time
    print(f"\n{'='*50}")
    print(f"Scraping completed in {elapsed_time:.2f} seconds")
    print(f"Total businesses scraped: {len(all_businesses.business_list)}")
    
    output_filename = args.output
    all_businesses.save_to_excel(output_filename)
    all_businesses.save_to_csv(output_filename)
    print(f"Results saved to: output/{output_filename}.xlsx and .csv")

if __name__ == "__main__":
    main()