from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.options import Options
import pandas as pd
import time
import json
from datetime import datetime
import os

class TumblrScraper:
    def __init__(self, headless=True):
        """Initialize the scraper with Chrome driver"""
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        self.data = []
        
    def scroll_page(self, scrolls=3):
        """Scroll the page to load dynamic content"""
        for i in range(scrolls):
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            print(f"Scroll {i+1}/{scrolls} completed")
    
    def extract_post_data(self, post_element):
        """Extract data from a single post element"""
        try:
            post_data = {
                'timestamp': datetime.now().isoformat(),
                'post_text': '',
                'post_type': '',
                'notes_count': 0,
                'tags': [],
                'image_url': '',
                'post_url': ''
            }
            
            
            try:
                text_elem = post_element.find_element(By.CSS_SELECTOR, "div[class*='post-body'], div[class*='caption']")
                post_data['post_text'] = text_elem.text.strip()
            except NoSuchElementException:
                pass
            
            try:
                if post_element.find_elements(By.CSS_SELECTOR, "img, figure"):
                    post_data['post_type'] = 'photo'
                elif post_element.find_elements(By.CSS_SELECTOR, "blockquote"):
                    post_data['post_type'] = 'quote'
                else:
                    post_data['post_type'] = 'text'
            except:
                post_data['post_type'] = 'unknown'
            
            try:
                notes_elem = post_element.find_element(By.CSS_SELECTOR, "a[class*='note-count'], span[class*='note']")
                notes_text = notes_elem.text
                notes_num = ''.join(filter(str.isdigit, notes_text))
                post_data['notes_count'] = int(notes_num) if notes_num else 0
            except:
                pass
            
            try:
                tag_elems = post_element.find_elements(By.CSS_SELECTOR, "a[class*='tag'], .tags a")
                post_data['tags'] = [tag.text.strip('#') for tag in tag_elems if tag.text]
            except:
                pass
            
            try:
                img_elem = post_element.find_element(By.CSS_SELECTOR, "img")
                post_data['image_url'] = img_elem.get_attribute('src')
            except:
                pass
            
            try:
                link_elem = post_element.find_element(By.CSS_SELECTOR, "a[class*='permalink'], a.timestamp")
                post_data['post_url'] = link_elem.get_attribute('href')
            except:
                pass
            
            return post_data
            
        except Exception as e:
            print(f"Error extracting post data: {e}")
            return None
    
    def scrape_blog(self, blog_url, num_scrolls=5):
        """Scrape posts from a Tumblr blog"""
        try:
            print(f"Navigating to {blog_url}")
            self.driver.get(blog_url)
            time.sleep(3)
            
            self.scroll_page(num_scrolls)
            
            print("Extracting posts...")
            post_elements = self.driver.find_elements(By.CSS_SELECTOR, "article, div[class*='post'], .post")
            print(f"Found {len(post_elements)} post elements")
            
            for idx, post in enumerate(post_elements):
                print(f"Processing post {idx+1}/{len(post_elements)}")
                post_data = self.extract_post_data(post)
                if post_data:  
                    self.data.append(post_data)
            
            print(f"Successfully extracted {len(self.data)} posts from {blog_url}")
            
        except Exception as e:
            print(f"Error scraping blog {blog_url}: {e}")
    
    def scrape_multiple_blogs(self, blog_urls, posts_per_blog=5):
        """Scrape multiple Tumblr blogs"""
        for blog_url in blog_urls:
            self.scrape_blog(blog_url, num_scrolls=posts_per_blog)
            time.sleep(3)  #delay betwen logs
    
    def save_data(self, filename='tumblr_data', format='both'):
        """Save the collected data to the data folder"""
        os.makedirs('data', exist_ok=True)
        df = pd.DataFrame(self.data)
        
        if df.empty:
            print("No data to save")
            return
        
        if format in ['csv', 'both']:
            df.to_csv(f'data/{filename}.csv', index=False)
            print(f"Data saved to data/{filename}.csv")
        
        if format in ['json', 'both']:
            df.to_json(f'data/{filename}.json', orient='records', indent=2)
            print(f"Data saved to data/{filename}.json")
    
    def close(self):
        """Close the browser"""
        self.driver.quit()
        print("Browser closed")


def main():
    """Main execution function"""
    
    blogs_to_scrape = [
        'https://staff.tumblr.com/',
        'https://engineering.tumblr.com/',
        'https://www.tumblr.com/tagged/photography',
        'https://www.tumblr.com/tagged/art',
        'https://www.tumblr.com/tagged/coding',
    ]
    
    print("Initializing Tumblr scraper...")
    scraper = TumblrScraper(headless=False)
    
    try:
        scraper.scrape_multiple_blogs(blogs_to_scrape, posts_per_blog=3)
        scraper.save_data('tumblr_posts', format='both')
    except Exception as e:
        print(f"Error during scraping: {e}")
    finally:
        scraper.close()


if __name__ == "__main__":
    main()
