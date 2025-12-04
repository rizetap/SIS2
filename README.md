Tumblr Data Collection and Preparation Project
This project scrapes, cleans, and loads Tumblr blog post data into a SQLite database for analysis.
Project Structure
project/
│   README.md
│   requirements.txt
│   airflow_dag.py
├── src/
│   ├── scraper.py      # Web scraping module
│   ├── cleaner.py      # Data cleaning module
│   └── loader.py       # Database loading module
└── data/
    ├── tumblr_posts.csv
    ├── cleaned_tumblr_data.csv
    └── output.db
Features
1. Data Collection (scraper.py)

Uses Selenium for web scraping
Handles dynamic content loading
Extracts structured data including:

Post text content
Post type (photo, text, quote)
Notes count (engagement metrics)
Tags
Image URLs
Post URLs
Timestamps



2. Data Cleaning (cleaner.py)

Removes duplicate posts
Filters empty content
Handles missing values
Cleans and normalizes text
Adds derived features:

Word count
Character count
Engagement levels
Tag counts


Ensures minimum 100 records after cleaning

3. Data Loading (loader.py)

Creates normalized SQLite database
Tables:

posts: Main post data
tags: Unique tags
post_tags: Many-to-many relationship
summary_statistics: Aggregated metrics


Generates summary statistics
Verifies data integrity

Installation

Install dependencies:

bashpip install -r requirements.txt

Download ChromeDriver:


Visit: https://chromedriver.chromium.org/
Download version matching your Chrome browser
Add to system PATH

Usage
Run Individual Modules
Scrape data:
bashpython src/scraper.py
Clean data:
bashpython src/cleaner.py
Load to database:
bashpython src/loader.py
Run Complete Pipeline
bash# Option 1: Run each step manually
python src/scraper.py && python src/cleaner.py && python src/loader.py

# Option 2: Use Airflow DAG (see below)
Airflow Integration
The project includes an Airflow DAG for automated pipeline execution:

Start Airflow:

bashairflow standalone

Access Airflow UI at http://localhost:8080
Enable the tumblr_data_pipeline DAG
Trigger manually or wait for scheduled run

Database Schema
posts table

id: Primary key
post_url: Unique post URL
post_text: Original text
post_text_clean: Cleaned text
post_type: Type of post
notes_count: Engagement metric
word_count, char_count: Text metrics
has_image: Boolean flag
image_url: Image URL if present
tags_str: Comma-separated tags
engagement_level: Categorized engagement
timestamp, scrape_date: Time metadata

tags table

id: Primary key
tag_name: Unique tag name

post_tags table

post_id: Foreign key to posts
tag_id: Foreign key to tags

Configuration
Customize Scraping Targets
Edit src/scraper.py line 150:
pythonblogs_to_scrape = [
    'https://your-blog.tumblr.com/',
    'https://www.tumblr.com/tagged/your-topic',
]
Adjust Cleaning Parameters
Edit src/cleaner.py line 150:
pythoncleaner.filter_quality_posts(min_word_count=5, max_word_count=10000)
Output Files

data/raw_tumblr_data.csv: Raw scraped data
data/cleaned_tumblr_data.csv: Cleaned and processed data
data/output.db: SQLite database with normalized tables

Data Quality
The pipeline ensures:

Minimum 100 records after cleaning
No duplicate posts
No empty content
Proper data types
Normalized tag structure
Engagement categorization

Troubleshooting
ChromeDriver issues:

Ensure ChromeDriver version matches Chrome browser
Add ChromeDriver to system PATH
Try headless mode: TumblrScraper(headless=True)

Insufficient data:

Increase posts_per_blog in scraper
Add more blogs to blogs_to_scrape
Reduce min_word_count in cleaner

Database errors:

Ensure data/ directory exists
Check file permissions
Verify CSV file format

License
Educational project for data collection and preparation coursework.
