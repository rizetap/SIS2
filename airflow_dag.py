"""
airflow_dag.py
Airflow DAG for orchestrating Tumblr data pipeline
"""

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

airflow_home = os.path.expanduser('~/airflow')
sys.path.insert(0, airflow_home)
sys.path.insert(0, os.path.join(airflow_home, 'dags'))
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

try:
    from src.scraper import TumblrScraper
    from src.cleaner import DataCleaner
    from src.loader import DataLoader
except ImportError:
    from scraper import TumblrScraper
    from cleaner import DataCleaner
    from loader import DataLoader


default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'tumblr_data_pipeline',
    default_args=default_args,
    description='ETL pipeline for Tumblr data collection',
    schedule='@weekly',
    catchup=False,
    tags=['tumblr', 'etl', 'web-scraping'],
)


def scrape_tumblr_data(**context):
    """Task 1: Scrape data from Tumblr"""
    print("Starting Tumblr scraping...")
    
    blogs_to_scrape = [
        'https://staff.tumblr.com/',
        'https://engineering.tumblr.com/',
        'https://www.tumblr.com/tagged/photography',
        'https://www.tumblr.com/tagged/art',
        'https://www.tumblr.com/tagged/coding',
        'https://www.tumblr.com/tagged/technology',
    ]
    scraper = TumblrScraper(headless=True)
    
    try:
        scraper.scrape_multiple_blogs(blogs_to_scrape, posts_per_blog=4)
        
        scraper.save_data('tumblr_posts', format='csv')
        
        output_file = 'data/tumblr_posts.csv'
        print(f"Scraping completed. Data saved to {output_file}")
        
        context['task_instance'].xcom_push(key='raw_data_file', value=output_file)
        
        return output_file
        
    except Exception as e:
        print(f"Error during scraping: {e}")
        raise
    
    finally:
        scraper.close()


def clean_tumblr_data(**context):
    """Task 2: Clean and process scraped data"""
    print("Starting data cleaning...")
    
    raw_data_file = context['task_instance'].xcom_pull(
        task_ids='scrape_data', 
        key='raw_data_file'
    )
    
    if not raw_data_file:
        raw_data_file = 'data/tumblr_posts.csv'
    
    cleaner = DataCleaner(input_file=raw_data_file)
    
    try:
        df = cleaner.clean_all(min_records=100)
        
        output_file = cleaner.save_cleaned_data('data/cleaned_tumblr_data.csv')
        
        print(f"Cleaning completed. Data saved to {output_file}")
        
        context['task_instance'].xcom_push(key='cleaned_data_file', value=output_file)
        
        return output_file
        
    except Exception as e:
        print(f"Error during cleaning: {e}")
        raise


def load_to_database(**context):
    """Task 3: Load cleaned data to SQLite database"""
    print("Starting database loading...")
    
    cleaned_data_file = context['task_instance'].xcom_pull(
        task_ids='clean_data',
        key='cleaned_data_file'
    )
    
    if not cleaned_data_file:
        cleaned_data_file = 'data/cleaned_tumblr_data.csv'
    
    loader = DataLoader(db_path='data/output.db')
    
    try:
        loader.load_all(csv_file=cleaned_data_file)
        
        print("Database loading completed successfully")
        
        return 'data/output.db'
        
    except Exception as e:
        print(f"Error during loading: {e}")
        raise
    
    finally:
        loader.close()


def validate_pipeline(**context):
    """Task 4: Validate the complete pipeline"""
    print("Validating pipeline execution...")
    
    import sqlite3
    
    db_path = 'data/output.db'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database not found at {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM posts")
        post_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tags")
        tag_count = cursor.fetchone()[0]
        
        print(f"Validation Results:")
        print(f"  - Posts in database: {post_count}")
        print(f"  - Unique tags: {tag_count}")
        
        if post_count < 100:
            raise ValueError(f"Insufficient records: {post_count} (minimum 100 required)")
        
        cursor.execute("SELECT COUNT(*) FROM posts WHERE word_count > 0")
        valid_posts = cursor.fetchone()[0]
        
        quality_ratio = valid_posts / post_count if post_count > 0 else 0
        print(f"  - Data quality ratio: {quality_ratio:.2%}")
        
        if quality_ratio < 0.8:
            print(f"WARNING: Low data quality ratio: {quality_ratio:.2%}")
        
        print("✓ Pipeline validation successful!")
        
    finally:
        conn.close()


#taskss
scrape_task = PythonOperator(
    task_id='scrape_data',
    python_callable=scrape_tumblr_data,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_tumblr_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_pipeline',
    python_callable=validate_pipeline,
    dag=dag,
)
scrape_task >> clean_task >> load_task >> validate_task