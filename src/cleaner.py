import pandas as pd
import re
import os
from datetime import datetime
import numpy as np


class DataCleaner:
    def __init__(self, input_file='data/tumblr_posts.csv'):
        """Initialize the data cleaner"""
        self.input_file = input_file
        self.df = None
        
    def load_data(self):
        """Load raw data from CSV"""
        print(f"Loading data from {self.input_file}")
        self.df = pd.read_csv(self.input_file)
        print(f"Loaded {len(self.df)} records")
        return self.df
    
    def remove_duplicates(self):
        """Remove duplicate posts based on post_url"""
        initial_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=['post_url'], keep='first')
        removed = initial_count - len(self.df)
        print(f"Removed {removed} duplicate records")
        
    def remove_empty_posts(self):
        """Remove posts with no text content"""
        initial_count = len(self.df)
        self.df = self.df[self.df['post_text'].notna()]
        self.df = self.df[self.df['post_text'].str.len() > 0]
        removed = initial_count - len(self.df)
        print(f"Removed {removed} empty posts")
    
    def clean_text(self):
        """Clean and normalize text content"""
        print("Cleaning text content...")
        
        #removing white spacess
        self.df['post_text'] = self.df['post_text'].str.strip()
        self.df['post_text'] = self.df['post_text'].str.replace(r'\s+', ' ', regex=True)
        
        self.df['post_text_clean'] = self.df['post_text'].apply(
            lambda x: re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', str(x))
        )
        
        self.df['post_text_clean'] = self.df['post_text_clean'].apply(
            lambda x: re.sub(r'[^\w\s.,!?;:\'-]', '', str(x))
        )
        
        self.df['post_text_clean'] = self.df['post_text_clean'].str.strip()
        self.df['post_text_clean'] = self.df['post_text_clean'].str.replace(r'\s+', ' ', regex=True)
        
    def handle_missing_values(self):
        """Handle missing values in the dataset"""
        print("Handling missing values...")
        self.df['notes_count'] = self.df['notes_count'].fillna(0).astype(int)
        
        self.df['post_type'] = self.df['post_type'].fillna('unknown')
        
        self.df['image_url'] = self.df['image_url'].fillna('')
        
        self.df['tags'] = self.df['tags'].fillna('[]')
        
    def add_derived_features(self):
        """Add new features derived from existing data"""
        print("Adding derived features...")
        
        self.df['word_count'] = self.df['post_text'].apply(lambda x: len(str(x).split()))
        
        self.df['char_count'] = self.df['post_text'].apply(lambda x: len(str(x)))
        
        self.df['has_image'] = self.df['image_url'].apply(lambda x: 1 if len(str(x)) > 0 else 0)
        
        def count_tags(tags_str):
            try:
                if pd.isna(tags_str) or tags_str == '[]':
                    return 0
                if isinstance(tags_str, str) and tags_str.startswith('['):
                    tags = eval(tags_str)
                    return len(tags) if isinstance(tags, list) else 0
                return 0
            except:
                return 0
        
        self.df['tag_count'] = self.df['tags'].apply(count_tags)
        
        def tags_to_string(tags_str):
            try:
                if pd.isna(tags_str) or tags_str == '[]':
                    return ''
                if isinstance(tags_str, str) and tags_str.startswith('['):
                    tags = eval(tags_str)
                    if isinstance(tags, list):
                        return ', '.join(str(tag) for tag in tags)
                return ''
            except:
                return ''
        
        self.df['tags_str'] = self.df['tags'].apply(tags_to_string)
        
        def categorize_engagement(notes):
            if notes == 0:
                return 'no_engagement'
            elif notes < 10:
                return 'low'
            elif notes < 100:
                return 'medium'
            else:
                return 'high'
        
        self.df['engagement_level'] = self.df['notes_count'].apply(categorize_engagement)
        
        self.df['scrape_date'] = pd.to_datetime(self.df['timestamp']).dt.date
        
    def filter_quality_posts(self, min_word_count=5, max_word_count=10000):
        """Filter posts based on quality criteria"""
        initial_count = len(self.df)
        
        self.df = self.df[
            (self.df['word_count'] >= min_word_count) & 
            (self.df['word_count'] <= max_word_count)
        ]
        
        removed = initial_count - len(self.df)
        print(f"Removed {removed} posts outside word count range ({min_word_count}-{max_word_count})")
    
    def standardize_post_types(self):
        """Standardize post type categories"""
        print("Standardizing post types...")
        
        self.df['post_type'] = self.df['post_type'].str.lower()
        
        type_mapping = {
            'image': 'photo',
            'picture': 'photo',
            'pic': 'photo',
            'txt': 'text',
            'quotation': 'quote'
        }
        
        self.df['post_type'] = self.df['post_type'].replace(type_mapping)
    
    def clean_all(self, min_records=100):
        """Execute all cleaning steps"""
        print("\n=== Starting Data Cleaning Process ===\n")
        
        self.load_data()
        
        print("\nStep 1: Removing duplicates")
        self.remove_duplicates()
        
        print("\nStep 2: Removing empty posts")
        self.remove_empty_posts()
        
        print("\nStep 3: Handling missing values")
        self.handle_missing_values()
        
        print("\nStep 4: Cleaning text")
        self.clean_text()
        
        print("\nStep 5: Standardizing post types")
        self.standardize_post_types()
        
        print("\nStep 6: Adding derived features")
        self.add_derived_features()
        
        print("\nStep 7: Filtering quality posts")
        self.filter_quality_posts()
        
        if len(self.df) < min_records:
            print(f"\nWARNING: Only {len(self.df)} records after cleaning. Minimum required: {min_records}")
        else:
            print(f"\n✓ Successfully cleaned data: {len(self.df)} records (minimum {min_records} required)")
        
        return self.df
    
    def save_cleaned_data(self, output_file='data/cleaned_tumblr_data.csv'):
        """Save cleaned data to CSV"""
        os.makedirs('data', exist_ok=True)
        
        self.df.to_csv(output_file, index=False)
        print(f"\nCleaned data saved to {output_file}")
        
        self.print_summary()
        
        return output_file
    
    def print_summary(self):
        """Print summary statistics of cleaned data"""
        print("\n=== Data Summary ===")
        print(f"Total records: {len(self.df)}")
        print(f"\nPost types distribution:")
        print(self.df['post_type'].value_counts())
        print(f"\nEngagement levels:")
        print(self.df['engagement_level'].value_counts())
        print(f"\nStatistics:")
        print(f"  Average word count: {self.df['word_count'].mean():.2f}")
        print(f"  Average notes: {self.df['notes_count'].mean():.2f}")
        print(f"  Posts with images: {self.df['has_image'].sum()}")
        print(f"  Average tags per post: {self.df['tag_count'].mean():.2f}")


def main():
    """Main execution function"""
    cleaner = DataCleaner(input_file='data/tumblr_posts.csv')
    cleaner.clean_all(min_records=100)
    cleaner.save_cleaned_data(output_file='data/cleaned_tumblr_data.csv')


if __name__ == "__main__":
    main()