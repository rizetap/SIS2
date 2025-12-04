import pandas as pd
import sqlite3
import os
from datetime import datetime


class DataLoader:
    def __init__(self, db_path='data/output.db'):
        """Initialize the data loader"""
        self.db_path = db_path
        self.conn = None
        
    def connect(self):
        """Connect to SQLite database"""
        os.makedirs('data', exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        print(f"Connected to database: {self.db_path}")
        
    def create_tables(self):
        """Create database tables"""
        cursor = self.conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_url TEXT UNIQUE,
                post_text TEXT,
                post_text_clean TEXT,
                post_type TEXT,
                notes_count INTEGER,
                word_count INTEGER,
                char_count INTEGER,
                has_image INTEGER,
                image_url TEXT,
                tag_count INTEGER,
                tags_str TEXT,
                engagement_level TEXT,
                scrape_date DATE,
                timestamp TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tag_name TEXT UNIQUE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS post_tags (
                post_id INTEGER,
                tag_id INTEGER,
                FOREIGN KEY (post_id) REFERENCES posts(id),
                FOREIGN KEY (tag_id) REFERENCES tags(id),
                PRIMARY KEY (post_id, tag_id)
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS summary_statistics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                total_posts INTEGER,
                avg_word_count REAL,
                avg_notes REAL,
                posts_with_images INTEGER,
                most_common_type TEXT,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
        print("Database tables created successfully")
    
    def load_posts(self, csv_file='data/cleaned_tumblr_data.csv'):
        """Load posts from CSV into database"""
        print(f"\nLoading data from {csv_file}")
        df = pd.read_csv(csv_file)
        
        print(f"Loading {len(df)} posts into database...")
        
        posts_columns = [
            'post_url', 'post_text', 'post_text_clean', 'post_type',
            'notes_count', 'word_count', 'char_count', 'has_image',
            'image_url', 'tag_count', 'tags_str', 'engagement_level',
            'scrape_date', 'timestamp'
        ]
        
        for col in posts_columns:
            if col not in df.columns:
                df[col] = None
        
        posts_df = df[posts_columns]
        
        posts_df.to_sql('posts', self.conn, if_exists='replace', index=False)
        
        print(f"✓ Loaded {len(posts_df)} posts")
        
        return df
    
    def load_tags(self, df):
        """Extract and load tags into normalized tables"""
        print("\nLoading tags...")
        
        cursor = self.conn.cursor()
        cursor.execute("DELETE FROM post_tags")
        cursor.execute("DELETE FROM tags")
        
        tag_id_map = {}
        tag_counter = 1
        
        for idx, row in df.iterrows():
            if pd.notna(row['tags']) and row['tags'] != '[]':
                try:
                    if isinstance(row['tags'], str):
                        if row['tags'].startswith('['):
                            tags = eval(row['tags'])
                        else:
                            tags = [t.strip() for t in row['tags'].split(',')]
                    else:
                        tags = []
                    
                    cursor.execute("SELECT id FROM posts WHERE post_url = ?", (row['post_url'],))
                    result = cursor.fetchone()
                    if not result:
                        continue
                    post_id = result[0]
                    for tag in tags:
                        if tag and tag.strip():
                            tag = tag.strip().lower()
                            
                            if tag not in tag_id_map:
                                cursor.execute("INSERT OR IGNORE INTO tags (tag_name) VALUES (?)", (tag,))
                                cursor.execute("SELECT id FROM tags WHERE tag_name = ?", (tag,))
                                tag_id = cursor.fetchone()[0]
                                tag_id_map[tag] = tag_id
                            else:
                                tag_id = tag_id_map[tag]
                            
                            cursor.execute(
                                "INSERT OR IGNORE INTO post_tags (post_id, tag_id) VALUES (?, ?)",
                                (post_id, tag_id)
                            )
                
                except Exception as e:
                    print(f"Error processing tags for post {idx}: {e}")
                    continue
        
        self.conn.commit()
        
        cursor.execute("SELECT COUNT(*) FROM tags")
        tag_count = cursor.fetchone()[0]
        print(f"✓ Loaded {tag_count} unique tags")
    
    def generate_summary_statistics(self):
        """Generate and store summary statistics"""
        print("\nGenerating summary statistics...")
        
        cursor = self.conn.cursor()
        
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        total_posts = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(word_count) FROM posts")
        avg_word_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(notes_count) FROM posts")
        avg_notes = cursor.fetchone()[0]
        
        cursor.execute("SELECT SUM(has_image) FROM posts")
        posts_with_images = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT post_type, COUNT(*) as count 
            FROM posts 
            GROUP BY post_type 
            ORDER BY count DESC 
            LIMIT 1
        """)
        result = cursor.fetchone()
        most_common_type = result[0] if result else 'unknown'
        
        
        cursor.execute("""
            INSERT INTO summary_statistics 
            (total_posts, avg_word_count, avg_notes, posts_with_images, most_common_type)
            VALUES (?, ?, ?, ?, ?)
        """, (total_posts, avg_word_count, avg_notes, posts_with_images, most_common_type))
        
        self.conn.commit()
        print("✓ Summary statistics generated")
    
    def verify_data(self):
        """Verify loaded data"""
        print("\n=== Database Verification ===")
        
        cursor = self.conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM posts")
        posts_count = cursor.fetchone()[0]
        print(f"Posts: {posts_count}")
        
        cursor.execute("SELECT COUNT(*) FROM tags")
        tags_count = cursor.fetchone()[0]
        print(f"Unique tags: {tags_count}")
        
        cursor.execute("SELECT COUNT(*) FROM post_tags")
        post_tags_count = cursor.fetchone()[0]
        print(f"Post-tag relationships: {post_tags_count}")
        
        print("\nSample posts:")
        cursor.execute("""
            SELECT post_type, word_count, notes_count, engagement_level 
            FROM posts 
            LIMIT 5
        """)
        for row in cursor.fetchall():
            print(f"  Type: {row[0]}, Words: {row[1]}, Notes: {row[2]}, Engagement: {row[3]}")
        
        print("\nTop 10 tags:")
        cursor.execute("""
            SELECT t.tag_name, COUNT(*) as usage_count
            FROM tags t
            JOIN post_tags pt ON t.id = pt.tag_id
            GROUP BY t.tag_name
            ORDER BY usage_count DESC
            LIMIT 10
        """)
        for row in cursor.fetchall():
            print(f"  {row[0]}: {row[1]} posts")
    
    def load_all(self, csv_file='data/cleaned_tumblr_data.csv'):
        """Execute all loading steps"""
        print("\n=== Starting Data Loading Process ===")
        
        self.connect()
        self.create_tables()
        
        df = self.load_posts(csv_file)
        self.load_tags(df)
        self.generate_summary_statistics()
        
        self.verify_data()
        
        print(f"\n✓ Data successfully loaded to {self.db_path}")
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            print("\nDatabase connection closed")


def main():
    """Main execution function"""
    loader = DataLoader(db_path='data/output.db')
    
    try:
        loader.load_all(csv_file='data/cleaned_tumblr_data.csv')
    except Exception as e:
        print(f"Error during loading: {e}")
    finally:
        loader.close()


if __name__ == "__main__":
    main()