import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

# --- Setup logging ---
logging.basicConfig(
    filename='etl_pipeline.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# --- Database connection info ---
DB_USER = 'your_username'
DB_PASS = 'your_password'
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'your_database'

# Create SQLAlchemy engine for PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

# --- Extraction function ---
def extract_api_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"Extracted data from {url}")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Failed to extract from {url}: {e}")
        return None

# --- Transformation function ---
def transform_data(users_json, posts_json):
    try:
        users_df = pd.json_normalize(users_json)
        posts_df = pd.json_normalize(posts_json)

        # Select & rename columns
        users_df = users_df[['id', 'name', 'username', 'email']]
        users_df.rename(columns={'id': 'user_id'}, inplace=True)

        posts_df = posts_df[['userId', 'id', 'title', 'body']]
        posts_df.rename(columns={'userId': 'user_id', 'id': 'post_id'}, inplace=True)

        # Merge posts with user info
        combined_df = posts_df.merge(users_df, on='user_id', how='left')

        logging.info("Transformation successful")
        return combined_df

    except Exception as e:
        logging.error(f"Transformation error: {e}")
        return None

# --- Load function ---
def load_data(df, table_name):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        logging.info(f"Loaded data into table {table_name}")
    except Exception as e:
        logging.error(f"Loading error: {e}")

# --- Main ETL orchestration ---
def run_etl():
    users_url = "https://jsonplaceholder.typicode.com/users"
    posts_url = "https://jsonplaceholder.typicode.com/posts"

    users_data = extract_api_data(users_url)
    posts_data = extract_api_data(posts_url)

    if not users_data or not posts_data:
        logging.error("Extraction failed. Aborting ETL.")
        return

    df = transform_data(users_data, posts_data)
    if df is None:
        logging.error("Transformation failed. Aborting ETL.")
        return

    load_data(df, "user_posts")

if __name__ == "__main__":
    logging.info("ETL pipeline started")
    run_etl()
    logging.info("ETL pipeline finished")
