import warnings
# Suppress deprecation warnings from Airflow providers (temporary until providers are updated)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="airflow.providers.*")

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import time
import logging


## Define the DAG
with DAG(
    dag_id = "nasa_apod_postgres",
    start_date = datetime(2025, 1, 1),
    schedule = '@daily',
    catchup = False,
) as dag:

    ## Step 1: Create the table if doesnt exist
    @task
    def create_table():
        ## Initialize the Postgres Hook
        postgreshook = PostgresHook(postgres_conn_id ="my_postgres_conn")

        ## SQL Query to create the table
        create_table_query = """
            CREATE TABLE IF NOT EXISTS apod_data (
                id SERIAL PRIMARY KEY,
                title VARCHAR(255),
                explanation TEXT,
                url TEXT,
                date DATE,
                media_type VARCHAR(50)
            );
        """

        ## Execute the table creation query
        postgreshook.run(create_table_query)

    ## Step 2: Extract the NASA API Data (APOD) - Astronomy Picture of the Day [Extract Pipeline]
    ## https://api.nasa.gov/planetary/apod?api_key=2Hg6r3LFmtaAq6dp49VaThYK38P2k0wQz0QXotPN

    @task
    def extract_apod():
        ## Make direct API call using requests with retry logic and timeout
        api_key = "2Hg6r3LFmtaAq6dp49VaThYK38P2k0wQz0QXotPN"
        url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"

        max_retries = 3
        timeout_seconds = 30

        for attempt in range(max_retries):
            try:
                logging.info(f"Attempting NASA APOD API call (attempt {attempt + 1}/{max_retries})")
                response = requests.get(url, timeout=timeout_seconds)
                response.raise_for_status()  ## Raise an exception for bad status codes
                logging.info("Successfully retrieved NASA APOD data")
                return response.json()

            except requests.exceptions.Timeout:
                logging.warning(f"Request timed out (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5  # Exponential backoff: 5s, 10s, 20s
                    logging.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"NASA APOD API request timed out after {max_retries} attempts")

            except requests.exceptions.HTTPError as e:
                if response.status_code == 504:  # Gateway Timeout
                    logging.warning(f"Gateway timeout (attempt {attempt + 1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) * 10  # Longer backoff for 504 errors: 10s, 20s, 40s
                        logging.info(f"Waiting {wait_time} seconds before retry due to gateway timeout...")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"NASA APOD API returned 504 Gateway Timeout after {max_retries} attempts")
                else:
                    logging.error(f"HTTP error occurred: {e}")
                    raise e

            except requests.exceptions.RequestException as e:
                logging.error(f"Request exception occurred: {e}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 5
                    logging.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    raise e


    ## Step 3: Transform the data [Pick the information I need to save]
    @task
    def transform_apod_data(response):
        apod_data = {
            "title": response.get("title", ""),
            "explanation": response.get("explanation", ""),
            "url": response.get("url", ""),
            "date": response.get("date", ""),
            "media_type": response.get("media_type", "")
        }

        return apod_data

    ## Step 4: Load the data into the Postgres SQL
    @task
    def load_apod_data_postgres(apod_data):
        ## Initialize the Postgres Hook 
        postgreshook = PostgresHook(postgres_conn_id = "my_postgres_conn")

        ## Define SQL Insert Query
        insert_query = """
            INSERT INTO apod_data (title, explanation, url, date, media_type)
            VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the insert query
        postgreshook.run(insert_query, parameters = (
            apod_data["title"], 
            apod_data["explanation"], 
            apod_data["url"], 
            apod_data["date"], 
            apod_data["media_type"]
            )
        )

    ## Step 5: Verify the data DBViewer

    ## Extract
    api_response = extract_apod()  ## Call the extract function

    ## Transform
    transformed_data = transform_apod_data(api_response)
    
    ## Load
    load_apod_data_postgres(transformed_data)


    ## Step 6: Define the task Dependencies
    create_table() >> api_response  ## Ensure the table is created before extracting the data