import warnings
# Suppress deprecation warnings from Airflow providers (temporary until providers are updated)
warnings.filterwarnings("ignore", category=DeprecationWarning, module="airflow.providers.*")

from airflow.sdk import DAG, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json


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
        ## Make direct API call using requests
        api_key = "2Hg6r3LFmtaAq6dp49VaThYK38P2k0wQz0QXotPN"
        url = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"
        response = requests.get(url)
        response.raise_for_status()  ## Raise an exception for bad status codes

        return response.json()


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