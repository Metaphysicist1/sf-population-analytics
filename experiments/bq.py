from google.cloud import bigquery
import pandas as pd
from google.api_core import exceptions
import logging
from google.oauth2 import service_account

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DATA_PATH = 'data/file_clean.csv'

def upload_csv_to_bigquery(csv_path, project_id, dataset_id, table_id):
    try:
        # Initialize BigQuery client
        credentials = service_account.Credentials.from_service_account_file("/home/metaphysicist/Schreibtisch/tokens/big-query-data-application-zoomcamp.json")

        client = bigquery.Client(credentials=credentials,project=project_id)
        
        # Define dataset and table references
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Configure the load job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,  # Auto-detect schema
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite table if exists
        )

        # Load CSV file
        logging.info(f"Uploading {csv_path} to {dataset_id}.{table_id}")
        with open(csv_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        # Wait for the job to complete
        job.result()
        logging.info(f"Loaded {job.output_rows} rows into {dataset_id}.{table_id}")

        # Query the table to verify
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 5"
        query_job = client.query(query)
        results = query_job.result()

        logging.info("Sample data from table:")
        for row in results:
            logging.info(row)

    except exceptions.GoogleAPIError as e:
        logging.error(f"BigQuery API error: {e}")
        raise
    except FileNotFoundError:
        logging.error(f"CSV file not found: {csv_path}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

def main():
    # Configuration
    credentials = service_account.Credentials.from_service_account_file("/home/metaphysicist/Schreibtisch/tokens/big-query-data-application-zoomcamp.json")

    csv_path = DATA_PATH  # Replace with your CSV path
    project_id = "big-query-data-application"      # Replace with your GCP project ID
    dataset_id = "Data_Engineer_Jobs_Table"      # Replace with your dataset ID
    table_id = "Data_Engineer_Jobs_Table"          # Replace with your table ID

    # Create dataset if it doesn't exist
    client = bigquery.Client(credentials=credentials,project=project_id)
    print(credentials)
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logging.info(f"Dataset {dataset_id} already exists")
    except exceptions.NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your preferred region
        client.create_dataset(dataset)
        logging.info(f"Created dataset {dataset_id}")

    # Upload CSV and query
    upload_csv_to_bigquery(csv_path, project_id, dataset_id, table_id)

if __name__ == "__main__":
    main()