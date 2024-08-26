import os
import json
from prefect import task, flow
from prefect.client.schemas.schedules import IntervalSchedule, CronSchedule
from datetime import datetime, timedelta
import pandas as pd
from prefect.deployments import DeploymentImage
from google.cloud import bigquery, storage
from google.oauth2 import service_account

@task(cache_expiration=timedelta(hours=1))
def fetch_data(wrong_data=123):
    data = [
        {'name': 'Andy', 'age': '33', 'time': datetime.now(), 'random': wrong_data},
        {'name': 'Bob', 'age': '10', 'time': datetime.now(), 'wrong': wrong_data},
        ]
    
    return data

@task
def transform_data(data):
    needed_keys = ['name', 'age', 'time']

    filtered_data = []
    for d in data:
        filtered_dict = {} 
        for k, v in d.items():
            if k in needed_keys: 
                if v is not None and not isinstance(v, str):
                    v = str(v)
                filtered_dict[k] = v 
        filtered_data.append(filtered_dict) 
    return filtered_data

@task
def save_data(data):
    data = pd.DataFrame(data)

    # Path to your CSV file
    csv_file = 'the_data.csv'

    # Append new data to the CSV file
    csv_data = data.to_csv(index=False)

    # Save the CSV to Google Cloud Storage
    storage_client = storage.Client()
    bucket_name = 'my-prefect-bucket'  # Replace with your bucket name
    destination_blob_name = 'the_data.csv'  # The name of the file in GCS

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(csv_data, content_type='text/csv')

    print(f"File uploaded to {destination_blob_name} in {bucket_name} bucket.")


# Define the insert_record_one_by_one task
@task
def insert_record_one_by_one(client, table_id, records):
    failed_records = []
    
    for record in records:
        errors = client.insert_rows_json(table_id, [record])
        
        if errors:
            failed_records.append({'record': record, 'errors': errors})
        else:
            print(f"Record inserted successfully: {record}")
    
    return failed_records

# Define the load_data_to_bigquery task
@task
def load_data_to_bigquery(client, data):
    table_id = 'my-tenth-project-432516.test_dataset.names'
    failed_records = insert_record_one_by_one(client, table_id, data)
    
    if failed_records:
        with open('failed_records_report.txt', 'w') as f:
            for entry in failed_records:
                f.write(f"Record: {entry['record']}\n")
                f.write(f"Errors: {entry['errors']}\n")
                f.write("\n---\n\n")
        print("Report of failed records has been written to 'failed_records_report.txt'.")
    else:
        print("All records were inserted successfully.")


# Define the Prefect flow
@flow(name="ETL Flow - v2", log_prints=True)
def etl_flow_2(wrong_data=10):
    data = fetch_data(wrong_data)
    transformed_data = transform_data(data)

    client = bigquery.Client()
    load_data_to_bigquery(client, transformed_data)

    save_data(transformed_data)

    print('Complete the task!')

# Run the flow
if __name__ == "__main__":
    etl_flow_2.deploy(                                                            
        name="my-etl-deployment",
        work_pool_name="my-cloud-run-pool",
        # cron="*/2 * * * *",
        image=DeploymentImage(
            name="my-image:latest",
            platform="linux/amd64",
        )
    )