82% of storage used … If you run out, you can't create, edit, and upload files. Get 30 GB of storage for ₹59 ₹0 for 1 month.
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import boto3
from io import BytesIO
import pandas as pd
import zipfile
import requests
import json
import pymongo
import tempfile
import os

# -----------------------------
# Functions
# -----------------------------

def zipfile_download(**kwargs):
    """Download ZIP from SEC and push to XCom as bytes"""
    ti = kwargs['ti']
    url_zip = kwargs.get("url_zip")

    print(f"Downloading ZIP from: {url_zip}")
    response = requests.get(url_zip, stream=True)
    if response.status_code == 200:
        zip_content = BytesIO(response.content)
        ti.xcom_push(key='zip_content', value=zip_content.getvalue())
        print("ZIP downloaded successfully and stored in XCom.")
    else:
        raise Exception(f"Failed to download ZIP. Status: {response.status_code}")


def zipfile_extraction_push_s3(**kwargs):
    """Extract first 3 JSON files from ZIP and upload to S3"""
    ti = kwargs['ti']
    zip_bytes = ti.xcom_pull(task_ids='zipfile_download_task', key='zip_content')

    aws_conn = BaseHook.get_connection("aws_conn")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "us-east-1")
    )

    bucket_name = kwargs.get("bucket_name")
    s3_folder = kwargs.get("s3_folder", "raw/")
    uploaded_files = []

    zip_file_like = BytesIO(zip_bytes)
    with zipfile.ZipFile(zip_file_like) as zip_ref:
        json_files = [f for f in zip_ref.namelist() if f.endswith(".json")]
        print(f"Found {len(json_files)} JSON files in ZIP. Uploading first 3 only...")

        for file_name in json_files[:3]:  # Limit to 3 files only
            with zip_ref.open(file_name) as f:
                s3_key = f"{s3_folder}{os.path.basename(file_name)}"
                s3_client.upload_fileobj(f, bucket_name, s3_key)
                uploaded_files.append(s3_key)
                print(f"Uploaded to S3: s3://{bucket_name}/{s3_key}")

    ti.xcom_push(key='s3_keys', value=uploaded_files)
    print("Uploaded 3 JSON files to S3 successfully.")


def s3_object_jsondoc_making(**kwargs):
    """Read JSONs from S3 and load them into memory"""
    ti = kwargs['ti']
    uploaded_files = ti.xcom_pull(task_ids='zipfile_extraction_push_s3_task', key='s3_keys')

    aws_conn = BaseHook.get_connection("aws_conn")
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_conn.login,
        aws_secret_access_key=aws_conn.password,
        region_name=aws_conn.extra_dejson.get("region_name", "us-east-1")
    )

    bucket_name = kwargs.get("bucket_name")
    json_docs = []

    for s3_key in uploaded_files:
        print(f"Reading {s3_key} from S3...")
        obj = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        try:
            data = obj['Body'].read()
            json_doc = json.loads(data)
            json_docs.append(json_doc)
            print(f"Loaded {s3_key} as JSON.")
        except json.JSONDecodeError:
            print(f"Skipping invalid JSON: {s3_key}")

    ti.xcom_push(key='json_docs', value=json_docs)
    print("JSON documents ready for transformation.")


def df_dict_making(**kwargs):
    """Convert JSONs into flattened dictionaries for MongoDB"""
    ti = kwargs['ti']
    json_docs = ti.xcom_pull(task_ids='s3_object_jsondoc_making_task', key='json_docs')

    docs_mongo = []
    for doc in json_docs:
        try:
            df = pd.json_normalize(doc)
            docs_mongo.append(df.to_dict(orient='records'))
        except Exception as e:
            print(f"Failed to normalize JSON: {e}")

    ti.xcom_push(key='docs_mongo', value=docs_mongo)
    print("DataFrames converted to dictionaries for MongoDB.")


def inserting_mongodb(**kwargs):
    """Insert records into MongoDB Atlas"""
    ti = kwargs['ti']
    docs_mongo = ti.xcom_pull(task_ids='df_dict_making_task', key='docs_mongo')

    mongo_conn = BaseHook.get_connection("mongo_conn")
    client = pymongo.MongoClient(mongo_conn.get_uri())
    db = client['airflow']

    for i, records in enumerate(docs_mongo):
        if records:
            collection_name = f"collection_{i+1}"
            db[collection_name].insert_many(records)
            print(f"Inserted {len(records)} documents into {collection_name}")
        else:
            print(f"No records found for collection_{i+1}")

    client.close()
    print("MongoDB insertion complete.")


# -----------------------------
# DAG Definition
# -----------------------------

default_args = {
    'owner': 'karan',
    'start_date': datetime(2025, 9, 9),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'airflow_data_migration_lite',
    default_args=default_args,
    description='ETL DAG: SEC ZIP (3 JSONs only) → S3 → MongoDB',
    schedule_interval=None,
    catchup=False,
)

# -----------------------------
# Tasks
# -----------------------------

zipfile_download_task = PythonOperator(
    task_id='zipfile_download_task',
    python_callable=zipfile_download,
    op_kwargs={"url_zip": "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"},
    dag=dag,
)

zipfile_extraction_push_s3_task = PythonOperator(
    task_id='zipfile_extraction_push_s3_task',
    python_callable=zipfile_extraction_push_s3,
    op_kwargs={"bucket_name": "data-migration-karan-bucket", "s3_folder": "json_files/"},
    dag=dag,
)

s3_object_jsondoc_making_task = PythonOperator(
    task_id='s3_object_jsondoc_making_task',
    python_callable=s3_object_jsondoc_making,
    op_kwargs={"bucket_name": "data-migration-karan-bucket"},
    dag=dag,
)

df_dict_making_task = PythonOperator(
    task_id='df_dict_making_task',
    python_callable=df_dict_making,
    dag=dag,
)

inserting_mongodb_task = PythonOperator(
    task_id='inserting_mongodb_task',
    python_callable=inserting_mongodb,
    dag=dag,
)

# -----------------------------
# DAG Dependencies
# -----------------------------

zipfile_download_task >> zipfile_extraction_push_s3_task >> s3_object_jsondoc_making_task >> df_dict_making_task >> inserting_mongodb_task
