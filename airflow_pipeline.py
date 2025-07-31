from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import boto3
from io import BytesIO
import pandas as pd
import zipfile
import requests
import json
import pymongo as py


def zipfile_download(url_zip=None,**kwargs):
    url_zip="URL_FOR_ZIP"
    ti = kwargs['ti']
    response = requests.get(url_zip)
    if response.status_code == 200:
        zip_file_like = BytesIO(response.content)     #--Read ZIP into memory
        ti.xcom_push(key='first_key',value=zip_file_like.getvalue())
    else:
        print(f"Failed to download ZIP. Status code: {response.status_code}")


def zipfile_extraction_push_s3(access_key=None,secret_key=None,**kwargs):
    access_key = "YOUR_ACCESS_KEY"
    secret_key = "YOUR_SECRET_KEY"
    ti = kwargs['ti']
    zip_bytes = ti.xcom_pull(task_ids='zipfile_download_task', key='first_key')
    zip_file_like = BytesIO(zip_bytes)
    # Step 3: Extract ZIP in memory
    with zipfile.ZipFile(zip_file_like) as zip_ref:
        json_files = [name for name in zip_ref.namelist() if name.endswith('.json')] #--getting filenames from zipfile

        # Step 4: Upload to S3
        s3_client = boto3.client("s3",aws_access_key_id = access_key,aws_secret_access_key = secret_key)
        bucket_name = 'your-bucket-name'
        s3_folder = 'your-folder-name/' 
        uploaded_files = [] 

        for file_name in json_files:
            with zip_ref.open(file_name) as f:
                s3_client.upload_fileobj(f, bucket_name, s3_folder + file_name)
                uploaded_files.append(s3_folder + file_name)
                print(f"Uploaded: {file_name} to s3://{bucket_name}/{s3_folder + file_name}")
        
    ti.xcom_push(key='second_key',value=uploaded_files)




def s3_object_jsondoc_making(**kwargs):
    access_key = "YOUR_ACCESS_KEY"
    secret_key = "YOUR_SECRET_KEY"
    bucket_name = 'your-bucket-name'
    ti = kwargs['ti']
    json_docs = []
    uploaded_files = ti.xcom_pull(task_ids='zipfile_extraction_push_s3_task', key='second_key')
    for files in uploaded_files:
        s3_client = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        v = s3_client.get_object(Bucket=bucket_name, Key=files)
        v_body = v["Body"]
        v_bytes = v_body.read()

        try:
            json_doc = json.loads(v_bytes)
            json_docs.append(json_doc)
        except json.JSONDecodeError:
            print(f"File {files} is empty or not valid JSON. Skipping.")
            continue

    ti.xcom_push(key='third_key',value=json_docs)

def df_dict_making(**kwargs):
    ti = kwargs['ti']
    json_docs= ti.xcom_pull(task_ids='s3_object_jsondoc_making_task', key='third_key')
    docs_mongo=[]
    for doc in json_docs:
        try:
            df=pd.json_normalize(doc)
            docs_mongo.append(df.to_dict(orient='records'))
        except Exception as e:
            print(f"Failed to normalize document: {e}")
    ti.xcom_push(key='fourth_key',value=docs_mongo)



def inserting_mongodb(**kwargs):
    ti = kwargs['ti']
    docs_mongo= ti.xcom_pull(task_ids='df_dict_making_task', key='fourth_key')
    con=py.MongoClient("mongodb+srv://karan:Karan2000@cluster0.jwd6v.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db=con['airflow']
    for i, record_set in enumerate(docs_mongo):
        if record_set:
            collection_name = f"collection_{i+1}"
            db[collection_name].insert_many(record_set)
            print(f"Inserted into {collection_name}: {len(record_set)} docs")

# Define default_args for the DAG
default_args = {
    'owner': 'karan',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
dag = DAG(
    'aiflow_data_migration',
    default_args=default_args,
    description='Dag for data migration pipeline',
    schedule_interval=timedelta(days=1),
)
# Task for downloading the zip file and push for extraction:

zipfile_download_task = PythonOperator(
    task_id='zipfile_download_task',
    python_callable=zipfile_download,
    op_kwargs={"url_zip":"URL_FOR_ZIP"},
    provide_context=True,                               # This makes sure the function receives the context including 'ti'
    dag=dag,
)

# Task for extraction of zipfile and uploading files to s3 bucket:

zipfile_extraction_push_s3_task = PythonOperator(
    task_id='zipfile_extraction_push_s3_task',
    python_callable=zipfile_extraction_push_s3,
    op_kwargs={"access_key":"YOUR_ACCESS_KEY","secret_key":"YOUR_SECRET_KEY"},
    provide_context=True,                               # This makes sure the function receives the context including 'ti'
    dag=dag,
)

# Task for getting the json docs from the file objects from s3 bucket:

s3_object_jsondoc_making_task = PythonOperator(
    task_id='s3_object_jsondoc_making_task',
    python_callable=s3_object_jsondoc_making,
    provide_context=True,
    dag=dag,
)

# Task for creating a df from the json docs and converting df to dict

df_dict_making_task= PythonOperator(
    task_id='df_dict_making_task',
    python_callable=df_dict_making,
    provide_context=True,
    dag=dag,
)

# Task for inserting into seperate collection in mongodb:

inserting_mongodb_task= PythonOperator(
    task_id='inserting_mongodb_task',
    python_callable=inserting_mongodb,
    provide_context=True,
    dag=dag,
)

zipfile_download_task >> zipfile_extraction_push_s3_task >> s3_object_jsondoc_making_task >> df_dict_making_task >> inserting_mongodb_task
