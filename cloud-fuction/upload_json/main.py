# import functions_framework
import io
import json
import os

import requests
from google.cloud import bigquery, storage


# @functions_framework.http
def download_and_upload_json(request):

    # file 水情
    url = 'https://www.taiwanstat.com/waters/latest'

    # 指定 Cloud Storage 的 bucket 名稱
    
    bucket_name = os.getenv("bucket_name")

    # 下載檔案
    response = requests.get(url)
    if response.status_code != 200:
        return f'Failed to download CSV: {response.content}', 500

    # 從 URL 中獲取檔案名稱
    file_name = f"workflow/水情動態"

    # 建立 Google Cloud Storage 客戶端
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    tmp = json.loads(response.text[1:-1])

    with io.StringIO() as memory_file:
        for reservoir, values in tmp.items():
            jsonl = json.dumps(values, ensure_ascii=False)
            memory_file.write(jsonl+"\n")

        memory_file.seek(0)
        content = memory_file.read()

        # # 建立 blob 並上傳檔案
        blob = bucket.blob(file_name)
        blob.upload_from_string(content, content_type='text/json')

    print(f'Successfully uploaded {file_name} to {bucket_name}')

    gcs_uri = f"gs://{bucket_name}/{file_name}"
    import_json(gcs_uri)


    return f'Successfully import {file_name} to BigQuery'

def import_json(gcs_uri):

    table_id = os.getenv("table_id")
    location = os.getenv("location")

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    # table_id = "your-project.your_dataset.your_table_name

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("updateAt", "STRING"),
            bigquery.SchemaField("volumn", "STRING"),
            bigquery.SchemaField("percentage", "STRING"),
            bigquery.SchemaField("daliyOverflow", "STRING"),
            bigquery.SchemaField("daliyInflow", "STRING"),
            bigquery.SchemaField("daliyNetflow", "STRING"),
            bigquery.SchemaField("baseAvailable", "STRING"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # body = io.BytesIO(b"Washington,WA")
    # client.load_table_from_file(body, table_id, job_config=job_config).result()
    # previous_rows = client.get_table(table_id).num_rows
    # assert previous_rows > 0

    # job_config = bigquery.LoadJobConfig(
    #     write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    #     source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    # )


    load_job = client.load_table_from_uri(
        gcs_uri,
        table_id,
        location=location,  # Must match the destination dataset location.
        job_config=job_config,  
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))