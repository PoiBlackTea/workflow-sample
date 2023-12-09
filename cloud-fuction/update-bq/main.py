import os

import functions_framework
from google.cloud import bigquery


@functions_framework.http
def execute_bigquery_sql(request):

    src_table_id = os.getenv("src_table_id")
    dst_table_id = os.getenv("dst_table_id")
    # 建立 BigQuery 客戶端
    client = bigquery.Client()

    # 設定您的 BigQuery 查詢
    query = f"""
    MERGE INTO `{dst_table_id}`
    USING ( SELECT `{src_table_id}`.id AS join_key,
        `{src_table_id}`.*
      FROM `{src_table_id}`
      UNION ALL
      SELECT  NULL,
        `{src_table_id}`.*,
        FROM `{src_table_id}`
        JOIN `{dst_table_id}`
        ON `{src_table_id}`.id = `{dst_table_id}`.id
        WHERE (`{src_table_id}`.percentage <> `{dst_table_id}`.percentage
          AND `{dst_table_id}`.end_date IS NULL )) sub
    ON sub.join_key = `{dst_table_id}`.id
    WHEN MATCHED AND sub.percentage <> `{dst_table_id}`.percentage THEN UPDATE
    SET end_date = CURRENT_DATE("UTC+8")
    WHEN NOT MATCHED THEN INSERT
    ( name,
      id,
      updateAt,
      volumn,
      percentage,
      daliyOverflow,
      daliyInflow,
      daliyNetflow,
      baseAvailable,
      modified_date,
      end_date
    )
    VALUES
    (
      sub.name,
      sub.id,
      sub.updateAt,
      sub.volumn,
      sub.percentage,
      sub.daliyOverflow,
      sub.daliyInflow,
      sub.daliyNetflow,
      baseAvailable,
      CURRENT_DATE(),
      NULL
    )
    """

    # 執行查詢
    query_job = client.query(query)

    # 輸出結果
    for row in query_job.result():
        print("name={}, age={}".format(row.name, row.age))

    return f'Successfully update BigQuery'