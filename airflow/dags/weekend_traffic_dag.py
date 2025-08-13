# 匯入時間日期函式庫
import pendulum
# 匯入DAG 類別
from airflow.models.dag import DAG
# 匯入 PythonOperator，讓 python 函式轉化成可以被 airflow 管理和執行的任務
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 從 tasks/crawler.py 匯入任務函式
from airflow.tasks.crawler_ver1 import get_target_csv_info, scrape_and_process_data, load_to_bigquery

with DAG(
    dag_id="weekend_traffic_analysis",
    schedule=[
        "*/5 23 * * 5", # 每週五 23:00 開始，每 5 分鐘執行一次
        "*/5 * * * 6",  # 每週六全天，每 5 分鐘執行一次
        "*/5 * * * 0",  # 每週日全天，每 5 分鐘執行一次
        "0-30/5 0 * * 1"    # 每週一 00:00 到 00:30，每 5 分鐘執行一次
    ],
    start_date=pendulum.datetime(2025, 8, 8, tz="Asia/Taipei"),
    catchup=False,
    tags=["traffic", "crawler", "weekend"],
) as dag:

    # 任務 1: 取得目標 CSV 檔案資訊
    get_csv_info_task = PythonOperator(
        task_id="get_csv_info",
        python_callable=get_target_csv_info,
        do_xcom_push=True,
    )

    # 任務 2: 爬取與處理資料
    scrape_and_process_task = PythonOperator(
        task_id="scrape_and_process",
        python_callable=scrape_and_process_data,
        do_xcom_push=True,
        # 這裡從上一個任務的 XCom 拉取資料
        op_args=[get_csv_info_task.output]
    )

    # 任務 3: 將資料載入 BigQuery
    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        # 這裡從上一個任務的 XCom 拉取資料
        op_args=[scrape_and_process_task.output]
    )

    # 定義任務順序
    get_csv_info_task >> scrape_and_process_task >> load_to_bigquery_task