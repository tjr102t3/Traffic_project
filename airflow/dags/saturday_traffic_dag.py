# 匯入時間日期函式庫
import pendulum
# 匯入DAG 類別
from airflow.models.dag import DAG
# 匯入 PythonOperator，讓 python 函式轉化成可以被 airflow 管理和執行的任務
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

# 從 tasks/crawler.py 匯入任務函式
from tasks.crawler import get_target_csv_info, scrape_and_process_data, load_to_bigquery

with DAG(
    dag_id="saturday_traffic_analysis",
    schedule_interval="*/5 * * * 6",
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
        op_args=[get_csv_info_task.output]
    )

    # 3: 將資料載入 BigQuery
    load_to_bigquery_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery,
        op_args=[scrape_and_process_task.output]
    )
    trigger_prediction_dag_task = TriggerDagRunOperator(
        task_id="trigger_prediction_dag",
        trigger_dag_id="traffic_prediction_dag",  
        conf={"message": "Data loaded, starting prediction!"}
    )
    # 定義任務順序
    get_csv_info_task >> scrape_and_process_task >> load_to_bigquery_task >> trigger_prediction_dag_task