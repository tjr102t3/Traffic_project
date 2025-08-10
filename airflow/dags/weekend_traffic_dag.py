from __future__ import annotations
import pendulum
from airflow.models.dag import DAG

# 從 tasks/crawler.py 匯入你需要的任務函式
from tasks.crawler import scrape_and_filter_data, load_to_bigquery

with DAG(
    dag_id="weekend_traffic_analysis",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 8, 8, tz="Asia/Taipei"),
    catchup=False,
    tags=["traffic", "crawler", "weekend"],
) as dag:
    
    # 執行爬蟲任務，它的回傳值會被傳遞給下一個任務
    scraped_data = scrape_and_filter_data()
    
    # 將爬蟲任務的輸出作為輸入，上傳到 BigQuery
    load_to_bigquery(scraped_data)