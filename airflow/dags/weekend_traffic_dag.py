from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from tasks.crawler import scrape_and_filter_data, save_data

with DAG(
    dag_id="weekend_traffic_analysis",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2025, 8, 8, tz="Asia/Taipei"),
    catchup=False,
    tags=["traffic", "crawler", "weekend"],
) as dag:
    
    # 執行爬蟲任務
    # scrape_and_filter_data 會回傳一個 Tuple (df, filename)
    processed_data = scrape_and_filter_data()
    
    # 將爬蟲任務的輸出作為下一個任務的輸入
    save_data(processed_data)