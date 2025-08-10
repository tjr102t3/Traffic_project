import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from io import StringIO
import re
from airflow.decorators import task, dag
from google.cloud import bigquery
from pandas_gbq import to_gbq
import pendulum

# === Airflow DAG 設定 ===
@dag(
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Taipei"),
    schedule_interval="0 */1 * * *",  # 每小時執行一次
    catchup=False,
    tags=["freeway_traffic"]
)
def freeway_traffic_dag():

    # === 參數設定 ===
    BASE_URL = "https://tisvcloud.freeway.gov.tw/history/TDCS/M05A/"
    COLUMNS = ["TimeStamp", "GantryFrom", "GantryTo", "VehicleType", "Speed", "Volume"]
    TARGET_IDS = {"05F0287N", "05F0055N", "05F0001N"}

    # === BigQuery 設定 ===
    PROJECT_ID = "test123-467809"  # <--- 請替換成您的 GCP 專案 ID
    DATASET_ID = "bigquery"      # <--- 請替換成您的 Dataset ID
    TABLE_ID = "traffic-data"          # <--- 請替換成您在 BigQuery 建立的 Table ID

    # === 工具: 抓取 HTML 連結 ===
    def get_links_by_suffix(url, suffix):
        try:
            res = requests.get(url)
            if res.status_code != 200:
                print(f"❌ 連線失敗: {url} ({res.status_code})")
                return []
            soup = BeautifulSoup(res.text, "html.parser")
            return [
                urljoin(url, a["href"])
                for td in soup.find_all("td", class_="indexcolname")
                for a in td.find_all("a")
                if a["href"].endswith(suffix)
            ]
        except Exception as e:
            print(f"⚠️ 抓取連結錯誤: {e}")
            return []

    # === 🔁 尋找最近 7 天內的週六/週日的資料夾 ===
    def find_latest_weekend_folder():
        today = datetime.now()
        for i in range(7):
            target_date = today - timedelta(days=i)
            if target_date.weekday() in [5, 6]:
                date_str = target_date.strftime("%Y%m%d")
                test_url = urljoin(BASE_URL, date_str + "/")
                print(f"🔍 嘗試尋找週末資料：{date_str}")
                if get_links_by_suffix(test_url, "/"):
                    return date_str
        return None

    # === 找最新的CSV檔案並解析日期 ===
    def get_latest_csv_link():
        date_str = find_latest_weekend_folder()
        if not date_str:
            print("❌ 找不到近 7 日的週末資料")
            return None, None

        date_obj = datetime.strptime(date_str, "%Y%m%d")

        if date_obj.weekday() == 6:
            target_date = date_obj + timedelta(days=1)
            target_hour = "00"
            print(f"✅ 找到星期日，準備尋找禮拜一 {target_hour} 點的資料")
        else:
            target_date = date_obj
            target_hour = "23"
            print(f"✅ 找到星期六，準備尋找當日 {target_hour} 點的資料")

        target_date_str = target_date.strftime("%Y%m%d")
        target_url = urljoin(BASE_URL, target_date_str + "/")
        target_hour_url = urljoin(target_url, target_hour + "/")

        csv_links = get_links_by_suffix(target_hour_url, ".csv")

        if not csv_links:
            print(f"⚠️ 找不到 {target_date_str} {target_hour} 點的CSV檔")
            return None, None
        
        sorted_links = sorted(csv_links)

        if target_hour == "00":
            latest_csv_url = sorted_links[0]
        else:
            latest_csv_url = sorted_links[-1]

        parsed = urlparse(latest_csv_url)
        filename = os.path.basename(parsed.path)

        match = re.search(r'(\d{8}_\d{6})', filename)
        if match:
            timestamp_str = match.group(1)
            return latest_csv_url, timestamp_str + ".csv"
        
        print("⚠️ 無法從檔名中解析日期與時間")
        return None, None

    # === Airflow 任務: 爬取與過濾資料 ===
    @task
    def scrape_and_filter_data():
        csv_url, filename = get_latest_csv_link()
        if not csv_url:
            print("❌ 沒有找到最新的 CSV 檔案")
            return None, None

        try:
            print(f"⬇️ 下載最新檔案：{csv_url}")
            r = requests.get(csv_url, timeout=20)
            if r.status_code != 200:
                print(f"❌ 無法下載: {csv_url}")
                return None, None
            df = pd.read_csv(StringIO(r.text), header=None)
            if df.shape[1] != 6:
                print("⚠️ 欄位數量錯誤")
                return None, None
            df.columns = COLUMNS
            df = df[(df["GantryFrom"].isin(TARGET_IDS)) & (df["Speed"] != 0)]
            
            # ✅ 新增 'Date' 和 'Time' 欄位
            df['Date'] = pd.to_datetime(df['TimeStamp']).dt.strftime('%Y/%m/%d')
            df['Time'] = pd.to_datetime(df['TimeStamp']).dt.strftime('%H:%M')

            if df.empty:
                print("⚠️ 無可用資料進行統計")
                return None, None

            timestamp = df["TimeStamp"].iloc[0]
            grouped = df.groupby(["GantryFrom", "GantryTo"]).agg({
                "Speed": "mean",
                "Volume": "sum"
            }).reset_index()

            grouped = grouped[grouped["GantryTo"].isin(TARGET_IDS)]
            grouped.insert(0, "Time", df['Time'].iloc[0])
            grouped.insert(0, "Date", df['Date'].iloc[0])
            grouped.insert(2, "TimeStamp", timestamp)

            return grouped

        except Exception as e:
            print(f"⚠️ 下載失敗：{e}")
            return None

    # === Airflow 任務: 將資料寫入 BigQuery ===
    @task
    def load_to_bigquery(df):
        if df is None:
            print("⚠️ 無資料可上傳至 BigQuery")
            return

        # 這裡的 'if_exists' 參數非常重要，請根據您的需求選擇：
        # 'append': 將新的資料附加到現有資料表。
        # 'replace': 刪除現有資料表，用新的 DataFrame 替換。
        # 'fail': 如果資料表已存在，則拋出錯誤。
        # 這裡選擇 'append' 以確保每次爬蟲都能新增資料
        try:
            to_gbq(
                dataframe=df,
                destination_table=f"{DATASET_ID}.{TABLE_ID}",
                project_id=PROJECT_ID,
                if_exists="append",
                chunksize=10000,
                table_schema=[
                    {'name': 'Date', 'type': 'DATE'},
                    {'name': 'Time', 'type': 'TIME'},
                    {'name': 'TimeStamp', 'type': 'DATETIME'},
                    {'name': 'GantryFrom', 'type': 'STRING'},
                    {'name': 'GantryTo', 'type': 'STRING'},
                    {'name': 'Speed', 'type': 'FLOAT'},
                    {'name': 'Volume', 'type': 'FLOAT'}
                ]
            )
            print(f"✅ 資料已成功寫入 BigQuery: {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}")

        except Exception as e:
            print(f"❌ 寫入 BigQuery 失敗: {e}")
            raise # 拋出錯誤讓 Airflow 知道任務失敗

    # === 定義任務流程 ===
    # 這邊直接將一個任務的結果傳給下一個任務
    scraped_data = scrape_and_filter_data()
    load_to_bigquery(scraped_data)

# 執行 DAG
freeway_traffic_dag()