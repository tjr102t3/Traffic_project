import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from io import StringIO
import re
from airflow.decorators import task
from pandas_gbq import to_gbq

# === 參數設定 ===
BASE_URL = "https://tisvcloud.freeway.gov.tw/history/TDCS/M05A/"
COLUMNS = ["TimeStamp", "GantryFrom", "GantryTo", "VehicleType", "Speed", "Volume"]
TARGET_IDS = {"05F0287N", "05F0055N", "05F0001N"}

# === BigQuery 設定 ===
PROJECT_ID = "test123-467809"
DATASET_ID = "bigquery"
TABLE_ID = "traffic-data"

# === 工具函式 ===
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
        return None

    try:
        print(f"⬇️ 下載最新檔案：{csv_url}")
        r = requests.get(csv_url, timeout=20)
        if r.status_code != 200:
            print(f"❌ 無法下載: {csv_url}")
            return None
        df = pd.read_csv(StringIO(r.text), header=None)
        if df.shape[1] != 6:
            print("⚠️ 欄位數量錯誤")
            return None
        df.columns = COLUMNS
        df = df[(df["GantryFrom"].isin(TARGET_IDS)) & (df["Speed"] != 0)]
        
        df['Date'] = pd.to_datetime(df['TimeStamp']).dt.date
        df['Time'] = pd.to_datetime(df['TimeStamp']).dt.time

        if df.empty:
            print("⚠️ 無可用資料進行統計")
            return None

        timestamp = df["TimeStamp"].iloc[0]
        grouped = df.groupby(["GantryFrom", "GantryTo"]).agg({
            "Speed": "mean",
            "Volume": "sum"
        }).reset_index()

        grouped = grouped[grouped["GantryTo"].isin(TARGET_IDS)]
        grouped.insert(0, "Time", df['Time'].iloc[0])
        grouped.insert(0, "Date", df['Date'].iloc[0])
        grouped.insert(2, "TimeStamp", timestamp)
        
        # === 將這段程式碼貼在這裡！ ===
        # 將 TimeStamp 欄位明確地轉換為 datetime 物件
        # 這能確保資料型態與 BigQuery Schema 中的 DATETIME 吻合
        grouped['TimeStamp'] = pd.to_datetime(grouped['TimeStamp'])
        
        # 確保欄位順序正確 (如果您有保留 table_schema)
        # 這是為了避免欄位順序不符導致寫入失敗
        grouped = grouped[[
            'Date', 
            'Time', 
            'TimeStamp', 
            'GantryFrom', 
            'GantryTo', 
            'Speed', 
            'Volume'
        ]]

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
        raise