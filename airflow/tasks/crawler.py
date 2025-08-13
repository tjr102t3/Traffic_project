import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from datetime import datetime, timedelta
from io import StringIO
import re
from pandas_gbq import to_gbq
from google.cloud import bigquery

# === 參數設定 ===
BASE_URL = "https://tisvcloud.freeway.gov.tw/history/TDCS/M05A/"
COLUMNS = ["TimeStamp", "GantryFrom", "GantryTo", "VehicleType", "Avg_speed", "Total_volume"]
TARGET_IDS = {"05F0287N", "05F0055N"}

# === BigQuery 設定 ===
# ***需更改***
PROJECT_ID = "test123-467809"
# ***需更改***
DATASET_ID = "bigquery"
# === 新增表格名稱對應關係 ===
TABLE_MAPPING = {
    "05F0287N": "traffic-data-05F0287N",
    "05F0055N": "traffic-data-05F0055N"
}
TABLE_SCHEMA = [
    {'name': 'Date', 'type': 'DATE'},
    {'name': 'Time', 'type': 'TIME'},
    {'name': 'TimeStamp', 'type': 'DATETIME'},
    {'name': 'GantryFrom', 'type': 'STRING'},
    {'name': 'GantryTo', 'type': 'STRING'},
    {'name': 'Avg_speed', 'type': 'FLOAT'},
    {'name': 'Total_volume', 'type': 'FLOAT'}
]

# === 工具函式 ===

def get_links_by_suffix(url, suffix):
    """從網頁上爬取符合特定副檔名的連結"""
    try:
        res = requests.get(url, timeout=10)
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

def check_data_exists(table_id, timestamp, gantry_from_id):
    """檢查 BigQuery 中是否已存在該筆資料，避免重複寫入"""
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
    SELECT count(*) FROM `{PROJECT_ID}.{DATASET_ID}.{table_id}`
    WHERE TimeStamp = '{timestamp}' AND GantryFrom = '{gantry_from_id}'
    """
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        if row[0] > 0:
            return True
    return False

# === 任務函式 ===

def get_target_csv_info(**kwargs):
    """
    根據 DAG 執行時間判斷是否爬取，並返回目標 CSV 檔案的資訊。
    """
    now = kwargs['logical_date']
    weekday = now.weekday() # 星期一(0)到星期日(6)

    perform_scrape = False
    
    if weekday == 5 or weekday == 6: # 星期六、日
        perform_scrape = True
    elif weekday == 0: # 星期一
        if now.hour == 0 and now.minute == 0:
            perform_scrape = True
    
    if not perform_scrape:
        print(f"🔍 目前邏輯日期 {now.strftime('%Y-%m-%d %H:%M')} 不在指定爬取時段，任務將跳過。")
        return None

    target_date_str = now.strftime("%Y%m%d")
    target_hour_str = now.strftime("%H")
    
    target_url_date = urljoin(BASE_URL, target_date_str + "/")
    target_url_hour = urljoin(target_url_date, target_hour_str + "/")

    csv_links = get_links_by_suffix(target_url_hour, ".csv")

    if not csv_links:
        print(f"⚠️ 找不到 {target_date_str} {target_hour_str} 點的 CSV 檔")
        return None

    sorted_links = sorted(csv_links)
    
    target_filename_prefix = now.strftime('%Y%m%d_%H%M')
    for link in reversed(sorted_links):
        if target_filename_prefix in link:
            parsed = urlparse(link)
            filename = os.path.basename(parsed.path)
            match = re.search(r'(\d{8}_\d{6})', filename)
            if match:
                timestamp_str = match.group(1)
                return {'url': link, 'timestamp': timestamp_str}
    
    latest_csv_url = sorted_links[-1]
    parsed = urlparse(latest_csv_url)
    filename = os.path.basename(parsed.path)
    match = re.search(r'(\d{8}_\d{6})', filename)
    if match:
        timestamp_str = match.group(1)
        return {'url': latest_csv_url, 'timestamp': timestamp_str}

    print("⚠️ 無法從檔名中解析日期與時間")
    return None

def scrape_and_process_data(csv_info):
    """下載、清洗並處理 CSV 資料，針對每個門架進行聚合"""
    if not csv_info:
        print("無爬取資訊，任務結束。")
        return None

    csv_url = csv_info['url']
    timestamp_str = csv_info['timestamp']

    try:
        print(f"⬇️ 下載檔案：{csv_url}")
        r = requests.get(csv_url, timeout=20)
        r.raise_for_status()
        
        df = pd.read_csv(StringIO(r.text), header=None, names=COLUMNS)
        
        filtered_df = df[
            (df["GantryFrom"].isin(TARGET_IDS)) & 
            (df["Avg_speed"] != 0)
        ].copy()

        if filtered_df.empty:
            print(f"⚠️ 過濾後無可用資料進行統計。")
            return None
        
        filtered_df['TimeStamp'] = pd.to_datetime(filtered_df['TimeStamp'])
        filtered_df['Date'] = filtered_df['TimeStamp'].dt.date
        filtered_df['Time'] = filtered_df['TimeStamp'].dt.time
        
        grouped_df = filtered_df.groupby(["GantryFrom", "GantryTo"]).agg(
            Avg_speed_mean=("Avg_speed", "mean"),
            Total_volume_sum=("Total_volume", "sum")
        ).reset_index()

        grouped_df.insert(0, "Date", filtered_df['Date'].iloc[0])
        grouped_df.insert(1, "Time", filtered_df['Time'].iloc[0])
        grouped_df.insert(2, "TimeStamp", filtered_df['TimeStamp'].iloc[0])
        
        grouped_df = grouped_df.rename(columns={
            'Avg_speed_mean': 'Avg_speed',
            'Total_volume_sum': 'Total_volume'
        })
        
        result = {}
        for gantry_id in TARGET_IDS:
            df_gantry = grouped_df[grouped_df["GantryFrom"] == gantry_id].copy()
            if not df_gantry.empty:
                result[gantry_id] = df_gantry
        
        return result

    except requests.exceptions.RequestException as e:
        print(f"❌ 下載失敗，HTTP 錯誤：{e}")
        return None
    except Exception as e:
        print(f"⚠️ 資料處理失敗：{e}")
        return None

def load_to_bigquery(processed_data):
    """將處理後的資料分別寫入 BigQuery"""
    if not processed_data:
        print("無資料可上傳至 BigQuery。")
        return

    client = bigquery.Client(project=PROJECT_ID)

    for gantry_id, df in processed_data.items():
        table_id = TABLE_MAPPING.get(gantry_id)
        if not table_id:
            print(f"❌ 找不到 {gantry_id} 對應的表格名稱，跳過寫入。")
            continue
            
        df = df[['Date', 'Time', 'TimeStamp', 'GantryFrom', 'GantryTo', 'Avg_speed', 'Total_volume']]
        
        timestamp = df['TimeStamp'].iloc[0]
        if check_data_exists(table_id, timestamp, gantry_id):
            print(f"⚠️ {timestamp} 的 {gantry_id} 資料已存在於表格 '{table_id}'，跳過寫入。")
            continue

        try:
            to_gbq(
                dataframe=df,
                destination_table=f"{DATASET_ID}.{table_id}",
                project_id=PROJECT_ID,
                if_exists="append",
                chunksize=10000,
                table_schema=TABLE_SCHEMA
            )
            formatted_timestamp = timestamp.strftime('%Y%m%d_%H%M%S')
            print(f"✅ 資料已成功寫入 BigQuery，門架 {gantry_id}，表格 '{table_id}'。")
            print(f"記錄名稱範例：{formatted_timestamp}_{gantry_id}")

        except Exception as e:
            print(f"❌ 寫入 BigQuery 失敗，門架 {gantry_id}，表格 '{table_id}': {e}")
            raise