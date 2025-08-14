import os
import pandas as pd
import torch
import torch.nn as nn
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from pymongo import MongoClient

# ====================================================================
# === 模型與 BigQuery 參數設定 ===
# ====================================================================

PROJECT_ID = "test123-467809"
DATASET_ID = "bigquery"

TABLE_MAPPING = {
    "05F0287N": "traffic-data-05F0287N",
    "05F0055N": "traffic-data-05F0055N"
}

# 假設模型訓練時使用的最佳超參數
input_size = 2
hidden_size = 33
num_layers = 3
output_size = 1
sequence_length = 40

# 模型保存的路徑和檔名
MODEL_SAVE_PATH = '/opt/airflow/lstm_model/best_lstm_model.pth' 

# ====================================================================
# === PyTorch 模型類別定義 ===
# ====================================================================
class LSTMModel(nn.Module):
    # ... (模型類別定義不變)
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTMModel, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

# ====================================================================
# === 核心任務函式：整合所有邏輯 ===
# ====================================================================
def run_all_predictions(**kwargs):
    """
    對所有門架執行資料抓取、模型預測與 MongoDB 儲存。
    """
    print("模型預測 DAG 已被觸發，開始執行預測任務。")
    client_bq = bigquery.Client(project=PROJECT_ID)
    
    # === 新增：連接 MongoDB ===
    try:
        mongo_client = MongoClient('mongodb', 27017)
        db = mongo_client['traffic_predictions'] # 這裡只定義一個資料庫名稱
        print("成功連接到 MongoDB。")
    except Exception as e:
        print(f"連接 MongoDB 失敗：{e}")
        raise

    # 載入模型
    if not os.path.exists(MODEL_SAVE_PATH):
        raise FileNotFoundError(f"找不到模型檔案：{MODEL_SAVE_PATH}")
    
    loaded_model = LSTMModel(input_size, hidden_size, num_layers, output_size)
    loaded_model.load_state_dict(torch.load(MODEL_SAVE_PATH))
    loaded_model.eval()
    print("模型已成功載入並設定為評估模式。")

    for gantry_id, table_name in TABLE_MAPPING.items():
        print("---")
        print(f"正在處理門架：{gantry_id}...")

        # 步驟1: 從 BigQuery 抓取資料
        query = f"""
        SELECT Avg_speed, Total_volume
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        ORDER BY TimeStamp DESC
        LIMIT {sequence_length}
        """
        df = client_bq.query(query).to_dataframe()

        if len(df) < sequence_length:
            print(f"⚠️ 門架 {gantry_id} 數據不足 {sequence_length} 筆，跳過預測。")
            continue

        df = df.iloc[::-1].reset_index(drop=True)
        
        # 步驟2: 模型預測
        latest_features_np = df[['Avg_speed', 'Total_volume']].values[-sequence_length:]
        input_for_prediction = torch.tensor(latest_features_np, dtype=torch.float32).unsqueeze(0)

        with torch.no_grad():
            predicted_speed_tensor = loaded_model(input_for_prediction)

        predicted_average_speed = predicted_speed_tensor.squeeze().item()
        print(f"門架 {gantry_id} 預測的下一個時間步平均車速為: {predicted_average_speed:.2f}")

        # === 步驟3: 儲存到 MongoDB ===
        # 使用門架 ID 來動態選擇不同的 Collection
        collection = db[f'predicted_speeds_{gantry_id}']
        
        prediction_record = {
            "gantry_id": gantry_id,
            "predicted_speed": predicted_average_speed,
            "timestamp": datetime.utcnow()
        }
        collection.insert_one(prediction_record)
        print(f"預測結果已成功儲存至 MongoDB，門架 ID: {gantry_id}，集合名稱: {collection.name}")

    mongo_client.close()
    print("MongoDB 連接已關閉。")


# ====================================================================
# === Airflow DAG 設定 ===
# ====================================================================

with DAG(
    dag_id="traffic_prediction_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['bigquery', 'pytorch', 'prediction'],
) as dag:
    run_all_predictions_task = PythonOperator(
        task_id='run_prediction_for_all_gantries',
        python_callable=run_all_predictions
    )