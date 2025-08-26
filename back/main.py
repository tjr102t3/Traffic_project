import os
import time
from flask import Flask, jsonify
from flask_cors import CORS
from pymongo import MongoClient
from bson.json_util import dumps
from google.cloud import secretmanager

# ====================================================================
# === 應用程式初始化與環境變數載入 ===
# ====================================================================

# 應用程式初始化
app = Flask(__name__)
# 啟用 CORS，允許跨域請求
CORS(app, resources={r"/api/*": {"origins": "*"}})

# 從環境變數取得設定
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_HOST = os.environ.get('MONGO_HOST')
MONGO_AUTH_SOURCE = os.environ.get('MONGO_AUTH_SOURCE')
DB_NAME = os.environ.get('DB_NAME', 'traffic_predictions')
SECRET_NAME = "projects/tjr102-traffic-project/secrets/mongo-password/versions/latest"
    
# 全域變數來儲存 MongoDB 客戶端和資料庫實例
client = None
db = None

# ====================================================================
# === 啟動時的強健連線管理（已重新設計） ===
# ====================================================================

def get_mongo_password_with_retry(max_retries=5, initial_delay=1):
    """
    從 Google Cloud Secret Manager 獲取 MongoDB 密碼，並帶有重試機制。
    """
    secret_client = secretmanager.SecretManagerServiceClient()
    retries = 0
    while retries < max_retries:
        try:
            print(f"嘗試從 Secret Manager 獲取密碼 (第 {retries + 1} 次嘗試)...")
            response = secret_client.access_secret_version(request={"name": SECRET_NAME})
            password = response.payload.data.decode("UTF-8")
            print("成功從 Secret Manager 獲取密碼。")
            return password
        except Exception as e:
            retries += 1
            delay = initial_delay * (2 ** retries)  # 指數退避
            print(f"從 Secret Manager 獲取密碼時發生錯誤: {e}. 將在 {delay} 秒後重試。")
            time.sleep(delay)
    print("從 Secret Manager 獲取密碼失敗，已達到最大重試次數。")
    return None

def init_mongodb_connection(max_retries=5, initial_delay=1):
    """
    在應用程式啟動時，建立一個持久的 MongoDB 連線。
    此函數將持續重試，直到連線成功為止。
    """
    global client, db
    
    # 步驟 1: 先獲取密碼
    mongo_password = get_mongo_password_with_retry()
    if not mongo_password:
        return False # 如果無法獲取密碼，直接返回失敗

    retries = 0
    while retries < max_retries:
        try:
            print(f"嘗試連線到 MongoDB... (第 {retries + 1} 次嘗試)")
            mongo_uri = f"mongodb://{MONGO_USER}:{mongo_password}@{MONGO_HOST}/?authSource={MONGO_AUTH_SOURCE}"
            
            # 建立連線，並設定一個較短的逾時，以避免啟動階段卡住
            client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            
            # 執行 ping 來確認連線
            client.admin.command('ping')
            
            # 如果成功，設定資料庫實例並返回
            db = client[DB_NAME]
            print(f"成功連線到 MongoDB 資料庫: {DB_NAME}")
            return True
        except Exception as e:
            retries += 1
            delay = initial_delay * (2 ** retries) # 指數退避
            print(f"連線到 MongoDB 時發生錯誤: {e}. 將在 {delay} 秒後重試。")
            client = None
            db = None
            time.sleep(delay)
    
    # 如果所有重試都失敗
    print("無法連線到 MongoDB，已達到最大重試次數。")
    return False

# ====================================================================
# === API 端點（已簡化） ===
# ====================================================================

def get_collection(collection_name):
    """
    直接從全局 db 實例獲取集合。
    """
    return db[collection_name]

@app.route('/api/traffic-status/<string:gantry_id>', methods=['GET'])
def get_traffic_status(gantry_id):
    """
    根據 gantry_id 獲取交通狀況數據。
    """
    if db is None:
        return jsonify({"error": "資料庫連線不可用，請檢查後端日誌"}), 500

    collection_name = f"predicted_speeds_{gantry_id}"
    collection = get_collection(collection_name)
    
    try:
        data = collection.find_one({"gantry_id": gantry_id}, sort=[('timestamp', -1)])
        if data:
            data['_id'] = str(data['_id'])
            return jsonify(data)
        else:
            return jsonify({"error": f"找不到指定的門架 ID 或集合: {collection_name}"}), 404
    except Exception as e:
        print(f"在查詢資料時發生錯誤: {e}")
        return jsonify({"error": "在查詢資料時發生後端錯誤"}), 500

@app.route('/api/weather-data/<string:gantry_id>', methods=['GET'])
def get_weather_data(gantry_id):
    """
    根據 gantry_id 獲取天氣數據。
    """
    if db is None:
        return jsonify({"error": "資料庫連線不可用，請檢查後端日誌"}), 500
        
    collection = get_collection('weather_data')
    
    try:
        data = collection.find_one({"gantry_id": gantry_id})
        if data:
            data['_id'] = str(data['_id'])
            return jsonify(data)
        else:
            return jsonify({"error": "找不到指定的門框數據"}), 404
    except Exception as e:
        print(f"在查詢資料時發生錯誤: {e}")
        return jsonify({"error": "在查詢資料時發生後端錯誤"}), 500

# ====================================================================
# === 伺服器啟動 ===
# ====================================================================

# 在應用程式啟動前，先嘗試建立資料庫連線
# 💡 這是一個關鍵的改變！
if not init_mongodb_connection():
    # 如果連線失敗，則主動終止應用程式
    # Cloud Run 會偵測到這個失敗並自動重啟容器
    # 讓它有機會在下一次重啟時，於更穩定的網路環境中連線
    print("啟動階段資料庫連線失敗，退出應用程式。")
    exit(1)

if __name__ == '__main__':
    # 在本地開發環境中，直接使用 Flask 內建伺服器。
    app.run(host='0.0.0.0', port=5000)
