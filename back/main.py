import os
from flask import Flask, jsonify, request
from flask_cors import CORS
from pymongo import MongoClient
from bson.json_util import dumps

# 應用程式初始化
app = Flask(__name__)
# 啟用 CORS，允許跨域請求
CORS(app, resources={r"/api/*": {"origins": "https://tjr102-traffic-project.de.r.appspot.com"}})

# ====================================================================
# === MongoDB 連線設定 ===
# ====================================================================

# 從環境變數中獲取 MongoDB 連線 URI
# 如果環境變數不存在，則使用預設值（適用於本機開發）
MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
DB_NAME = os.environ.get('DB_NAME', 'traffic_predictions')

# 建立 MongoDB 客戶端連線
# 使用 try...except 塊來處理連線錯誤
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    print(f"成功連線到 MongoDB 資料庫: {DB_NAME}")
except Exception as e:
    print(f"連線到 MongoDB 時發生錯誤: {e}")
    client = None
    db = None

# ====================================================================
# === API 端點 ===
# ====================================================================

def get_collection(collection_name):
    """
    根據名稱獲取指定的 collection。
    如果資料庫連線失敗，則回傳 None。
    """
    if db is None:
        return None
    return db[collection_name]

@app.route('/api/traffic-status/<string:gantry_id>', methods=['GET'])
def get_traffic_status(gantry_id):
    """
    根據 gantry_id 獲取交通狀況數據。
    現在會根據 gantry_id 動態選擇 collection。
    """
    # 根據 gantry_id 動態建立 collection 名稱
    collection_name = f"predicted_speeds_{gantry_id}"
    collection = get_collection(collection_name)
    if collection is None:
        return jsonify({"error": "資料庫連線不可用"}), 500

    # 查詢指定 gantry_id 的最新一筆文件
    data = collection.find_one({"gantry_id": gantry_id}, sort=[('timestamp', -1)])

    if data:
        # 將 MongoDB 的 ObjectId 轉換為字串以便於 JSON 序列化
        data['_id'] = str(data['_id'])
        return jsonify(data)
    else:
        # 如果找不到數據，回傳 404 Not Found
        return jsonify({"error": f"找不到指定的門架 ID 或集合: {collection_name}"}), 404

@app.route('/api/weather-data/<string:gantry_id>', methods=['GET'])
def get_weather_data(gantry_id):
    """
    根據 gantry_id 獲取天氣數據。
    """
    collection = get_collection('weather_data')
    if collection is None:
        return jsonify({"error": "資料庫連線不可用"}), 500

    # 這裡的查詢邏輯需要根據您的數據結構調整
    # 假設查詢條件仍然是 gantry_id
    data = collection.find_one({"gantry_id": gantry_id})

    if data:
        return jsonify(dumps(data))
    else:
        return jsonify({"error": "找不到指定的天氣數據"}), 404

# ====================================================================
# === 伺服器啟動 ===
# ====================================================================

if __name__ == '__main__':
    # 僅在本機開發時執行此區塊
    app.run(host='0.0.0.0', port=5000)
