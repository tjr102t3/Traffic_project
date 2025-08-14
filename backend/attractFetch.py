from flask import Flask, request, jsonify
from flask_cors import CORS
import json
import math

app = Flask(__name__)
CORS(app)  # 允許所有來源，開發測試用

# 計算兩點球面距離
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return round(R * c, 2)

def load_all_places():
    file_paths = {
        "咖啡廳": "/home/webserver-gcp-user/Traffic_project/MongoDBoutput.json",
        "伴手禮": "/home/webserver-gcp-user/Traffic_project/MongoDBoutput.json",
        "溫泉": "/home/webserver-gcp-user/Traffic_project/MongoDBoutput.json",
        "加油站": "/home/webserver-gcp-user/Traffic_project/MongoDBoutput.json"
    }
    all_places = {}
    for category, path in file_paths.items():
        try:
            with open(path, "r", encoding="utf-8") as f:
                all_places[category] = json.load(f)
        except Exception as e:
            print(f"❌ 讀取 {category} 失敗：{e}")
            all_places[category] = []

    return all_places

def find_nearest_by_category(user_lat, user_lon):
    all_places = load_all_places()
    result = {}

    for category, places in all_places.items():
        distances = []
        for place in places:
            try:
                lat = float(place.get("緯度") or place.get("lat"))
                lon = float(place.get("經度") or place.get("lng"))
                name = place.get("名稱") or place.get("name") or "未知名稱"
                address = place.get("地址") or place.get("address") or "未知名稱"
                googlemap = place.get("網址") or place.get("googlemap") or "未知名稱"
                dist = haversine_distance(user_lat, user_lon, lat, lon)
                distances.append({
                    "名稱": name,
                    "距離(km)": dist,
                    "地址":address,
                    "網址":googlemap
                })
            
            except:
                continue
        
        top3 = sorted(distances, key=lambda x: x["距離(km)"])[:3]
        result[category] = top3
    return result

@app.route('/api/location', methods=['POST'])
def receive_location():
    data = request.get_json()
    lat = data.get('lat')
    lng = data.get('lng')
    print(f"收到經緯度: {lat}, {lng}")

    nearest = find_nearest_by_category(lat, lng)
    return jsonify(nearest)

if __name__ == "__main__":
    app.run(port=5000, debug=True)

