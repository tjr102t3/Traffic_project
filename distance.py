import json
import math

# ====== 輔助函式 ======
def haversine_distance(lat1, lon1, lat2, lon2):
    """計算兩點間的球面距離（單位：公里）"""
    R = 6371  # 地球半徑 (km)
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return round(R * c, 2)

# ====== 載入各類型資料 ======
def load_all_places():
    file_paths = {
        "咖啡廳": "",####file path
        "伴手禮": "",####file path
        "溫泉": "", ####file path      
        "加油站": ""####file path
    }

    all_places = {}
    for category, path in file_paths.items():
        try:
            with open(path, "r", encoding="utf-8") as f:
                all_places[category] = json.load(f)
        except FileNotFoundError:
            print(f"❌ 檔案不存在：{path}")
            all_places[category] = []
        except json.JSONDecodeError:
            print(f"❌ JSON 格式錯誤：{path}")
            all_places[category] = []

    return all_places

# ====== 主功能：找出每類別前三近的地點 ======
def find_nearest_by_category(user_lat, user_lon):
    all_places = load_all_places()
    result = {}

    for category, places in all_places.items():
        distances = []
        for place in places:
            try:
                # 處理不同欄位名稱
                lat = float(place.get("緯度") or place.get("lat"))
                lon = float(place.get("經度") or place.get("lng"))
                name = place.get("名稱") or place.get("name") or "未知名稱"

                dist = haversine_distance(user_lat, user_lon, lat, lon)
                distances.append({
                    "名稱": name,
                    "距離(km)": dist
                })
            except (KeyError, ValueError, TypeError):
                continue  # 忽略無效資料

        # 取前三近距離
        top3 = sorted(distances, key=lambda x: x["距離(km)"])[:3]
        result[category] = top3

    return result

# # ====== 測試範例 ======
# if __name__ == "__main__":
#     # 假設使用者在宜蘭火車站附近
#     user_lat = 24.608951963674198
#     user_lon = 121.8308405813788

#     nearest = find_nearest_by_category(user_lat, user_lon)
#     print(json.dumps(nearest, ensure_ascii=False, indent=2))
