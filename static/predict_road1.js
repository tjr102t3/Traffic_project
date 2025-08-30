// ====================================================================
// === 全局設定與 DOM 元素獲取 ===
// ====================================================================

// Flask 後端服務的基礎 URL
// VM 外部 IP
const API_BASE_URL = 'https://traffic-api-127768281696.asia-east1.run.app/';

// 獲取路段的 DOM 元素，這些元素將會根據 API 數據更新顏色
const roadSegment1 = document.getElementById('road-segment-1');
const roadSegment2 = document.getElementById('road-segment-2');
const roadSegment3 = document.getElementById('road-segment-3');
const roadSegment4 = document.getElementById('road-segment-4');
// 獲取「更新」按鈕的 DOM 元素
const updateBtn = document.querySelector('.btn-primary');
/**
 * 根據預測時速判斷對應的顏色類別。
 * @param {number} speed - 預測時速 (km/h)。
 * @returns {string} - 對應的顏色類別名稱 ('red', 'orange', 'green')。
 */
function getColorBySpeed(speed) {
    if (speed < 40) {
        return 'red'; // 嚴重壅塞
    } else if (speed >= 40 && speed < 60) {
        return 'orange'; // 壅塞
    } else { // speed >= 60
        return 'green'; // 暢通
    }
}

// ====================================================================
// === 輔助函數：根據數據更新路段顏色 ===
// ====================================================================

/**
 * 根據提供的數據和設定，更新路段的樣式。
 * 這個函數將更新所有相關的路段 DOM 元素。
 * @param {Array<HTMLElement>} roadSegments - 包含多個路段 DOM 元素的陣列。
 * @param {number} speed - 從 API 獲取的預測時速。
 * @param {string} classNamePrefix - 樣式類別的前綴，例如 'status-bar PinglineToToucheng'。
 * @param {string} gantryId - 門架的 ID，用於日誌輸出。
 */
function updateRoadSegment(roadSegments, speed, classNamePrefix, gantryId) {
    // 檢查預測時速是否存在
    if (speed !== undefined) {
        const color = getColorBySpeed(speed);
        // 遍歷所有相關路段，並更新其 class 名稱
        roadSegments.forEach(segment => {
            segment.className = `${classNamePrefix} ${color}`;
        });
        console.log(`門架 ${gantryId} 的預測時速為: ${speed} km/h，顏色為: ${color}`);
    } else {
        console.warn(`API 回應中找不到 ${gantryId} 的預測時速。`);
    }
}

// ====================================================================
// === 主要函數：從 API 取得數據並更新路況 ===
// ====================================================================

async function updateTrafficStatusFromApi() {
    
    console.log("正在從後端 API 獲取最新的交通數據...");

    // 將所有門架的配置儲存在一個陣列中，便於擴展和管理
    const gantryConfigs = [
        {
            id: '05F0287N',
            roadSegments: [roadSegment1, roadSegment2],
            classNamePrefix: 'status-bar PinglineToToucheng'
        },
        {
            id: '05F0055N',
            roadSegments: [roadSegment3, roadSegment4],
            classNamePrefix: 'status-bar TouchengToNangang'
        }
    ];

    try {
        // 使用 Promise.all 同時發送所有 API 請求，提高效率
        const fetchPromises = gantryConfigs.map(config => 
            fetch(`${API_BASE_URL}/api/traffic-status/${config.id}`)
        );

        const responses = await Promise.all(fetchPromises);
        
        // 處理所有回應，並解析為 JSON
        const data = await Promise.all(responses.map((response, index) => {
            if (!response.ok) {
                // 如果任一請求失敗，則拋出錯誤
                throw new Error(`HTTP 錯誤! 門架 ${gantryConfigs[index].id} 狀態碼: ${response.status}`);
            }
            return response.json();
        }));

        // 根據配置和獲取的數據，動態更新所有路段的路況
        gantryConfigs.forEach((config, index) => {
            const speed = data[index].predicted_speed;
            updateRoadSegment(config.roadSegments, speed, config.classNamePrefix, config.id);
        });

    } catch (error) {
        console.error('從後端 API 讀取數據時發生錯誤:', error);
    }
}

// 首次載入頁面時執行，並每隔 15 秒重新獲取一次數據
// 💡 請根據你的需求調整時間間隔 (單位: 毫秒)
// document.addEventListener('DOMContentLoaded', () => {
//     updateTrafficStatusFromApi();
//     setInterval(updateTrafficStatusFromApi, 15000); 
// });

// 按下才更新
document.addEventListener('DOMContentLoaded', () => {
    // 頁面載入時首次執行一次
    // updateTrafficStatusFromApi();

    // 監聽按鈕點擊事件
    if (updateBtn) {
        updateBtn.addEventListener('click', updateTrafficStatusFromApi);
    }
});
