// JSON 檔案的 URL
const jsonUrl = '   dataTest.json';

// 根據時速判斷對應的顏色類別
function getColorBySpeed(speed) {
    if (speed < 40) {
        return 'red';
    } else if (speed >= 40 && speed < 60) {
        return 'orange';
    } else { // speed >= 60
        return 'green';
    }
}
// 獲取路段和圓點的DOM元素
const roadSegment1 = document.getElementById('road-segment-1');
const roadSegment2 = document.getElementById('road-segment-2');
const roadSegment3 = document.getElementById('road-segment-3');
const roadSegment4 = document.getElementById('road-segment-4');
// 主要函數：讀取 JSON 並更新路況
async function updateTrafficStatusFromJson() {
    try {
        const response = await fetch(jsonUrl);
        
        // 檢查請求是否成功
        if (!response.ok) {
            throw new Error(`HTTP 錯誤! 狀態碼: ${response.status}`);
        }

        const data = await response.json(); // 使用 .json() 方法自動解析 JSON
        console.log("從 JSON 檔案中獲取到的所有數據:", data);

        // 確保數據是一個非空的陣列
        if (!Array.isArray(data) || data.length === 0) {
            console.error('JSON 檔案沒有有效的數據。');
            return;
        }

        // 獲取陣列中最後一個物件，即最新的數據
        const latestData = data[data.length - 1];
        const latestSpeed = latestData.predictSpeed;
        
        console.log(`最新的預測時速為: ${latestSpeed} km/h`);

        // 根據最新的速度來更新路況
        const color = getColorBySpeed(latestSpeed);
        
        // 這裡我們假設整條路段都使用這個最新的速度來判斷顏色
        roadSegment1.className = 'status-bar PinglineToToucheng ' + color;
        roadSegment2.className = 'status-bar PinglineToToucheng ' + color;
        roadSegment3.className = 'status-bar TouchengToNangang ' + color;
        roadSegment4.className = 'status-bar TouchengToNangang ' + color;


    } catch (error) {
        console.error('讀取或解析 JSON 檔案時發生錯誤:', error);
    }
}