// CSV 檔案的 URL，在實際部署時，請確保這個路徑是正確的
const csvUrl = 'speed_color_datatest.csv';

// 根據時速判斷對應的顏色類別
function getColorBySpeed(speed) {
    if (speed < 40) {
        return 'red';
    } else if (speed >= 40 && speed < 60) {
        return 'orange';
    } else { // speed > 60
        return 'green';
    }
}

// 獲取路段和圓點的DOM元素
const roadSegment1 = document.getElementById('road-segment-1');
const roadSegment2 = document.getElementById('road-segment-2');
const dotStart = document.getElementById('dot-start');
const dotMiddle = document.getElementById('dot-middle');
const dotEnd = document.getElementById('dot-end');

// 主要函數：讀取 CSV 並更新路況
async function updateTrafficStatusFromCsv() {
    try {
        const response = await fetch(csvUrl);
        // console.log(response);
        const csvText = await response.text();
        console.log(csvText);
        // 將 CSV 內容按行分割
        // const lines = csvText.trim().split('\n').map(line => line.trim());
        const lines = csvText.replace(/\r/g, '').split('\n');
        console.log(lines)
        // 移除表頭
        const dataLines = lines.slice(1);
        
        if (dataLines.length === 0) {
            console.error('CSV 檔案沒有數據。');
            return;
        }

        // 獲取最後一行數據，即最新的數據
        const lastLine = dataLines[dataLines.length - 1];
        const columns = lastLine.split(',');

        // 假設 predictSpeed 在第三個位置 (索引 2)
        const latestSpeed = parseInt(columns[2], 10);
        console.log(`最新的預測時速為: ${latestSpeed} km/h`);

        // 根據最新的速度來更新路況
        const color = getColorBySpeed(latestSpeed);
        
        // 這裡我們假設整條路段都使用這個最新的速度來判斷顏色
        roadSegment1.className = 'status-bar ' + color;
        roadSegment2.className = 'status-bar ' + color;
        
        dotStart.className = 'dot ' + color;
        dotMiddle.className = 'dot ' + color;
        dotEnd.className = 'dot ' + color;

    } catch (error) {
        console.error('讀取或解析 CSV 檔案時發生錯誤:', error);
    }
}

// 頁面載入後立即執行一次，顯示初始路況
document.addEventListener('DOMContentLoaded', () => {
    updateTrafficStatusFromCsv();
});