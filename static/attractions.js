document.getElementById('allowBtn').addEventListener('click', function() {
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
      function(position) {
        alert(`經度: ${position.coords.longitude}, 緯度: ${position.coords.latitude}`);
        const modal = document.getElementById('locationModal');
        if (modal) {
            modal.style.display = 'none';
        }
        // 緯度 latitude、經度 longitude
        const latitude = position.coords.latitude;
        const longitude = position.coords.longitude;

        // 建立要送給後端的物件
        const data = {
          lat: latitude,
          lng: longitude
        };

        // 用 fetch 發送 POST 請求（保證拿到位置後才送）
        fetch('/api/location', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json'
          },
          body: JSON.stringify(data)
        })
        .then(response => response.json())
        .then(result => {
          console.log('後端回應:', result);

          // 對應關係：分類名稱 → ul 的 id
          const categoryMap = {
            "咖啡廳": "list1",
            "伴手禮": "list2",
            "溫泉": "list3",
            "加油站": "list4"
          };

          for (const category in categoryMap) {
            const ul = document.getElementById(categoryMap[category]);
            ul.innerHTML = ''; // 先清空原本內容

            if (result[category] && result[category].length > 0) {
              result[category].forEach(place => {
                const li = document.createElement('li');
                li.className = "list-group-item";
                li.innerHTML = `${place.名稱} <a href="${place.網址}">${place.地址}</a>`;
                ul.appendChild(li);
              });
            } else {
              // 如果沒有資料，顯示提示
              const li = document.createElement('li');
              li.className = "list-group-item";
              li.textContent = "目前沒有資料";
              ul.appendChild(li);
            }
          }
        })
        .catch(error => {
          console.error('送出經緯度失敗:', error);
          alert('送出經緯度失敗，請檢查後端或網路連線');
        });

      },
      function(error) {
        alert('定位失敗或被拒絕');
      }
    );
  } else {
    alert('此瀏覽器不支援定位功能');
  }
});
