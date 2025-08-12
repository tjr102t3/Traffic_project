// 存取使用者位置
document.getElementById('allowBtn').addEventListener('click', function() {
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
      function(position) {
        alert(`經度: ${position.coords.longitude}, 緯度: ${position.coords.latitude}`);
        document.getElementById('locationModal').style.display = 'none';
      },
      function(error) {
        alert('定位失敗或被拒絕');
      }
    );
  } else {
    alert('此瀏覽器不支援定位功能');
  }
});