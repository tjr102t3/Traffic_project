// 頁面載入後立即執行一次，顯示初始路況
document.addEventListener('DOMContentLoaded', () => {
    updateTrafficStatusFromJson();
});

// 切換icon
const icons = document.querySelectorAll('.attractions .icon_item');
const lists = document.querySelectorAll('#list-container ul');
icons.forEach(icon => {
  icon.addEventListener('click', () => {
    // 移除全部 active
    icons.forEach(i => i.classList.remove('active'));
    lists.forEach(list => list.classList.add('d-none'));
    // 加 active 並顯示對應清單
    icon.classList.add('active');
    const target = icon.getAttribute('data-list');
    document.getElementById(target).classList.remove('d-none');
  });
});
