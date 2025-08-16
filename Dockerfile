# 使用您 Airflow 的基礎映像檔
FROM apache/airflow:2.11.0-python3.12

# 為了確保安全性，切換回 airflow 使用者來安裝套件
# 這是 Airflow 官方映像檔建議的做法
USER airflow

# 使用 pip 安裝 PyMongo (MongoDB 的 Python 驅動程式)
RUN pip install "pymongo==4.7.3"

# 使用 pip 安裝 PyTorch
RUN pip install "torch==2.3.1"