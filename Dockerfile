# 使用您 Airflow 的基礎映像檔
FROM apache/airflow:2.11.0-python3.12

# 安裝 MongoDB 和 PyTorch
# USER root 用來暫時切換成 root 權限來安裝套件
USER root

# 使用 pip 安裝 PyMongo (MongoDB 的 Python 驅動程式)
# 請注意，這裡指定了版本以確保穩定性
RUN pip install "pymongo==4.7.3"

# 使用 pip 安裝 PyTorch
# 這裡指定了版本以確保穩定性
RUN pip install "torch==2.3.1"

# 為了確保安全性，切換回 airflow 使用者
USER airflow