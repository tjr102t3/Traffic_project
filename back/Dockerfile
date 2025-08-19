# 使用官方 Python 3.12 映像檔作為基礎
FROM python:3.12-slim

# 設定工作目錄
WORKDIR /app

# 將 requirements.txt 複製到容器中並安裝依賴
COPY requirements.txt .
RUN pip install -r requirements.txt

# 將所有程式碼複製到容器中
COPY . .

# 設定環境變數
ENV MONGO_URI="mongodb://10.140.0.2:27017/"
ENV DB_NAME="traffic_predictions"
ENV PORT=8080

# 應用程式在容器內運行的指令
CMD ["gunicorn", "--bind", "0.0.0.0:8080", "main:app"]