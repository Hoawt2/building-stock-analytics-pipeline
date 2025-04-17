# Dùng image python chính thức
FROM python:3.10-slim

# Đặt thư mục làm việc trong container là /app
WORKDIR /etl

# Copy file requirements.txt vào container
COPY requirements.txt .

# Cài đặt thư viện từ requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY . .

# Lệnh mặc định để chạy app
CMD ["python", "run_all.py"]
