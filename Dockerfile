FROM apache/airflow:2.9.2-python3.10 

# Cài đặt netcat (nc) cho lệnh chờ database trong docker-compose.yaml
USER root
RUN apt-get update && apt-get install -y netcat-openbsd && apt-get clean
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip
RUN pip install  -r /requirements.
RUN pip install --upgrade yfinance --no-cache-dir