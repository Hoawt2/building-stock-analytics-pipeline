```markdown
# 🏗️ Building a Stock Data Warehouse with Airflow & Docker

This project builds a **data engineering pipeline** that fetches, stages, and transforms historical and daily stock data into a centralized **Data Warehouse**, using **Airflow**, **Docker**, **MySQL**, and **PostgreSQL**.

---

## 🚀 Project Overview

### 🎯 Goals
- Automate daily stock data ingestion and transformation.
- Apply modern data engineering practices using Airflow for orchestration.
- Design a scalable multi-layer data architecture (Landing → Staging → DWH).
- Containerize the entire system with Docker for easy deployment.

---

## 🧩 Architecture

```
<img width="951" height="321" alt="Building-DW-with-Airflow-Python-for-IMDB drawio" src="https://github.com/user-attachments/assets/73bc0751-3e54-4c5d-be02-c34ea029f6f2" />


```

### Layers:
| Layer | Description | Tech |
|-------|--------------|------|
| **Landing** | Raw data fetched daily from APIs | MySQL |
| **Staging** | Cleaned and standardized tables | PostgreSQL |
| **DWH** | Transformed data for analytics | PostgreSQL |
| **Orchestration** | Scheduled ETL jobs | Apache Airflow |

---

## ⚙️ Tech Stack

| Component | Technology |
|------------|-------------|
| Orchestration | Apache Airflow 2.9.2 |
| Containers | Docker & Docker Compose |
| Databases | MySQL 8.0 (Landing), PostgreSQL 14 (Staging & DWH) |
| Language | Python 3.10+ |
| APIs | Financial Modeling Prep, YFinance |
| Data Libraries | Pandas, SQLAlchemy |
| Scheduler | Airflow DAGs (daily + historical runs) |

---

## 📁 Project Structure

---

├── dags/
│   ├── daily_update_dag.py           # Schedule daily incremental updates
│   ├── historical_load_dag.py        # One-time historical data load
│
├── plugins/
│   ├── mysql_operator.py             # Custom operator for MySQL I/O
│   ├── postgresql_operator.py        # Custom operator for PostgreSQL I/O
│   └── support_processing.py         # SQL template utilities
│
├── scripts/
│   ├── fetch_financial_modeling_prep.py  # Fetch stock data from API to Landing
│   ├── load_landing_to_staging.py        # Move + clean data into staging schema
│   ├── transform_staging_to_dwh.py       # Transform data to star-schema marts
│   ├── utils.py                          # Shared helper functions
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
└── README.md

---

---

## 🧠 Pipeline Logic

### 1️⃣ Historical Load (`historical_load_dag.py`)
- Runs once to backfill data from 2015 → present.
- Fetches data via `fetch_financial_modeling_prep.py`.
- Loads into **MySQL (Landing)**.

### 2️⃣ Daily Update (`daily_update_dag.py`)
- Runs automatically every day.
- Fetches new trading data from APIs.
- Cleans & loads to **PostgreSQL (Staging)**.
- Transforms to **Data Warehouse** via `transform_staging_to_dwh.py`.

---

## 🔧 Environment Setup

### 1. Clone repository
```bash
git clone https://github.com/<your_username>/stock-data-pipeline.git
cd stock-data-pipeline
````

### 2. Configure `.env`

```bash
# MySQL (Landing)
MYSQL_USER=landing_user
MYSQL_PASSWORD=landing_pass
MYSQL_DATABASE=landing_db
MYSQL_HOST=mysql
MYSQL_PORT=3306

# PostgreSQL (Staging/DWH)
POSTGRES_USER=staging_user
POSTGRES_PASSWORD=staging_pass
POSTGRES_DB=staging_db
POSTGRES_HOST=de_psql
POSTGRES_PORT=5432

# API Keys
FMP_API_KEY=your_fmp_api_key_here
```

### 3. Build & start Airflow cluster

```bash
docker-compose up --build
```

### 4. Access Airflow UI

Visit → [http://localhost:8080](http://localhost:8080)

```
username: airflow
password: airflow
```

---

## 🧩 Connections in Airflow UI

Before running DAGs, define your connections:

* **MySQL** → `mysql://landing_user:landing_pass@mysql:3306/landing_db`
* **Postgres** → `postgresql://staging_user:staging_pass@de_psql:5432/staging_db`

These should match your `.env` configuration.

---

## 📊 Data Model Example

**Dimension: Company**

```sql
CREATE TABLE dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(50),
    ipo_year INT
);
```

**Fact: Daily Stock Prices**

```sql
CREATE TABLE fact_stock_prices (
    company_id INT,
    date DATE,
    open NUMERIC,
    close NUMERIC,
    high NUMERIC,
    low NUMERIC,
    volume BIGINT,
    FOREIGN KEY (company_id) REFERENCES dim_company(company_id)
);
```

---

## 🧪 Future Improvements

* Add monitoring via Airflow sensors.
* Integrate DBT or Great Expectations for data quality.
* Deploy to cloud (GCP/AWS) using Terraform.
* Build dashboard using Metabase / Power BI.

---

## 🧑‍💻 Author

**Hoàng Nguyễn**
Data Engineer | Python & Airflow Enthusiast

---

## 📜 License

MIT License

