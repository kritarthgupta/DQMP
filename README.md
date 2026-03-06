# Data Quality Monitoring Platform (DQMP)

## 🚀 Project Overview

The **Data Quality Monitoring Platform (DQMP)** is a **Python + MySQL based framework** designed to automatically monitor and validate the quality of data pipelines.

It detects anomalies across multiple **data quality dimensions** and generates alerts when issues occur. All checks are **metadata-driven**, making the system flexible, scalable, and production-ready.

The platform performs automated checks such as:

- **Volume Check** – Ensures table row counts fall within expected ranges  
- **Null Check** – Identifies unexpected missing values in critical columns  
- **Freshness Check** – Validates that data updates occur within SLA thresholds  
- **Duplicate Check** – Detects duplicate records in primary key columns  
- **Referential Integrity Check** – Identifies orphan records between related tables  
- **Schema Drift Detection** – Detects unexpected changes in table structures  

Each run generates **Data Quality metrics, alerts, and a Data Quality Score**, enabling easy monitoring and troubleshooting.

---

## 🎯 Why DQMP?

Modern data pipelines often fail **silently** due to upstream data issues. DQMP helps prevent this by providing **automated monitoring and alerting**.

Key benefits:

- Detects **data issues early** before they impact analytics or ML models
- Provides **automated monitoring instead of manual checks**
- Generates **structured alerts for failures**
- Maintains **audit-friendly run history**
- Designed as a **modular and scalable data engineering framework**
- Serves as a strong **portfolio project for Data Engineer / Analytics Engineer roles**

---

## 📂 Folder Structure

```
dqmp/
│
├── config/
│   └── db_config.py            # Database connection configuration
│
├── sql/
│   ├── 00_create_database.sql  # Creates database: dqmp
│   ├── 01_create_tables.sql    # Creates tables for data, metadata, results, alerts
│   ├── 02_insert_sample_data.sql
│   ├── 03_dq_checks.sql        # Optional SQL-based DQ checks
│   └── 04_dq_alerts.sql        # Optional SQL-based alert generation
│
├── engine/
│   ├── __init__.py             # Package initializer exposing main functions
│   ├── db_utils.py             # Database utilities (connection, query execution)
│   └── dq_checks.py            # All DQ check implementations
│
├── main.py                     # Orchestrates DQ execution and alert generation
│
└── README.md
```

---

## ⚙️ Setup Instructions

### 1️⃣ Install Python Dependencies

```bash
pip install mysql-connector-python
```

### 2️⃣ Configure Database Connection

Update `config/db_config.py` with your MySQL credentials:

```python
db_config = {
    "host": "localhost",
    "user": "your_mysql_user",
    "password": "your_mysql_password",
    "database": "dqmp"
}
```

### 3️⃣ Initialize the Database

Run the SQL scripts in order using **MySQL Workbench** or CLI:

```bash
sql/00_create_database.sql
sql/01_create_tables.sql
sql/02_insert_sample_data.sql
```

These scripts will create:

- Sample tables (`customers`, `orders`)
- Metadata tables
- Result logging tables
- Alert tables
- Schema baseline tables

### 4️⃣ Run the Data Quality Checks

Execute the main pipeline:

```bash
python main.py
```

The script will:

1. Fetch metadata from `table_metadata`  
2. Run all configured DQ checks  
3. Insert results into `dq_results`  
4. Generate alerts in `dq_alerts`  
5. Calculate a **Data Quality Score**  
6. Print a summary of the run  

---

## 📊 Data Quality Checks Implemented

### Volume Check

Ensures the row count of a table is within the expected range defined in metadata.

Example:

```
orders expected volume: 5 – 100 rows
```

### Null Check

Identifies unexpected null values in critical columns.

Example:

```
orders.customer_id → NOT NULL
```

### Freshness Check

Validates that the latest timestamp in a column falls within the defined SLA.

Example:

```
orders.updated_at → freshness SLA = 60 minutes
```

### Duplicate Check

Detects duplicate records in columns that should be unique.

Example:

```
customers.customer_id → must be unique
```

### Referential Integrity Check

Ensures relationships between tables are valid.

Example:

```
orders.customer_id must exist in customers.customer_id
```

Detects **orphan records**.

### Schema Drift Check

Detects structural changes in tables by comparing the current schema with a baseline.

Examples:

- Columns added unexpectedly  
- Columns removed  
- Structural mismatches  

Schema drift details include:

```
missing:['email']
extra:['discount']
```

---

## 📊 Data Quality Score

Each run generates a **Data Quality Score** representing the overall health of the data.

### Formula

```
DQ Score = (Passed Checks / Total Checks) × 100
```

Example:

| Metric | Value |
|--------|-------|
| Total Checks | 15 |
| Passed Checks | 12 |
| Failed Checks | 3 |
| DQ Score | 80% |

This provides a **single health indicator** for the pipeline.

---

## 📊 Example Output Tables

### dq_results

| run_id | table_name | column_name | metric_type | metric_value | threshold | status |
|--------|------------|-------------|------------|--------------|----------|--------|
| 2026-03-06 | orders | customer_id | null_pct | 20 | NOT_NULL | FAIL |
| 2026-03-06 | orders | order_id | duplicate_count | 1 | NO_DUPLICATES | FAIL |

### dq_alerts

| alert_time | table_name | column_name | metric_type | severity | message |
|------------|-----------|------------|------------|----------|---------|
| 2026-03-06 | orders | customer_id | null_pct | WARNING | DQ check failed for orders.customer_id |

Schema drift alerts include detailed diagnostics:

```
missing:['email']
extra:['discount']
```

### dq_run_summary

| run_id | total_checks | passed_checks | failed_checks | dq_score |
|--------|-------------|---------------|---------------|---------|
| 2026-03-06 | 15 | 11 | 4 | 73.33 |

---

## 💡 Key Design Highlights

- **Metadata-Driven Checks** – thresholds defined in `table_metadata`  
- **Schema Baseline Validation** – detects structural changes  
- **Automated Alerting** – warnings and failures logged in `dq_alerts`  
- **Audit Trail** – every check stored in `dq_results`  
- **Data Quality Score** – aggregated health metric per run  
- **Modular Python Design** – easily extendable for additional checks  

---

## 🚀 Future Enhancements

- Integrate **Airflow orchestration**  
- Add **Slack / Email alert notifications**  
- Implement **Anomaly Detection** using statistical thresholds  
- Add **DQ dashboards** using Tableau / PowerBI  
- Introduce **pipeline-level SLA monitoring**  
- Add **historical trend analysis for DQ Score**  

---

## 💻 Tech Stack

- **Python 3.x**  
- **MySQL 8.x**  
- **Libraries:** `mysql-connector-python`, `datetime`  

---

## 🧠 Learning Outcomes

This project demonstrates practical implementation of:

- Data Quality Monitoring  
- Metadata-Driven Data Engineering  
- SQL-based validation frameworks  
- Data Observability principles  
- Modular Python architecture