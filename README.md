# 🛒 E-Commerce ETL Pipeline with Apache Airflow

A production-style ETL pipeline built with **Apache Airflow 3.x**, orchestrating ingestion, transformation, validation, conditional branching, and incremental loading of e-commerce order data into **MySQL**.

---

## 📐 Architecture

Olist Dataset (CSV) → FileSensor → Extract → Transform & Clean
→ Data Quality Checks → Incremental Load Check → Branch Decision
├── Large Dataset Path ──┐
└── Small Dataset Path ──┘
↓
Load to MySQL → Audit Log → Pipeline Summary


---

## 🛠️ Tech Stack

Apache Airflow (TaskFlow API) · Python · Pandas · MySQL 8.0 · Docker · SQLAlchemy · DBeaver

---

## ✨ Key Features

- **Event-driven ingestion** via `FileSensor` — waits for the source file instead of running on a blind schedule
- **Multi-layer validation** — empty dataset, required columns, duplicate/null primary keys, future dates, row retention %
- **Conditional branching** — `@task.branch` routes execution based on data volume
- **Incremental loading** — only new records (via `MAX(timestamp)` in MySQL) load each run
- **Secure credentials** — Airflow Connections used throughout, no hardcoded secrets
- **Audit logging** — every run writes row counts/status to a dedicated audit table
- **Automatic retries** on transient failures

---

## 🚀 Setup

```bash
git clone https://github.com/YOUR_USERNAME/Apache_Airflow.git
cd Apache_Airflow
docker-compose up -d

# Create MySQL Airflow Connection
docker exec -it <airflow_container_name> airflow connections add 'mysql_default' \
    --conn-type 'mysql' --conn-host 'mysql' --conn-schema 'ecommerce_db' \
    --conn-login 'airflow_user' --conn-password 'airflow_pass' --conn-port 3306
```

Access Airflow at `http://localhost:8081` and trigger `ecommerce_etl_pipeline_v5`.

---

## 🐛 Debugging Notes

- **Trigger rule after branching:** the unchosen branch path is `skipped`, not `failed`. `load`'s default trigger rule (`all_success`) would skip itself too — fixed with `trigger_rule="none_failed_min_one_success"`.
- **SQLAlchemy dtype:** `to_sql()`'s `dtype` param needs an actual SQLAlchemy type object, not a string — fixed by using `DateTime()` from `sqlalchemy` instead of `"DATETIME"`.

---

## 🔭 Known Limitations

- Load isn't fully idempotent on exact-duplicate timestamps
- Branch paths converge to identical load logic (no `.expand()` yet)
- No alerting or CI/CD pipeline yet
- Single-node execution — no distributed processing

---

## 📌 Resume Summary

> Built a production-style ETL pipeline using Apache Airflow 3.x and Python to orchestrate CSV ingestion, transformation, validation, conditional branching, and incremental loading into MySQL — with FileSensor ingestion, TaskFlow API, audit logging, retries, and multi-layer data quality checks in a Dockerized environment.

---
