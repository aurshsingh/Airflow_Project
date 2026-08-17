# 🛒 E-Commerce ETL Pipeline with Apache Airflow

I built this project to practice building a complete ETL pipeline using **Apache Airflow, Python, Pandas, and MySQL**.

The idea was to take e-commerce order data from CSV files, process and validate it, and then load the results into MySQL. I also wanted the pipeline to behave more like a real production workflow rather than just a simple Python script.

## 📐 How the Pipeline Works

The pipeline follows this flow:

**CSV File → FileSensor → Extract → Transform & Clean → Data Validation → Incremental Check → Branching → MySQL → Audit Log**

The source data is the **Brazilian Olist e-commerce dataset**.

Airflow is responsible for coordinating the different steps and making sure they run in the correct order.

## 🛠️ Technologies Used

* Apache Airflow 3.x
* Python
* Pandas
* MySQL 8.0
* Docker
* SQLAlchemy
* SQL
* DBeaver

## 🔧 What I Implemented

### 1. File-based ingestion

Instead of assuming that the input file is always available, I used an Airflow **FileSensor** to wait for the source file.

Once the file is available, the pipeline starts processing it.

### 2. Data cleaning and transformation

Python and Pandas are used to:

* Read the source CSV
* Clean the data
* Handle missing values
* Remove duplicates
* Convert data types
* Prepare the data for MySQL

### 3. Data quality checks

I added several checks before loading the data.

The pipeline checks things such as:

* Whether the dataset is empty
* Whether required columns exist
* Duplicate primary keys
* Null primary keys
* Future dates
* Whether an unexpected percentage of records was lost during transformation

This prevents obviously bad data from continuing through the pipeline.

### 4. Conditional branching

I used Airflow's `@task.branch` to make the pipeline take different paths depending on the size of the dataset.

There is a path for larger datasets and another for smaller datasets.

Both paths eventually continue to the MySQL loading step.

### 5. Incremental loading

The pipeline does not simply reload everything every time.

Before loading, it checks the latest timestamp already present in MySQL and identifies records that are newer than the existing data.

This gives the project a basic incremental ETL pattern instead of a full reload on every run.

### 6. MySQL loading

The processed data is loaded into **MySQL** using SQLAlchemy.

I used **DBeaver** to inspect the database, tables, and loaded records.

### 7. Audit logging

I created an audit table to keep track of pipeline executions.

The audit information includes things such as:

* Run status
* Number of records processed
* Pipeline execution information

This makes it easier to understand what happened during each run.

### 8. Error handling and retries

Airflow retries tasks when temporary failures occur.

This is useful for situations where a task fails because of a temporary connection or infrastructure problem rather than a permanent data issue.

## 🐛 Problems I Ran Into

One issue I encountered was related to **branching in Airflow**.

When one branch is not selected, Airflow marks the tasks in that branch as `skipped`. Because the load task originally used the default `all_success` trigger rule, it could also be skipped when one of the branches was skipped.

I fixed this by using:

`none_failed_min_one_success`

This allows the load task to continue when at least one branch succeeds and the other branch is simply skipped.

I also ran into an issue with SQLAlchemy when using Pandas `to_sql()`.

The `dtype` argument needs an actual SQLAlchemy type rather than a string, so I changed the implementation to use `DateTime()`.

## 🧠 What I Learned

This project helped me understand how an ETL workflow is structured when using an orchestration tool rather than running everything from one Python script.

Some of the main things I practiced were:

* Building Airflow DAGs using the TaskFlow API
* Working with sensors and task dependencies
* Handling branching and trigger rules
* Performing data quality checks
* Designing incremental loads
* Loading data into MySQL
* Using Airflow Connections instead of putting credentials directly in code
* Debugging Airflow task failures
* Running the complete environment with Docker

## ⚠️ Current Limitations

There are still some things I would improve in a future version:

* The incremental load is not completely idempotent for exact duplicate timestamps.
* Both branch paths currently converge to similar loading logic.
* There is no CI/CD pipeline yet.
* There is no external alerting system.
* The project currently runs on a single-node setup.

These are areas I would address if I continued developing the project toward a more production-oriented implementation.

## 📌 Project Summary

This project is a practical implementation of an ETL workflow where **Apache Airflow handles orchestration**, **Python/Pandas handles transformation and validation**, and **MySQL stores the processed data**.

The main goal was not just to move data from CSV to MySQL, but to understand how scheduling, dependencies, validation, branching, incremental processing, logging, retries, and database loading fit together in a complete pipeline.
