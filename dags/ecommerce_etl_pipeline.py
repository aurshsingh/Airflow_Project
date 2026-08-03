# ═══════════════════════════════════════════════════════════════
# APACHE AIRFLOW 3.x
# PROJECT: PRODUCTION-STYLE E-COMMERCE ETL PIPELINE
# DAG ID: ecommerce_etl_pipeline_csv
# ═══════════════════════════════════════════════════════════════
#
# WHAT IS THIS PROJECT?
#
# This project demonstrates how a production ETL pipeline works
# using Apache Airflow.
#
# ETL = Extract, Transform, Load
#
# Instead of running one huge Python script,
# Airflow breaks the work into individual tasks,
# each with its own retries, logging, and history.
#
#
# FULL PIPELINE FLOW
#
#              Orders CSV
#                   │
#                   ▼
#             FileSensor
#                   │
#                   ▼
#               Extract
#                   │
#                   ▼
#              Transform
#                   │
#                   ▼
#              Validate
#                   │
#                   ▼
#               Branch
#                   │
#          ┌────────┴────────┐
#          ▼                 ▼
#      Large Load      Standard Load
#          │                 │
#          └────────┬────────┘
#                   ▼
#                 Load
#                   │
#                   ▼
#              Summary
#
#
# WHY AIRFLOW?
#
# Airflow manages: Scheduling, Dependencies, Retries,
# Monitoring, Logging, Workflow execution.
#
#
# WHY FILE SENSOR?
#
# Production systems rarely know exactly when a file will arrive
# (bank feed, vendor drop, SAP export, FTP, S3 upload).
# Instead of blindly running every day at 6 AM and hoping the file
# is there, the sensor waits until the file actually exists.
#
#
# TASKFLOW API
#
# @dag creates the DAG.
# @task turns a normal Python function into an Airflow task,
# and handles passing return values between tasks automatically.
#
#
# XCOM
#
# Tasks communicate using XCom (Airflow's small internal
# message-passing system). Never pass full DataFrames through it —
# XCom is meant for small values. Pass only:
# • row counts
# • file paths
# • metadata dictionaries
#
# ═══════════════════════════════════════════════════════════════

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor

from datetime import datetime, timedelta

import pandas as pd
import logging


logger = logging.getLogger(__name__)


@dag(

    dag_id="ecommerce_etl_pipeline_csv",

    start_date=datetime(2024, 1, 1),

    schedule="@daily",

    catchup=False,

    default_args={

        "retries": 2,

        "retry_delay": timedelta(minutes=2)

    },

    tags=[
        "ecommerce",
        "etl",
        "production"
    ]

)
def ecommerce_etl_pipeline_csv():

    # ==========================================================
    # SENSOR
    # ==========================================================
    #
    # Wait until the input file exists.
    # Checks every 30 seconds (poke_interval).
    # If the file does not arrive within 10 minutes (timeout),
    # the task fails.
    #
    # mode="reschedule" frees up the worker slot between checks
    # instead of blocking it — important so one waiting sensor
    # doesn't tie up a worker for 10 minutes doing nothing.
    #

    wait_for_orders_file = FileSensor(

        task_id="wait_for_orders_file",

        filepath="/opt/airflow/dags/data/olist_orders_dataset.csv",

        poke_interval=30,

        timeout=600,

        mode="reschedule"

    )

    # ==========================================================
    # EXTRACT
    # ==========================================================
    #
    # Read raw CSV.
    #
    # In production this could instead be reading from:
    # • an API
    # • MySQL / PostgreSQL
    # • Snowflake
    # • AWS S3
    #
    # For this project we keep it simple with local CSVs so the
    # pipeline logic itself is the focus, not the data source.
    #

    @task
    def extract():

        logger.info("Starting extraction")

        orders = pd.read_csv(
            "/opt/airflow/dags/data/olist_orders_dataset.csv"
        )

        customers = pd.read_csv(
            "/opt/airflow/dags/data/olist_customers_dataset.csv"
        )

        metadata = {

            "orders_count": len(orders),

            "customers_count": len(customers),

            "source": "CSV"

        }

        logger.info(metadata)

        # Only metadata is returned (not the DataFrame itself) —
        # this is what gets stored in XCom.
        return metadata

    # ==========================================================
    # TRANSFORM
    # ==========================================================
    #
    # Clean data, convert dates, create new columns,
    # drop rows that are unusable, and save the result to disk.
    #
    # Note: transform() re-reads the CSV from disk rather than
    # receiving the DataFrame from extract(). This is intentional —
    # XCom is not meant to carry large DataFrames between tasks,
    # so each task reloads what it needs from disk/storage instead.
    #

    @task
    def transform(metadata):

        logger.info(
            f"Input metadata: {metadata}"
        )

        orders = pd.read_csv(
            "/opt/airflow/dags/data/olist_orders_dataset.csv"
        )

        # Convert purchase date from string to real datetime
        orders["order_purchase_timestamp"] = pd.to_datetime(

            orders["order_purchase_timestamp"]

        )

        # Create Year column (useful for later analytics/BI)
        orders["year"] = (

            orders["order_purchase_timestamp"]

            .dt.year

        )

        # Create Month column
        orders["month"] = (

            orders["order_purchase_timestamp"]

            .dt.month

        )

        rows_before = len(orders)

        # Remove records where approval date is missing —
        # an order that was never approved isn't a "clean" order
        # for downstream reporting.
        orders_clean = orders.dropna(

            subset=["order_approved_at"]

        )

        rows_after = len(orders_clean)

        output_path = "/tmp/orders_transformed.csv"

        orders_clean.to_csv(

            output_path,

            index=False

        )

        transform_info = {

            "rows_before": rows_before,

            "rows_after": rows_after,

            "rows_removed": rows_before - rows_after,

            "output_path": output_path

        }

        logger.info(transform_info)

        return transform_info

    # ==========================================================
    # VALIDATE
    # ==========================================================
    #
    # Never load bad data.
    #
    # Data Quality Checks:
    # ✓ Empty dataset
    # ✓ Required columns present
    # ✓ No duplicate Order IDs
    # ✓ Row retention (didn't lose too much data in transform)
    #
    # In production, many companies deliberately FAIL the pipeline
    # here (rather than silently loading bad data) if quality
    # checks don't pass — a loud failure is safer than a quiet
    # bad load that corrupts downstream reports.
    #

    @task
    def validate(transform_info):

        logger.info("Starting data validation")

        df = pd.read_csv(
            transform_info["output_path"]
        )

        # -------------------------------
        # Check 1: Dataset should not be empty
        # -------------------------------
        assert len(df) > 0, "Dataset is empty"

        # -------------------------------
        # Check 2: Required columns must exist
        # -------------------------------
        required_columns = [
            "order_id",
            "customer_id",
            "order_status",
            "year",
            "month"
        ]
        for column in required_columns:
            assert column in df.columns, (
                f"Missing column : {column}"
            )

        # -------------------------------
        # Check 3: No duplicate Order IDs
        # -------------------------------
        duplicate_orders = (
            df["order_id"]
            .duplicated()
            .sum()
        )
        assert duplicate_orders == 0, (
            f"{duplicate_orders} duplicate orders found"
        )

        # -------------------------------
        # Check 4: Retention % — did transform() drop too much data?
        # If we lose more than 20% of rows, something is likely
        # wrong upstream and needs investigating, not auto-loading.
        # -------------------------------
        retention = (
            transform_info["rows_after"]
            /
            transform_info["rows_before"]
        )
        assert retention >= 0.80, (
            f"Retention only {retention:.2%}"
        )

        validation_result = {

            "validated": True,

            "row_count": len(df),

            "output_path": transform_info["output_path"]

        }

        logger.info(validation_result)

        return validation_result

    # ==========================================================
    # BRANCHING
    # ==========================================================
    #
    # Production pipelines often choose different processing
    # paths depending on data size.
    #
    # Small dataset  → normal/standard processing
    # Large dataset  → optimized processing (e.g. chunked writes,
    #                  parallel loading, different compute)
    #
    # @task.branch must return the task_id (a string) of whichever
    # downstream task should run next. Airflow will automatically
    # SKIP the task_id that was not returned.
    #

    @task.branch
    def choose_processing(validation_result):

        rows = validation_result["row_count"]

        logger.info(
            f"Rows after validation : {rows}"
        )

        if rows > 100000:
            logger.info("Large dataset detected")
            return "large_dataset_processing"
        else:
            logger.info("Standard dataset detected")
            return "standard_processing"

    # ==========================================================
    # LARGE DATASET PROCESSING
    # ==========================================================
    #
    # Placeholder for optimized logic (e.g. chunked reads,
    # parallel processing) — kept simple here since the Olist
    # dataset won't actually cross the 100k-row threshold.
    #

    @task
    def large_dataset_processing():

        logger.info("Running optimized processing")

        return "large_processing_complete"

    # ==========================================================
    # STANDARD DATASET PROCESSING
    # ==========================================================

    @task
    def standard_processing():

        logger.info("Running standard processing")

        return "standard_processing_complete"

    # ==========================================================
    # LOAD
    # ==========================================================
    #
    # WHAT IS LOAD?
    #
    # Load is the final ETL stage — the cleaned and validated
    # data is written to its destination.
    #
    # Production examples: PostgreSQL, MySQL, Snowflake,
    # BigQuery, Redshift.
    #
    # For now we write a final CSV. Later this task will load
    # into MySQL instead.
    #
    # WHY trigger_rule="none_failed_min_one_success" HERE?
    #
    # load() sits right after the branch fans back in
    # (both large_dataset_processing and standard_processing
    # point into it). Because branching SKIPS the path not
    # chosen, load()'s default trigger rule (all_success) would
    # see one skipped upstream task and skip itself too.
    # "none_failed_min_one_success" tells Airflow:
    # "run me as long as nothing failed and at least one
    # upstream branch actually succeeded" — which is exactly
    # the behavior we want at a branch fan-in point.
    #

    @task(trigger_rule="none_failed_min_one_success")
    def load(validation_result):

        logger.info("Starting LOAD phase")

        if not validation_result["validated"]:

            raise ValueError(

                "Validation failed."

            )

        df = pd.read_csv(

            validation_result["output_path"]

        )

        final_output = "/tmp/final_orders.csv"

        df.to_csv(

            final_output,

            index=False

        )

        logger.info(

            f"Final dataset saved : {final_output}"

        )

        return {

            "rows_loaded": len(df),

            "destination": final_output

        }

    # ==========================================================
    # PIPELINE SUMMARY
    # ==========================================================
    #
    # Every production pipeline should clearly log what happened —
    # this is the "did it actually work, and what did it do"
    # task that you'd check first thing every morning.
    #

    @task
    def pipeline_summary(load_result):

        logger.info("===================================")

        logger.info("PIPELINE COMPLETED SUCCESSFULLY")

        logger.info(
            f"Rows Loaded : {load_result['rows_loaded']}"
        )

        logger.info(
            f"Output File : {load_result['destination']}"
        )

        logger.info("===================================")

        return "SUCCESS"

    # ==========================================================
    # TASK DEPENDENCIES
    # ==========================================================
    #
    # Note the difference between the two dependency styles here:
    #
    # 1) extracted = extract() — TaskFlow calls, where the
    #    return value of one task is passed directly as the
    #    argument to the next (data dependency + task dependency
    #    in one line).
    #
    # 2) wait_for_orders_file >> extracted — the >> operator,
    #    used for the FileSensor (a classic operator, not a
    #    TaskFlow @task) since it has no return value to pass.
    #
    # branch >> large >> loaded
    # branch >> standard >> loaded
    #    This is the fan-out/fan-in: branch picks ONE of the two
    #    paths to actually run (the other gets skipped), and both
    #    paths converge back into loaded.
    #

    extracted = extract()
    transformed = transform(extracted)
    validated = validate(transformed)
    branch = choose_processing(validated)
    large = large_dataset_processing()
    standard = standard_processing()
    loaded = load(validated)
    summary = pipeline_summary(loaded)

    wait_for_orders_file >> extracted

    extracted >> transformed >> validated >> branch

    branch >> large >> loaded
    branch >> standard >> loaded

    loaded >> summary


ecommerce_etl_pipeline_csv()