# ══════════════════════════════════════════════════════════════════════════════
# APACHE AIRFLOW 3.x
#
# PROJECT: Production Grade E-Commerce ETL Pipeline
# VERSION: 5 (Final Resume Version)
#
# PURPOSE:
# Simulate a production ETL pipeline similar to what companies build
# using Apache Airflow.
#
# DATA FLOW
#
#                    CSV Source
#                        │
#                        ▼
#                  FileSensor
#                        │
#                        ▼
#                    Extract
#                        │
#                        ▼
#               Data Cleaning
#                        │
#                        ▼
#             Data Quality Checks
#                        │
#                        ▼
#            Incremental Load Check
#                        │
#                        ▼
#                 Branch Decision
#                /               \
#        Large Dataset       Small Dataset
#              │                    │
#              ▼                    ▼
#      Optimized Loading     Standard Loading
#               \                 /
#                 ▼             ▼
#                 Load To MySQL
#                        │
#                        ▼
#                  Update Audit Log
#                        │
#                        ▼
#                 Pipeline Summary
#
# TECHNOLOGIES
# Apache Airflow, Docker, Python, Pandas, MySQL, SQLAlchemy
#
# ══════════════════════════════════════════════════════════════════════════════

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook
from sqlalchemy import DateTime

from datetime import datetime, timedelta

import pandas as pd
import logging

logger = logging.getLogger(__name__)


@dag(

    dag_id="ecommerce_etl_pipeline_v5",

    start_date=datetime(2024, 1, 1),

    schedule="@daily",

    catchup=False,

    default_args={

        "owner": "Aursh",

        "retries": 2,

        "retry_delay": timedelta(minutes=2)

    },

    tags=["etl", "production", "mysql", "airflow", "resume"]

)
def ecommerce_etl_pipeline_v5():

    # ==========================================================
    # SENSOR
    # ==========================================================
    #
    # Wait until the input file exists.
    #
    # Checks every 30 seconds (poke_interval).
    # If the file does not arrive within 10 minutes (timeout),
    # the task fails.
    #
    # mode="reschedule" releases the worker slot between checks
    # instead of holding it for the entire wait — important so
    # one waiting sensor doesn't tie up a worker doing nothing.
    #
    # WHY A SENSOR AT ALL?
    #
    # Production systems rarely know exactly when a file will
    # land (bank feed, vendor drop, SAP export, FTP, S3 upload).
    # Instead of blindly running on a fixed schedule and hoping
    # the file is there, the sensor waits until it actually is.
    #
    # ==========================================================

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
    # Read raw CSVs.
    #
    # In production this could instead be an API call, a
    # database query, or a read from S3/Snowflake. Kept as
    # local CSVs here so the pipeline LOGIC is the focus, not
    # the data source.
    #
    # ==========================================================

    @task
    def extract():

        logger.info("========== EXTRACT PHASE ==========")

        orders = pd.read_csv(
            "/opt/airflow/dags/data/olist_orders_dataset.csv"
        )

        customers = pd.read_csv(
            "/opt/airflow/dags/data/olist_customers_dataset.csv"
        )

        logger.info(f"Orders Loaded : {len(orders)}")
        logger.info(f"Customers Loaded : {len(customers)}")

        # Only metadata goes into XCom — never the DataFrame
        # itself. XCom is meant for small values (row counts,
        # paths, status flags), not bulk data.
        extraction_metadata = {

            "orders_rows": len(orders),

            "customers_rows": len(customers),

            "extract_time": str(datetime.now()),

            "status": "SUCCESS"

        }

        logger.info(extraction_metadata)

        return extraction_metadata

    # ==========================================================
    # TRANSFORM
    # ==========================================================
    #
    # PURPOSE
    #
    # Raw data is rarely ready for loading into a database.
    #
    # During transformation we:
    # • Convert datatypes
    # • Create new business columns
    # • Remove invalid records
    # • Standardize values
    # • Prepare clean data for validation
    #
    # Output: /tmp/orders_transformed.csv
    #
    # NOTE: transform() re-reads the CSV from disk instead of
    # receiving the DataFrame from extract(). This is intentional
    # — XCom isn't meant to carry large DataFrames between tasks,
    # so each task reloads what it needs from disk instead.
    #
    # ==========================================================

    @task
    def transform(metadata):

        logger.info("========== TRANSFORM PHASE ==========")
        logger.info(f"Received Metadata : {metadata}")

        orders = pd.read_csv(
            "/opt/airflow/dags/data/olist_orders_dataset.csv"
        )

        logger.info(f"Rows before transformation : {len(orders)}")

        # ------------------------------------------------------
        # Convert purchase timestamp to datetime
        #
        # Before:  "2017-08-10 10:30:00"   (plain string)
        # After:   datetime object
        #
        # Allows year/month/quarter extraction below.
        # ------------------------------------------------------

        orders["order_purchase_timestamp"] = pd.to_datetime(
            orders["order_purchase_timestamp"]
        )

        # ------------------------------------------------------
        # Feature Engineering: Year, Month
        #
        # Business teams frequently request reports like
        # "Orders by Month" or "Revenue by Year." Instead of
        # calculating this repeatedly downstream, we create
        # these columns once, here.
        # ------------------------------------------------------

        orders["year"] = orders["order_purchase_timestamp"].dt.year
        orders["month"] = orders["order_purchase_timestamp"].dt.month

        # ------------------------------------------------------
        # Order Quarter
        #
        # Example:  Jan → Q1,  Apr → Q2
        #
        # Useful for quarterly reporting.
        # ------------------------------------------------------

        orders["quarter"] = orders["order_purchase_timestamp"].dt.quarter

        # ------------------------------------------------------
        # Purchase Date — time portion stripped
        #
        # Example:  2018-03-10 11:45:20  →  2018-03-10
        # ------------------------------------------------------

        orders["purchase_date"] = orders["order_purchase_timestamp"].dt.date

        # ------------------------------------------------------
        # Remove rows where order approval is missing.
        #
        # These records are considered incomplete — an order
        # that was never approved isn't "clean" data for
        # downstream reporting.
        # ------------------------------------------------------

        rows_before = len(orders)

        orders_clean = orders.dropna(subset=["order_approved_at"])

        rows_after = len(orders_clean)
        rows_removed = rows_before - rows_after

        logger.info(f"Rows Removed : {rows_removed}")

        # ------------------------------------------------------
        # Standardize Order Status
        #
        # Example:  "Delivered", "DELIVERED", "delivered"
        # all become:  "delivered"
        #
        # Prevents the same real-world status from being
        # treated as multiple different values downstream.
        # ------------------------------------------------------

        orders_clean["order_status"] = (
            orders_clean["order_status"]
            .str.lower()
            .str.strip()
        )

        # ------------------------------------------------------
        # Remove duplicate Order IDs
        #
        # Production systems should never load duplicate
        # business keys — a duplicate order_id would corrupt
        # any downstream aggregation (order counts, revenue).
        # ------------------------------------------------------

        duplicate_count = (
            orders_clean
            .duplicated(subset=["order_id"])
            .sum()
        )

        logger.info(f"Duplicate Orders Found : {duplicate_count}")

        orders_clean = orders_clean.drop_duplicates(subset=["order_id"])

        # ------------------------------------------------------
        # Save transformed dataset — becomes input for Validation
        # ------------------------------------------------------

        output_path = "/tmp/orders_transformed.csv"

        orders_clean.to_csv(output_path, index=False)

        logger.info(f"Transformed File Saved : {output_path}")

        transform_metadata = {

            "rows_before": rows_before,

            "rows_after": len(orders_clean),

            "rows_removed": rows_removed,

            "duplicates_removed": int(duplicate_count),

            "output_path": output_path,

            "status": "SUCCESS"

        }

        logger.info(transform_metadata)

        return transform_metadata

    # ==========================================================
    # DATA VALIDATION
    # ==========================================================
    #
    # PURPOSE
    #
    # Never load bad data into production databases.
    #
    # This task checks:
    # • Dataset is not empty
    # • Required columns exist
    # • No duplicate Order IDs
    # • No null primary keys
    # • No future purchase dates
    # • Row retention percentage
    #
    # If any validation fails, the DAG stops immediately —
    # many production teams deliberately FAIL loudly here
    # rather than silently loading questionable data.
    #
    # ==========================================================

    @task
    def validate(transform_metadata):

        logger.info("========== VALIDATION PHASE ==========")

        df = pd.read_csv(transform_metadata["output_path"])

        logger.info(f"Rows Received : {len(df)}")

        # --------------------------------------------------
        # Validation 1: Dataset must not be empty
        # --------------------------------------------------

        if len(df) == 0:
            raise ValueError("Dataset is empty.")
        logger.info("Dataset contains records.")

        # --------------------------------------------------
        # Validation 2: Required Columns
        # --------------------------------------------------

        required_columns = [
            "order_id", "customer_id", "order_status",
            "year", "month", "quarter", "purchase_date"
        ]

        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Missing column : {column}")
        logger.info("All required columns exist.")

        # --------------------------------------------------
        # Validation 3: Duplicate Order IDs
        # --------------------------------------------------

        duplicate_orders = df["order_id"].duplicated().sum()

        if duplicate_orders > 0:
            raise ValueError(f"{duplicate_orders} duplicate orders found.")
        logger.info("No duplicate Order IDs.")

        # --------------------------------------------------
        # Validation 4: Null Primary Keys
        # --------------------------------------------------

        if df["order_id"].isnull().sum() > 0:
            raise ValueError("Null Order IDs found.")
        logger.info("Primary Key validation passed.")

        # --------------------------------------------------
        # Validation 5: Future Purchase Dates
        #
        # A purchase dated in the future almost always means
        # a data quality issue upstream (bad timestamp, wrong
        # timezone, test data leaking in) — not a real order.
        # --------------------------------------------------

        df["purchase_date"] = pd.to_datetime(df["purchase_date"])

        future_rows = (df["purchase_date"] > pd.Timestamp.today()).sum()

        if future_rows > 0:
            raise ValueError("Future purchase dates detected.")
        logger.info("No future dates.")

        # --------------------------------------------------
        # Validation 6: Row Retention
        #
        # If transform() dropped too many rows (more than 20%
        # here), that's a signal something upstream broke —
        # better to stop and investigate than silently load
        # a partial dataset.
        # --------------------------------------------------

        retention = (
            transform_metadata["rows_after"]
            /
            transform_metadata["rows_before"]
        )

        logger.info(f"Retention : {retention:.2%}")

        if retention < 0.80:
            raise ValueError("Too many rows removed.")
        logger.info("Retention validation passed.")

        validation_result = {

            "validated": True,

            "row_count": len(df),

            "output_path": transform_metadata["output_path"],

            "status": "SUCCESS"

        }

        logger.info(validation_result)

        return validation_result

    # ==========================================================
    # INCREMENTAL LOAD CHECK
    # ==========================================================
    #
    # PURPOSE
    #
    # Production pipelines should not reload the entire dataset
    # every day. Instead, load only NEW records.
    #
    # Example:
    #   Yesterday : 100,000 rows already in MySQL
    #   Today     : 100,050 rows in the source file
    #   Load only : the 50 new rows
    #
    # HOW: query MAX(order_purchase_timestamp) already loaded
    # into MySQL, then keep only rows newer than that.
    #
    # WHY except Exception IS DELIBERATELY BROAD:
    # On the very first run, the orders_clean_v5 table doesn't
    # exist yet, so the SELECT MAX(...) query raises an error.
    # We catch that and treat it as "first run → full load."
    # Tradeoff to be aware of: this also swallows a genuine DB
    # connection failure and misreads it as "first run." A
    # stricter version would catch the specific SQLAlchemy
    # "table not found" error instead of Exception broadly.
    #
    # ==========================================================

    @task
    def incremental_check(validation_result):

        logger.info("========== INCREMENTAL CHECK ==========")

        df = pd.read_csv(validation_result["output_path"])

        hook = MySqlHook(mysql_conn_id="mysql_default")

        engine = hook.get_sqlalchemy_engine()

        try:

            existing = pd.read_sql(
                """
                SELECT MAX(order_purchase_timestamp) AS latest_date
                FROM orders_clean_v5
                """,
                engine
            )

            latest_date = existing.iloc[0]["latest_date"]

            logger.info(f"Latest Loaded Date : {latest_date}")

            if latest_date is not None:

                df["order_purchase_timestamp"] = pd.to_datetime(
                    df["order_purchase_timestamp"]
                )

                # Keep only rows strictly newer than what's
                # already loaded.
                df = df[
                    df["order_purchase_timestamp"] > pd.to_datetime(latest_date)
                ]

        except Exception:

            logger.info("orders_clean_v5 table not found.")
            logger.info("Performing first full load.")

        incremental_path = "/tmp/orders_incremental.csv"

        df.to_csv(incremental_path, index=False)

        logger.info(f"Rows To Load : {len(df)}")

        return {

            "rows_to_load": len(df),

            "output_path": incremental_path,

            "status": "SUCCESS"

        }

    # ==========================================================
    # BRANCH DECISION
    # ==========================================================
    #
    # PURPOSE
    #
    # Large datasets may warrant a different processing path.
    #
    # Example:
    #   5,000 rows    → normal processing
    #   500,000 rows  → optimized processing
    #
    # @task.branch must return the task_id (a string) of
    # whichever downstream task should run next. Airflow
    # automatically SKIPS the task_id that was NOT returned.
    #
    # ==========================================================

    @task.branch
    def choose_processing(incremental_result):

        rows = incremental_result["rows_to_load"]

        logger.info(f"Rows to Load : {rows}")

        if rows > 100000:
            logger.info("Large Dataset Detected")
            return "large_dataset_processing"
        else:
            logger.info("Standard Dataset")
            return "standard_processing"

    # ------------------------------------------------------
    # These are placeholders for real optimized-vs-standard
    # logic (e.g. different chunksize, parallel writes, bulk
    # LOAD DATA INFILE for large datasets). Kept simple here
    # since the Olist dataset won't actually cross the
    # 100,000-row threshold — the branch mainly demonstrates
    # conditional workflow logic.
    # ------------------------------------------------------

    @task
    def large_dataset_processing():

        logger.info("========== LARGE DATASET ==========")
        logger.info("Using optimized loading strategy.")

        return "large_processing_completed"

    @task
    def standard_processing():

        logger.info("========== STANDARD DATASET ==========")
        logger.info("Using standard loading.")

        return "standard_processing_completed"

    # ==========================================================
    # LOAD
    # ==========================================================
    #
    # PURPOSE
    #
    # Load transformed, incremental records into MySQL via the
    # mysql_default Airflow Connection (credentials never live
    # in this file).
    #
    # WHY trigger_rule="none_failed_min_one_success" HERE:
    #
    # load() sits right after the branch fans back in (both
    # large_dataset_processing and standard_processing point
    # into it). Because branching SKIPS the path not chosen,
    # load()'s default trigger rule (all_success) would see one
    # skipped upstream task and skip itself too.
    # "none_failed_min_one_success" tells Airflow: "run me as
    # long as nothing failed and at least one upstream branch
    # actually succeeded" — exactly what's needed at a branch
    # fan-in point.
    #
    # WHY if_exists="append" (not "replace"):
    # This is incremental loading — we only want to ADD the new
    # rows computed in incremental_check(), not wipe and
    # reload the whole table.
    #
    # WHY dtype={"order_purchase_timestamp": "DATETIME"}:
    # Without this, pandas/SQLAlchemy may infer this column as
    # VARCHAR instead of a real MySQL DATETIME. That would break
    # incremental_check()'s MAX(...) and > comparisons, which
    # rely on chronological (not string/lexicographic) ordering.
    #
    # ==========================================================

    @task(trigger_rule="none_failed_min_one_success")
    def load(incremental_result):

        logger.info("========== LOAD ==========")

        df = pd.read_csv(incremental_result["output_path"])

        if len(df) == 0:

            logger.info("No new records to load.")

            return {

                "rows_loaded": 0,

                "status": "NO DATA"

            }

        hook = MySqlHook(mysql_conn_id="mysql_default")

        engine = hook.get_sqlalchemy_engine()

        df.to_sql(

            name="orders_clean_v5",

            con=engine,

            if_exists="append",

            index=False,

            chunksize=5000,

            dtype={"order_purchase_timestamp": DateTime()}

        )

        logger.info(f"{len(df)} rows loaded.")

        return {

            "rows_loaded": len(df),

            "status": "SUCCESS",

            "destination": "orders_clean_v5"

        }

    # ==========================================================
    # AUDIT TABLE
    # ==========================================================
    #
    # PURPOSE
    #
    # Every production pipeline should leave a trail: when did
    # it run, how many rows landed, did it succeed. This is
    # what you'd query first when someone asks "did yesterday's
    # load actually happen" — a lightweight version of the
    # observability real orchestration systems provide.
    #
    # ==========================================================

    @task
    def update_audit(load_result):

        logger.info("========== AUDIT ==========")

        hook = MySqlHook(mysql_conn_id="mysql_default")

        engine = hook.get_sqlalchemy_engine()

        audit = pd.DataFrame({

            "run_time": [datetime.now()],

            "rows_loaded": [load_result["rows_loaded"]],

            "status": [load_result["status"]]

        })

        audit.to_sql(

            "etl_audit_v5",

            con=engine,

            if_exists="append",

            index=False

        )

        logger.info("Audit table updated.")

        return {"audit": "SUCCESS"}

    # ==========================================================
    # PIPELINE SUMMARY
    # ==========================================================
    #
    # PURPOSE
    #
    # Final task of the pipeline — the "did it actually work,
    # and what did it do" summary you'd check first thing every
    # morning.
    #
    # ==========================================================

    @task
    def pipeline_summary(load_result):

        logger.info("=========================================")
        logger.info("E-COMMERCE ETL PIPELINE COMPLETED")
        logger.info("=========================================")

        logger.info(f"Rows Loaded : {load_result['rows_loaded']}")
        logger.info(f"Pipeline Status : {load_result['status']}")
        logger.info(f"Completed At : {datetime.now()}")

        logger.info("=========================================")

        return "SUCCESS"

    # ==========================================================
    # TASK DEPENDENCIES
    # ==========================================================
    #
    # Two dependency styles in play:
    #
    # 1) extracted = extract() — TaskFlow calls, where the
    #    return value of one task is passed directly as the
    #    argument to the next (data dependency + task
    #    dependency in one line).
    #
    # 2) wait_for_orders_file >> extracted — the >> operator,
    #    used for the FileSensor (a classic operator, not a
    #    TaskFlow @task) since it has no return value to pass.
    #
    # branch >> large >> loaded
    # branch >> standard >> loaded
    #    This is the fan-out/fan-in: branch picks ONE of the
    #    two paths to actually run (the other is skipped), and
    #    both paths converge back into the SAME loaded task.
    #
    # ==========================================================

    extracted = extract()

    transformed = transform(extracted)

    validated = validate(transformed)

    incremental = incremental_check(validated)

    branch = choose_processing(incremental)

    large = large_dataset_processing()

    standard = standard_processing()

    loaded = load(incremental)

    audit = update_audit(loaded)

    summary = pipeline_summary(loaded)

    wait_for_orders_file >> extracted

    extracted >> transformed
    transformed >> validated
    validated >> incremental
    incremental >> branch

    branch >> large >> loaded
    branch >> standard >> loaded

    loaded >> audit
    loaded >> summary


ecommerce_etl_pipeline_v5()