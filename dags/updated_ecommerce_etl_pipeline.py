# ═══════════════════════════════════════════════════════════════
# APACHE AIRFLOW 3.x
# PROJECT: PRODUCTION-STYLE E-COMMERCE ETL PIPELINE
# DAG ID: ecommerce_etl_pipeline_mysql
# VERSION 3: LOADS INTO MYSQL (VIEWABLE IN DBEAVER)
# ═══════════════════════════════════════════════════════════════
#
# WHAT CHANGED FROM VERSION 2?
#
# Before: load() wrote a CSV to /tmp/final_orders.csv
# Now:    load() writes rows into a MySQL table using an
#         Airflow Connection (mysql_default) — no credentials
#         are hardcoded anywhere in this file.
#
# WHY MySqlHook INSTEAD OF A RAW pymysql CONNECTION?
#
# MySqlHook is Airflow's wrapper around the Connection you set
# up in the UI/CLI. It reads mysql_default's host/user/password
# from Airflow's encrypted metadata database, so this DAG file
# stays credential-free and portable — you could commit it to
# GitHub safely.
#
# ═══════════════════════════════════════════════════════════════

from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta

import pandas as pd
import logging


logger = logging.getLogger(__name__)


@dag(

    dag_id="ecommerce_etl_pipeline_mysql",

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
        "production",
        "mysql"
    ]

)
def ecommerce_etl_pipeline_mysql():

    # ==========================================================
    # SENSOR  (unchanged)
    # ==========================================================

    wait_for_orders_file = FileSensor(

        task_id="wait_for_orders_file",

        filepath="/opt/airflow/dags/data/olist_orders_dataset.csv",

        poke_interval=30,

        timeout=600,

        mode="reschedule"

    )

    # ==========================================================
    # EXTRACT  (unchanged)
    # ==========================================================

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

        return metadata

    # ==========================================================
    # TRANSFORM  (unchanged)
    # ==========================================================

    @task
    def transform(metadata):

        logger.info(
            f"Input metadata: {metadata}"
        )

        orders = pd.read_csv(
            "/opt/airflow/dags/data/olist_orders_dataset.csv"
        )

        orders["order_purchase_timestamp"] = pd.to_datetime(

            orders["order_purchase_timestamp"]

        )

        orders["year"] = (

            orders["order_purchase_timestamp"]

            .dt.year

        )

        orders["month"] = (

            orders["order_purchase_timestamp"]

            .dt.month

        )

        rows_before = len(orders)

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
    # VALIDATE  (unchanged)
    # ==========================================================

    @task
    def validate(transform_info):

        logger.info("Starting data validation")

        df = pd.read_csv(
            transform_info["output_path"]
        )

        assert len(df) > 0, "Dataset is empty"

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

        duplicate_orders = (
            df["order_id"]
            .duplicated()
            .sum()
        )
        assert duplicate_orders == 0, (
            f"{duplicate_orders} duplicate orders found"
        )

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
    # BRANCHING  (unchanged)
    # ==========================================================

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

    @task
    def large_dataset_processing():

        logger.info("Running optimized processing")

        return "large_processing_complete"

    @task
    def standard_processing():

        logger.info("Running standard processing")

        return "standard_processing_complete"

    # ==========================================================
    # LOAD  →  NOW WRITES TO MYSQL
    # ==========================================================
    #
    # WHAT'S DIFFERENT HERE:
    #
    # 1. MySqlHook(mysql_conn_id="mysql_default") looks up the
    #    Connection you created in Step 3. No host/user/password
    #    live in this file.
    #
    # 2. hook.get_sqlalchemy_engine() gives pandas a SQLAlchemy
    #    engine, which is what df.to_sql() needs to write rows.
    #
    # 3. if_exists="replace" drops and recreates the table each
    #    run — fine for a daily learning pipeline. In a real
    #    production system with true incremental loads you'd use
    #    if_exists="append" plus a dedupe/upsert strategy instead
    #    (that's Version 5 territory — incremental loading).
    #
    # 4. trigger_rule stays "none_failed_min_one_success" because
    #    load() is still the fan-in point right after the branch.
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

        hook = MySqlHook(mysql_conn_id="mysql_default")

        engine = hook.get_sqlalchemy_engine()

        df.to_sql(

            name="orders_clean",

            con=engine,

            if_exists="replace",

            index=False,

            chunksize=1000

        )

        logger.info(

            f"Loaded {len(df)} rows into MySQL table 'orders_clean'"

        )

        return {

            "rows_loaded": len(df),

            "destination": "mysql://ecommerce_db/orders_clean"

        }

    # ==========================================================
    # PIPELINE SUMMARY  (unchanged)
    # ==========================================================

    @task
    def pipeline_summary(load_result):

        logger.info("===================================")

        logger.info("PIPELINE COMPLETED SUCCESSFULLY")

        logger.info(
            f"Rows Loaded : {load_result['rows_loaded']}"
        )

        logger.info(
            f"Destination : {load_result['destination']}"
        )

        logger.info("===================================")

        return "SUCCESS"

    # ==========================================================
    # TASK DEPENDENCIES  (unchanged)
    # ==========================================================

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


ecommerce_etl_pipeline_mysql()