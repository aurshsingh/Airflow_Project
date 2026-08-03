from airflow.sdk import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import logging

# ============================================================
# APACHE AIRFLOW - FILE SENSOR (Airflow 3.3)
# ============================================================
#
# WHAT IS A SENSOR?
#
# A Sensor is a special Airflow task that WAITS until
# a specific condition becomes TRUE.
#
# Examples:
#
# FileSensor
#   -> Wait until a file arrives.
#
# HttpSensor
#   -> Wait until an API returns HTTP 200.
#
# ExternalTaskSensor
#   -> Wait until another DAG finishes.
#
# SqlSensor
#   -> Wait until a SQL query returns expected data.
#
#
# WHY ARE SENSORS IMPORTANT?
#
# Real ETL pipelines rarely start at a fixed time.
#
# Example:
#
# Bank uploads today's transactions.
#             ↓
# Wait until file arrives
#             ↓
# Process file
#             ↓
# Load into Database
#
# Without sensors:
#
# Schedule every day at 6 AM.
# If file arrives at 6:15 AM
# → Pipeline fails.
#
# With FileSensor:
#
# Pipeline waits until file appears.
# More reliable.
#
# ============================================================

logger = logging.getLogger(__name__)

# ============================================================
# CREATE DAG
# ============================================================

@dag(

    # DAG name shown in Airflow UI
    dag_id="pipeline_with_sensor",

    # Must always be in the past
    start_date=datetime(2024, 1, 1),

    # Manual trigger only
    # Change to "@daily" if you want scheduled execution
    schedule=None,

    # Ignore historical runs
    catchup=False,

    tags=["sensor", "filesystem", "etl"]

)
def pipeline_with_sensor():

    # ========================================================
    # FILE SENSOR
    # ========================================================
    #
    # Waits until a file exists.
    #
    # It DOES NOT process the file.
    #
    # It ONLY checks:
    #
    # "Does this file exist yet?"
    #
    # If NO
    #
    # Wait...
    #
    # Check again...
    #
    # Wait...
    #
    # Check again...
    #
    # Until timeout.
    #
    # ========================================================

    wait_for_orders_file = FileSensor(

        # Task name shown in UI
        task_id="wait_for_orders_file",

        # Filesystem Connection
        #
        # Airflow looks inside Admin → Connections
        #
        # Connection ID:
        #
        # fs_default
        #
        # Base Path:
        #
        # /opt/airflow/dags/data
        #
        fs_conn_id="fs_default",

        # Relative path from Base Path
        #
        # Base Path:
        # /opt/airflow/dags/data
        #
        # filepath:
        # daily_orders.csv
        #
        # Final location checked:
        #
        # /opt/airflow/dags/data/daily_orders.csv
        #
        filepath="daily_orders.csv",

        # Check every 30 seconds
        poke_interval=30,

        # Maximum waiting time
        #
        # 600 seconds = 10 minutes
        #
        timeout=600,

        # VERY IMPORTANT
        #
        # poke
        #
        # Keeps Worker busy.
        #
        # reschedule
        #
        # Releases Worker while waiting.
        #
        # Recommended for long waits.
        #
        mode="reschedule",

        # False
        #
        # Timeout = FAIL DAG
        #
        # True
        #
        # Timeout = SKIP task
        #
        soft_fail=False,

    )

    # ========================================================
    # PROCESS FILE
    # ========================================================
    #
    # Runs ONLY AFTER sensor succeeds.
    #
    # If file never arrives
    #
    # This task NEVER runs.
    #
    # ========================================================

    @task
    def process_file():

        import pandas as pd

        logger.info("File detected.")

        # Read file created by DAG 1
        df = pd.read_csv(
            "/opt/airflow/dags/data/daily_orders.csv"
        )

        logger.info(f"Rows received : {len(df)}")

        # Return row count using XCom
        return len(df)

    # ========================================================
    # FINAL TASK
    # ========================================================
    #
    # Receives row count automatically
    # through Airflow XCom.
    #
    # ========================================================

    @task
    def completed(rows):

        logger.info(
            f"Successfully processed {rows} rows."
        )

    # ========================================================
    # TASKFLOW API
    # ========================================================
    #
    # process_file()
    #
    # Creates Task
    #
    # Also returns XCom reference.
    #
    # ========================================================

    rows = process_file()

    completed(rows)

    # ========================================================
    # TASK DEPENDENCIES
    # ========================================================
    #
    # Sensor
    #      ↓
    # Process File
    #      ↓
    # Complete
    #
    # ========================================================

    wait_for_orders_file >> rows


# ============================================================
# REGISTER DAG
# ============================================================
#
# Without this line Airflow will NOT discover the DAG.
#
# ============================================================

pipeline_with_sensor()