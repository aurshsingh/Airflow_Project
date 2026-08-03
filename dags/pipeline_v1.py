# ════════════════════════════════════════════════════════════
# APACHE AIRFLOW — CORE CONCEPTS (Airflow 3.3)
# ════════════════════════════════════════════════════════════
#
# WHAT IS AIRFLOW?
# A tool to schedule, monitor, and manage data workflows
# Like a smart cron job that can handle dependencies between tasks
# Shows you what ran, what failed, what is running right now
#
# WHAT IS A DAG?
# Directed Acyclic Graph
# Directed = tasks flow one direction (extract → transform → load)
# Acyclic = no circular dependencies (task A cannot depend on itself)
# Graph = multiple tasks connected
#
# WHAT IS TASKFLOW API?
# The way to write Airflow DAGs using Python decorators
# @dag and @task — this has been the primary way since Airflow 2.0
# and remains the primary way in Airflow 3.x
#
# WHAT IS XCOM?
# Cross Communication — how tasks share data
# When a @task function returns a value, Airflow stores it
# The next task receives it as a parameter automatically
# Important: only use XCom for small data (counts, file paths, metadata)
# Never put a full DataFrame in XCom — it will break or be very slow

from airflow.sdk import dag, task
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# @dag decorator turns this Python function into an Airflow DAG
@dag(
    # Unique ID for this DAG in the Airflow UI
    dag_id='ecommerce_pipeline_v1',
    
    # When does the first run happen?
    # Must be in the past for Airflow to schedule it
    start_date=datetime(2024, 1, 1),
    
    # How often to run?
    # In Airflow 3.x this is called 'schedule' (not 'schedule_interval')
    # @daily = once per day at midnight
    # @hourly = once per hour
    # '0 6 * * *' = every day at 6 AM (cron syntax)
    schedule='@daily',
    
    # catchup=False = do not run for all past dates
    # If True and start_date is 1 year ago, it would try to run 365 times
    catchup=False,
    
    # Default settings applied to all tasks in this DAG
    default_args={
        'retries': 2,                           # Retry failed tasks 2 times
        'retry_delay': timedelta(minutes=5),    # Wait 5 minutes between retries
    },
    
    # Tags help organise DAGs in the UI
    tags=['ecommerce', 'etl', 'daily']
)
def ecommerce_pipeline():
    """
    Daily E-Commerce Data Pipeline
    Flow: extract → transform → validate → load
    This docstring appears in the Airflow UI as the DAG description.
    """
    
    # @task decorator turns this function into an Airflow Task
    # The function name becomes the task_id in the UI
    @task
    def extract():
        """
        EXTRACT: Load raw data from source
        In production: reads from S3, SFTP, API, database
        Here: reads from local CSV files
        Returns a dict (sent to next task via XCom)
        """
        logger.info("Starting extraction phase")
        
        orders = pd.read_csv("/opt/airflow/dags/data/olist_orders_dataset.csv")
        customers = pd.read_csv("/opt/airflow/dags/data/olist_customers_dataset.csv")
        
        # Return metadata only (not the full DataFrame)
        result = {
            'orders_count': len(orders),
            'customers_count': len(customers),
            'source': 'local_csv'
        }
        
        logger.info(f"Extraction complete: {result}")
        return result  # This value goes to XCom automatically
    
    @task
    def transform(extracted_info):
        """
        TRANSFORM: Clean and prepare data
        extracted_info is automatically received from extract() via XCom
        """
        logger.info(f"Starting transform. Input info: {extracted_info}")
        
        orders = pd.read_csv("/opt/airflow/dags/data/olist_orders_dataset.csv")
        
        orders['order_purchase_timestamp'] = pd.to_datetime(
            orders['order_purchase_timestamp']
        )
        orders['year'] = orders['order_purchase_timestamp'].dt.year
        orders['month'] = orders['order_purchase_timestamp'].dt.month
        
        rows_before = len(orders)
        orders_clean = orders.dropna(subset=['order_approved_at'])
        rows_after = len(orders_clean)
        
        output_path = "/opt/airflow/dags/data/daily_orders.csv"
        orders_clean.to_csv(output_path, index=False)
        
        result = {
            'rows_before': rows_before,
            'rows_after': rows_after,
            'rows_removed': rows_before - rows_after,
            'output_path': output_path
        }
        
        logger.info(f"Transform complete: {result}")
        return result
    
    @task
    def validate(transform_info):
        """
        VALIDATE: Check data quality before loading
        """
        logger.info(f"Starting validation: {transform_info}")
        
        df = pd.read_csv(transform_info['output_path'])
        
        assert len(df) > 0, "VALIDATION FAILED: Dataset is empty!"
        
        required_columns = ['order_id', 'customer_id', 'order_status', 'year', 'month']
        for col in required_columns:
            assert col in df.columns, f"VALIDATION FAILED: Missing column: {col}"
        
        duplicate_count = df['order_id'].duplicated().sum()
        assert duplicate_count == 0, f"VALIDATION FAILED: {duplicate_count} duplicate order IDs!"
        
        retention = transform_info['rows_after'] / transform_info['rows_before']
        assert retention >= 0.80, f"VALIDATION FAILED: Only {retention:.0%} rows retained!"
        
        logger.info(f"All validation checks PASSED. Rows: {len(df)}")
        
        return {
            'validated': True,
            'row_count': len(df),
            'output_path': transform_info['output_path']
        }
    
    @task
    def load(validation_result):
        """
        LOAD: Write clean validated data to destination
        """
        if not validation_result['validated']:
            raise ValueError("Cannot load — validation did not pass")
        
        logger.info(f"Loading {validation_result['row_count']} validated rows")
        
        import os
        file_exists = os.path.exists(validation_result['output_path'])
        
        if file_exists:
            logger.info("Load complete. Data ready for consumption.")
        else:
            raise FileNotFoundError("Output file missing!")
        
        return "pipeline_success"
    
    # ── CONNECT THE TASKS ─────────────────────────────────────
    # This defines the execution order
    # Each function call creates the task AND passes XCom data automatically
    extracted = extract()
    transformed = transform(extracted)
    validated = validate(transformed)
    load(validated)

# ── REGISTER THE DAG ─────────────────────────────────────────
# This line creates the actual DAG object
# Without this, Airflow does not pick up the DAG
ecommerce_pipeline()