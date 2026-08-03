# ════════════════════════════════════════════════════════════
# APACHE AIRFLOW — BRANCHING (Airflow 3.x)
# ════════════════════════════════════════════════════════════
#
# WHAT IS BRANCHING?
# Branching allows a DAG to choose different paths
# based on a condition.
#
# Example:
# If data is large:
#       run optimized processing
#
# If data is small:
#       run normal processing
#
# Only the selected branch runs.
# Other branches become SKIPPED.
#
# Real examples:
# - Large file -> distributed processing
# - Small file -> normal processing
# - Monday -> weekly report
# - Failed validation -> stop pipeline
#
#
# WHAT IS @task.branch?
#
# @task.branch creates a branching task.
#
# The function must return the task_id
# of the next task to execute.
#
# Example:
#
# return "large_dataset_processing"
#
# Airflow will run:
#
# large_dataset_processing ✅
#
# and skip:
#
# weekly_full_report       skipped
# standard_processing      skipped
#
#
# WHAT IS TRIGGER_RULE?
#
# Normally Airflow uses:
#
# all_success
#
# Meaning:
# Run only when ALL previous tasks succeed.
#
# But branching creates skipped tasks.
#
# Example:
#
# weekly_report       SKIPPED
# large_processing    SUCCESS
# standard_processing SKIPPED
#
# Therefore notification task uses:
#
# none_failed_min_one_success
#
# Meaning:
# Run when:
# - At least one upstream task succeeds
# - No upstream task failed
#
# ════════════════════════════════════════════════════════════


from airflow.sdk import dag, task
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


@dag(
    dag_id="branching_pipeline",
    
    # First execution date
    start_date=datetime(2024, 1, 1),
    
    # Runs every day
    schedule="@daily",
    
    # Do not run missed historical dates
    catchup=False,
    
    tags=["ecommerce", "branching"]
)
def branching_pipeline():


    @task
    def check_data():
        """
        Simulates checking incoming data.
        
        In real projects:
        - Count rows from database
        - Check file size
        - Check data quality
        """

        orders_count = 95000

        logger.info(
            f"Found {orders_count} orders"
        )

        # This value goes through XCom
        return orders_count



    @task.branch
    def choose_processing_path(orders_count):
        """
        Decide which branch should run.

        Returns:
        task_id of the next task.
        """

        today = datetime.now().weekday()


        # Monday = 0
        if today == 0:

            logger.info(
                "Monday detected - weekly report"
            )

            return "weekly_full_report"


        elif orders_count > 50000:

            logger.info(
                "Large dataset - optimized processing"
            )

            return "large_dataset_processing"


        else:

            logger.info(
                "Normal dataset - standard processing"
            )

            return "standard_processing"



    @task
    def weekly_full_report():
        """
        Runs only on Monday.
        """

        logger.info(
            "Running weekly full report"
        )



    @task
    def large_dataset_processing():
        """
        Runs when data is large.

        Example:
        Use parallel processing,
        optimized SQL,
        distributed compute.
        """

        logger.info(
            "Running large dataset processing"
        )



    @task
    def standard_processing():
        """
        Runs for normal data size.
        """

        logger.info(
            "Running standard processing"
        )



    @task(
        trigger_rule="none_failed_min_one_success"
    )
    def send_notification():
        """
        Runs after branch completion.

        It does not require all branches
        to succeed because skipped branches
        are expected.
        """

        logger.info(
            "Pipeline completed. Sending notification."
        )



    # -------------------------------
    # CREATE TASKS
    # -------------------------------

    orders = check_data()


    branch = choose_processing_path(
        orders
    )


    weekly = weekly_full_report()

    large = large_dataset_processing()

    standard = standard_processing()


    notification = send_notification()



    # -------------------------------
    # DEFINE DEPENDENCIES
    # -------------------------------

    #
    # check_data
    #      |
    #      ↓
    # choose_processing_path
    #      |
    #      ├── weekly
    #      ├── large
    #      └── standard
    #

    branch >> [
        weekly,
        large,
        standard
    ]


    #
    # After selected branch completes:
    # notification runs
    #

    [
        weekly,
        large,
        standard
    ] >> notification



# Register DAG
branching_pipeline()