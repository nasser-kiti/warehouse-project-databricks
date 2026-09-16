from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
from dotenv import load_dotenv
import pendulum

load_dotenv()

DATABRICKS_TOKEN = os.environ.get("DATABRICKS_DBT_ACCESS_TOKEN")
DBT_PROJECT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="walmart_data_pipeline",
    # schedule="0 11 * * *",
    # catchup=False,
    # start_date=pendulum.datetime(year=2026, month=9, day=18, tz="America/Chicago"),
)
def orchestrate():

    @task
    def ingest_cdc():
        client = WorkspaceClient(
            host="dbc-402075c9-dd7d.cloud.databricks.com",
            token=DATABRICKS_TOKEN,
        )

        job_trigger = client.jobs.run_now(job_id=78036674000819)

        while True:
            job_status = client.jobs.get_run(run_id=job_trigger.run_id)
            if not job_status.state:
                print("Job status is None. Unable to retrieve job state.")
                break
            error_states = [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR]
            if job_status.state.result_state == RunResultState.SUCCESS:
                print("Job completed successfully.")
                break
            elif job_status.state.life_cycle_state == RunLifeCycleState.SKIPPED:
                print("Job was skipped. No further action will be taken.")
                break
            elif job_status.state.result_state == RunResultState.FAILED or job_status.state.life_cycle_state in error_states:
                raise Exception(f"Job failed with state: {job_status.state.result_state}\nJob details: {job_status}")
            else:
                print(f"Job is {str(job_status.state.life_cycle_state).split('.')[-1]}. . .")
            time.sleep(5)  # Wait for 5 seconds before checking the status again
        return

    @task.bash
    def clean_target():
        return "rm -rf " + DBT_PROJECT_DIR + "/target && rm -rf " + DBT_PROJECT_DIR + "/dbt_modules && rm -rf " + DBT_PROJECT_DIR + "/logs"

    @task.bash
    def source_freshness():
        return "cd " + DBT_PROJECT_DIR + " && dbt source freshness"

    silver_technical_tests = BashOperator(
        task_id="silver_t_quality_check",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --select silver_t"
    )

    silver_business_tests = BashOperator(
        task_id="silver_b_quality_check",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --select silver_b"
    )

    silver_technical = BashOperator(
        task_id="build_silver_t",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select silver_t"
    )

    silver_business = BashOperator(
        task_id="build_silver_b",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="build_gold_ephemeral",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="build_gold_dimensions",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="build_gold_fact",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select gold/fact"
    )

    check_data_freshness = ingest_cdc() >> clean_target() >> source_freshness()
    quality_check_silver_technical = check_data_freshness >> silver_technical_tests
    quality_check_silver_business = check_data_freshness >> silver_business_tests

    quality_check_silver_technical >> silver_technical
    quality_check_silver_business >> silver_technical

    build_silver_layer = silver_technical >> silver_business

    build_silver_layer >> gold_ephemeral >> gold_dimensions >> gold_fact


orchestrate_dag = orchestrate()
