from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
import time
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
from dotenv import load_dotenv
import pendulum

load_dotenv()

DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_DBT_ACCESS_TOKEN")
DATABRICKS_JOB_ID = int(os.environ.get("DATABRICKS_JOB_ID", 0))
DBT_PROJECT_DIR = "/opt/airflow/dbt"


@dag(
    dag_id="walmart_data_pipeline",
    # schedule=\"0 11 * * *\",
    # catchup=False,
    # start_date=pendulum.datetime(year=2026, month=9, day=18, tz=\"America/Chicago\"),
)
def orchestrate():

    @task
    def ingest_cdc():
        client = WorkspaceClient(host=DATABRICKS_HOST, token=DATABRICKS_TOKEN)
        job_trigger = client.jobs.run_now(job_id=DATABRICKS_JOB_ID)

        while True:
            job_status = client.jobs.get_run(run_id=job_trigger.run_id)
            if not job_status.state:
                raise Exception("Job status is None. Unable to retrieve job state.")
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
            time.sleep(5)
        return

    @task.bash
    def clean_target():
        return "cd " + DBT_PROJECT_DIR + " && dbt clean"

    @task.bash
    def source_freshness():
        return "cd " + DBT_PROJECT_DIR + " && dbt deps && dbt source freshness"

    build_silver_t = BashOperator(
        task_id="build_silver_t",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select silver_t",
    )

    test_silver_t = BashOperator(
        task_id="test_silver_t",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --select silver_t",
    )

    build_silver_b = BashOperator(
        task_id="build_silver_b",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select silver_b",
    )

    test_silver_b = BashOperator(
        task_id="test_silver_b",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --select silver_b",
    )

    gold_ephemeral = BashOperator(
        task_id="build_gold_ephemeral",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select gold/ephemeral",
    )

    gold_dimensions = BashOperator(
        task_id="build_gold_dimensions",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt snapshot",
    )

    gold_fact = BashOperator(
        task_id="build_gold_fact",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt run --select gold/fact",
    )

    test_gold = BashOperator(
        task_id="test_gold",
        cwd=DBT_PROJECT_DIR,
        bash_command="dbt test --select gold",
    )

    (
        ingest_cdc()
        >> clean_target()
        >> source_freshness()
        >> build_silver_t
        >> test_silver_t
        >> build_silver_b
        >> test_silver_b
        >> gold_ephemeral
        >> gold_dimensions
        >> gold_fact
        >> test_gold
    )


orchestrate_dag = orchestrate()
