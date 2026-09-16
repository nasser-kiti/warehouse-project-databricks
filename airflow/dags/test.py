

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
import os
from dotenv import load_dotenv
from warehouse_airflow_dbt.utils.logger import get_logger

load_dotenv()

DATABRICKS_TOKEN = os.environ.get("DATABRICKS_DBT_ACCESS_TOKEN")
logger = get_logger(__name__)

client = WorkspaceClient(
    host="dbc-402075c9-dd7d.cloud.databricks.com",
    token=DATABRICKS_TOKEN,
)

job_trigger = client.jobs.run_now(job_id=78036674000819)

while True:
    job_status = client.jobs.get_run(run_id=job_trigger.run_id)
    if not job_status.state:
        logger.error("Job status is None. Unable to retrieve job state.")
        break

    error_states = [RunLifeCycleState.TERMINATED, RunLifeCycleState.INTERNAL_ERROR]
    if job_status.state.result_state == RunResultState.SUCCESS:
        logger.info("Job completed successfully.")
        break
    elif job_status.state.life_cycle_state == RunLifeCycleState.SKIPPED:
        logger.warning("Job was skipped. No further action will be taken.")
        break
    elif job_status.state.result_state == RunResultState.FAILED or job_status.state.life_cycle_state in error_states:
        logger.error(f"Job failed with state: {job_status.state.result_state}")
        raise Exception(f"Job failed with state: {job_status.state.result_state}\nJob details: {job_status}")
    else:
        logger.info(f"Job is now {str(job_status.state.life_cycle_state).split('.')[-1]}. . .")
    time.sleep(5)  # Wait for 5 seconds before checking the status again
