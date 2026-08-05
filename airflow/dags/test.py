from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState
import time
import os
ws = WorkspaceClient(
    host=os.environ["DATABRICKS_HOST"],
    token=os.environ["DATABRICKS_TOKEN"]
)

job_trigger = ws.jobs.run_now(job_id=387494904914486)

while True:
    job_status = ws.jobs.get_run(run_id=job_trigger.run_id)
    state = job_status.state

    if state is None:
        raise Exception("Job state is None")

    if state.life_cycle_state in (
        RunLifeCycleState.TERMINATED,
        RunLifeCycleState.SKIPPED,
        RunLifeCycleState.INTERNAL_ERROR,
    ):
        if str(state.result_state) == "SUCCESS":
            print("Job completed successfully.")
            break
        else:
            raise Exception(f"Job failed with state: {state.result_state}")
    time.sleep(10)  # Wait for 10 seconds before checking the status again