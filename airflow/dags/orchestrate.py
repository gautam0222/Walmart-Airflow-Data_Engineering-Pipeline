import time
import os
from airflow.operators.bash import BashOperator
from databricks.sdk.service.jobs import RunLifeCycleState
from airflow.sdk import dag, task
from databricks.sdk import WorkspaceClient
import pendulum

@dag(
    dag_id="orchestrate",
    schedule="0 0 * * *",  # Run daily at midnight
    catchup=False,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC")
)

def orchestrate():

    @task
    def ingest_cdc():
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
        return "CDC ingestion completed successfully."

    @task.bash
    def source_freshness():
        return """
        cd /opt/airflow/walmart_project && \
        rm -rf target && \
        dbt source freshness
        """

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_technical"
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_technical_tests"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt run --select silver_business"
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt test --select silver_business_tests"
    )
    
    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt run --select gold_ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt snapshot"
    )

    gold_fact = BashOperator(
        task_id="gold_fact",
        cwd= "/opt/airflow/walmart_project",
        bash_command="dbt run --select gold_fact"
    )


    ingest_cdc() >> source_freshness() >> silver_technical  >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_fact


orchestration_dag = orchestrate()