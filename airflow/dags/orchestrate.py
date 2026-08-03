from airflow.sdk import dag, task

@dag(schedule=None, start_date=None, catchup=False)
def orchestrate():

    @task
    def ingest_cdc():
        return "CDC data ingestion"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_project && source_freshness"

    ingest_cdc() >> source_freshness() 

orchestration_dag = orchestrate()