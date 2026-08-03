from apache/airflow:3.3.0

USER root

RUN apt-get update && apt-get install -y gcc && apt-clean

USER airflow

copy requirements.txt .

run pip install --no-cache-dir -r requirements.txt