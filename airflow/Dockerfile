FROM apache/airflow:slim-3.0.2-python3.12

USER root

RUN apt-get update && apt-get install -y gcc pkg-config default-libmysqlclient-dev libpq-dev && apt-get clean

USER airflow

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt