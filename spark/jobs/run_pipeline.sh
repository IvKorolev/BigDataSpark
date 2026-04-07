#!/bin/bash

set -e

echo "Waiting for PostgreSQL and ClickHouse startup..."
sleep 30

echo "Running Spark ETL: raw -> star schema..."
spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar \
  /home/jovyan/spark/jobs/create_star_tables.py

echo "Running Spark ETL: star schema -> ClickHouse reports and views..."
spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  /home/jovyan/spark/jobs/clickhouse.py

echo "Pipeline completed successfully."