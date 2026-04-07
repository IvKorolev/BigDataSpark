#!/bin/bash

set -e

echo "Waiting for PostgreSQL and ClickHouse startup..."
sleep 20

echo "Running Spark ETL: raw -> star schema..."
spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar \
  /home/jovyan/spark/jobs/create_star_tables.py

echo "Running Spark ETL: star schema -> ClickHouse reports..."
spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  /home/jovyan/spark/jobs/clickhouse.py

echo "Creating ClickHouse views..."
curl -u default:clickhouse "http://clickhouse:8123/" --data-binary @/home/jovyan/clickhouse_sql/2_clickhouse_views.sql

echo "Pipeline completed successfully."