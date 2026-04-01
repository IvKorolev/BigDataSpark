#!/bin/bash

set -e

echo "Waiting for PostgreSQL..."
until docker exec highload_lab2 pg_isready -U postgres -d lab2_db > /dev/null 2>&1; do
  sleep 2
done
echo "PostgreSQL is ready."

echo "Waiting for ClickHouse..."
until docker exec clickhouse_lab2 clickhouse-client --user default --password clickhouse --query "SELECT 1" > /dev/null 2>&1; do
  sleep 2
done
echo "ClickHouse is ready."

echo "Running Spark ETL: raw -> star schema..."
docker exec spark_jupyter spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar \
  /home/jovyan/spark/jobs/create_star_tables.py

echo "Running Spark ETL: star schema -> ClickHouse reports..."
docker exec spark_jupyter spark-submit \
  --jars /home/jovyan/work/jars/postgresql-42.7.3.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --driver-class-path /home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  --conf spark.executor.extraClassPath=/home/jovyan/work/jars/postgresql-42.7.3.jar:/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar \
  /home/jovyan/spark/jobs/clickhouse.py

echo "Creating ClickHouse views..."
docker exec -i clickhouse_lab2 clickhouse-client --user default --password clickhouse < clickhouse_sql/2_clickhouse_views.sql

echo "Pipeline completed successfully."