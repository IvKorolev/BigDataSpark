# BigDataSpark

## Описание решения

Исходные данные из CSV-файлов `MOCK_DATA*.csv` загружаются в PostgreSQL в таблицу `mock_data`, затем с помощью Spark трансформируются в модель данных **звезда** в PostgreSQL. После этого с помощью Spark на основе модели звезда строятся аналитические витрины и загружаются в ClickHouse.

## Используемый стек

- PostgreSQL
- Apache Spark
- Jupyter Notebook
- ClickHouse
- Docker / Docker Compose
- DBeaver

---

## Структура проекта

```bash
BigDataSpark/
├── data/                        # исходники
├── spark/
│   └── jobs/                   
├── sql_scripts/
│   └── 1_raw_table.sql          # mock_data в PostgreSQL
├── work/
│   ├── jars/
│   │   ├── postgresql-42.7.3.jar     # надо скачать
│   │   └── clickhouse-jdbc-0.6.3.jar # надо скачать
│   ├── create_star_tables.ipynb # Spark ETL: в звезду
│   ├── create_star_tables.py
│   ├── clickhouse.ipynb         # Spark ETL: репорты в кликхаус
│   └── clickhouse.py
├── docker-compose.yml
└── README.md


```
## Алгоритм запуска

## JDBC-драйверы

Перед запуском необходимо скачать JDBC-драйверы в папку `work/jars`:

```bash
mkdir -p work/jars
curl -L -o work/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
curl -L -o work/jars/clickhouse-jdbc-0.6.3.jar https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.3/clickhouse-jdbc-0.6.3.jar
```
### 1. Поднять инфраструктуру

```bash
docker compose up -d
``` 

### 2. Открыть Jupyter
Перейти в браузере по адресу:

```text
http://localhost:8888
```

Если нужен token:
```bash
docker logs spark_jupyter
```

### 3. Построить модель звезда в PostgreSQL
В Jupyter открыть и выполнить сверху вниз ноутбук:

```text
work/create_star_tables.ipynb
```

### 4. Построить витрины в ClickHouse
В Jupyter открыть и выполнить сверху вниз ноутбук:

```text
work/clickhouse.ipynb
```

### 5. Проверить результат

#### PostgreSQL
Подключение:
- Host: `localhost`
- Port: `5434`
- Database: `lab2_db`
- User: `postgres`
- Password: `postgres`

Проверка:
```sql
SELECT COUNT(*) FROM mock_data;
SELECT COUNT(*) FROM fact_sales;
```

#### ClickHouse
Подключение:
- Host: `localhost`
- Port: `8123`
- Database: `default`
- User: `default`
- Password: `clickhouse`

Проверка:
```sql
SHOW TABLES;

SELECT COUNT(*) FROM report_product_sales;
SELECT COUNT(*) FROM report_customer_sales;
SELECT COUNT(*) FROM report_time_sales;
SELECT COUNT(*) FROM report_store_sales;
SELECT COUNT(*) FROM report_supplier_sales;
SELECT COUNT(*) FROM report_product_quality;
```