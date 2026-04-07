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
├── clickhouse_sql/
│   └──2_clickhouse_views.sql    # создаем вьюшки
├── data/                        # исходники
├── spark/
│   └── jobs/       
│   ├── create_star_tables.ipynb # Spark ETL: в звезду
│   ├── create_star_tables.py
│   ├── clickhouse.ipynb         # Spark ETL: репорты в кликхаус
│   └── clickhouse.py            
├── sql_scripts/
│   └── 1_raw_table.sql          # mock_data в PostgreSQL
├── work/
│   ├── jars/
│   │   ├── postgresql-42.7.3.jar     # надо скачать
│   │   └── clickhouse-jdbc-0.6.3.jar # надо скачать
│
├── docker-compose.yml
└── README.md


```
## Алгоритм запуска

### 1. Поднять инфраструктуру

```bash
docker compose up -d
``` 

### 2. Подождать 20-30 секунд до поднятия бд, для логов
```bash
docker logs -f spark_jobs
``` 
Скрипт: 
1. ожидает готовности PostgreSQL и ClickHouse
2. запускает Spark ETL raw -> star schema; 
3. запускает Spark ETL star schema -> ClickHouse; 
4. создаёт представления (views) в ClickHouse.

### 3. Проверить результат

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