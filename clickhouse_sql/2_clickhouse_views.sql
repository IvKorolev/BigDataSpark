-- 1.1 Топ-10 самых продаваемых продуктов
DROP VIEW IF EXISTS vw_top_10_products;
CREATE VIEW vw_top_10_products AS
SELECT
    product_name,
    product_category,
    product_brand,
    total_sales_qty,
    total_revenue
FROM report_product_sales
ORDER BY total_sales_qty DESC
LIMIT 10;

-- 1.2 Общая выручка по категориям продуктов
DROP VIEW IF EXISTS vw_category_revenue;
CREATE VIEW vw_category_revenue AS
SELECT DISTINCT
    product_category,
    category_revenue
FROM report_product_sales
ORDER BY category_revenue DESC;

-- 1.3 Средний рейтинг и количество отзывов для каждого продукта
DROP VIEW IF EXISTS vw_product_rating_reviews;
CREATE VIEW vw_product_rating_reviews AS
SELECT
    product_name,
    product_category,
    product_brand,
    avg_rating,
    total_reviews
FROM report_product_sales
ORDER BY avg_rating DESC;

-- 2.1 Топ-10 клиентов с наибольшей общей суммой покупок
DROP VIEW IF EXISTS vw_top_10_customers;
CREATE VIEW vw_top_10_customers AS
SELECT
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_country,
    total_spent,
    orders_count
FROM report_customer_sales
ORDER BY total_spent DESC
LIMIT 10;

-- 2.2 Распределение клиентов по странам
DROP VIEW IF EXISTS vw_customers_by_country;
CREATE VIEW vw_customers_by_country AS
SELECT DISTINCT
    customer_country,
    country_customer_count
FROM report_customer_sales
ORDER BY country_customer_count DESC;

-- 2.3 Средний чек для каждого клиента
DROP VIEW IF EXISTS vw_customer_avg_check;
CREATE VIEW vw_customer_avg_check AS
SELECT
    customer_first_name,
    customer_last_name,
    customer_email,
    avg_check,
    orders_count,
    total_spent
FROM report_customer_sales
ORDER BY avg_check DESC;

-- 3.1 Месячные и годовые тренды продаж
DROP VIEW IF EXISTS vw_monthly_yearly_trends;
CREATE VIEW vw_monthly_yearly_trends AS
SELECT
    year,
    month,
    quarter,
    orders_count,
    total_revenue
FROM report_time_sales
ORDER BY year, month;

-- 3.2 Сравнение выручки за разные периоды
DROP VIEW IF EXISTS vw_revenue_comparison;
CREATE VIEW vw_revenue_comparison AS
SELECT
    year,
    quarter,
    sum(total_revenue) AS quarter_revenue,
    sum(orders_count) AS quarter_orders
FROM report_time_sales
GROUP BY year, quarter
ORDER BY year, quarter;

-- 3.3 Средний размер заказа по месяцам
DROP VIEW IF EXISTS vw_avg_order_by_month;
CREATE VIEW vw_avg_order_by_month AS
SELECT
    year,
    month,
    avg_order_size,
    avg_order_value
FROM report_time_sales
ORDER BY year, month;

-- 4.1 Топ-5 магазинов с наибольшей выручкой
DROP VIEW IF EXISTS vw_top_5_stores;
CREATE VIEW vw_top_5_stores AS
SELECT
    store_name,
    store_city,
    store_country,
    total_revenue,
    orders_count
FROM report_store_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- 4.2 Распределение продаж по городам и странам
DROP VIEW IF EXISTS vw_sales_by_city_country;
CREATE VIEW vw_sales_by_city_country AS
SELECT DISTINCT
    store_city,
    store_country,
    city_revenue,
    country_revenue
FROM report_store_sales
ORDER BY country_revenue DESC, city_revenue DESC;

-- 4.3 Средний чек для каждого магазина
DROP VIEW IF EXISTS vw_store_avg_check;
CREATE VIEW vw_store_avg_check AS
SELECT
    store_name,
    store_city,
    store_country,
    avg_check,
    orders_count,
    total_revenue
FROM report_store_sales
ORDER BY avg_check DESC;

-- 5.1 Топ-5 поставщиков с наибольшей выручкой
DROP VIEW IF EXISTS vw_top_5_suppliers;
CREATE VIEW vw_top_5_suppliers AS
SELECT
    supplier_name,
    supplier_country,
    total_revenue,
    orders_count
FROM report_supplier_sales
ORDER BY total_revenue DESC
LIMIT 5;

-- 5.2 Средняя цена товаров от каждого поставщика
DROP VIEW IF EXISTS vw_supplier_avg_price;
CREATE VIEW vw_supplier_avg_price AS
SELECT
    supplier_name,
    supplier_country,
    avg_product_price,
    total_sales_qty
FROM report_supplier_sales
ORDER BY avg_product_price DESC;

-- 5.3 Распределение продаж по странам поставщиков
DROP VIEW IF EXISTS vw_supplier_sales_by_country;
CREATE VIEW vw_supplier_sales_by_country AS
SELECT DISTINCT
    supplier_country,
    country_revenue
FROM report_supplier_sales
ORDER BY country_revenue DESC;

-- 6.1 Продукты с наивысшим рейтингом
DROP VIEW IF EXISTS vw_products_by_rating;
CREATE VIEW vw_products_by_rating AS
SELECT
    product_name,
    product_category,
    product_brand,
    product_rating,
    total_sales_qty,
    total_revenue
FROM report_product_quality
ORDER BY product_rating DESC;

-- 6.2 Продукты с наименьшим рейтингом
DROP VIEW IF EXISTS vw_products_by_rating;
CREATE VIEW vw_products_by_rating AS
SELECT
    product_name,
    product_category,
    product_brand,
    product_rating,
    total_sales_qty,
    total_revenue
FROM report_product_quality
ORDER BY product_rating ASC;

-- 6.3 Корреляция между рейтингом и объемом продаж
DROP VIEW IF EXISTS vw_rating_sales_correlation;
CREATE VIEW vw_rating_sales_correlation AS
SELECT
    round(product_rating, 0) AS rating_group,
    count(*) AS products_count,
    sum(total_sales_qty) AS group_total_sales,
    avg(total_sales_qty) AS group_avg_sales,
    any(sales_rating_correlation) AS overall_correlation
FROM report_product_quality
GROUP BY rating_group
ORDER BY rating_group DESC;

-- 6.4 Продукты с наибольшим количеством отзывов
DROP VIEW IF EXISTS vw_most_reviewed_products;
CREATE VIEW vw_most_reviewed_products AS
SELECT
    product_name,
    product_category,
    product_brand,
    product_reviews,
    product_rating,
    total_sales_qty,
    total_revenue
FROM report_product_quality
ORDER BY product_reviews DESC
LIMIT 20;