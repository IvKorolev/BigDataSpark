#!/usr/bin/env python
# coding: utf-8

# In[7]:


import os
os.listdir('/home/jovyan/work/jars')


# In[8]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lab2 Star to ClickHouse") \
    .config(
        "spark.jars",
        "/home/jovyan/work/jars/postgresql-42.7.3.jar,/home/jovyan/work/jars/clickhouse-jdbc-0.6.3.jar"
    ) \
    .getOrCreate()


# In[9]:


pg_url = "jdbc:postgresql://postgres:5432/lab2_db"

pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


# In[10]:


ch_url = "jdbc:clickhouse://clickhouse:8123/default?compress=0"

ch_properties = {
    "user": "default",
    "password": "clickhouse",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver"
}


# In[11]:


test_ch_df = spark.createDataFrame(
    [(1, "ok")],
    ["id", "status"]
)


# In[12]:


test_ch_df.write.jdbc(
    url=ch_url,
    table="test_clickhouse_connection",
    mode="overwrite",
    properties=ch_properties
)


# In[13]:


dim_customer_df = spark.read.jdbc(pg_url, "dim_customer", properties=pg_properties)
dim_seller_df = spark.read.jdbc(pg_url, "dim_seller", properties=pg_properties)
dim_store_df = spark.read.jdbc(pg_url, "dim_store", properties=pg_properties)
dim_supplier_df = spark.read.jdbc(pg_url, "dim_supplier", properties=pg_properties)
dim_product_df = spark.read.jdbc(pg_url, "dim_product", properties=pg_properties)
dim_date_df = spark.read.jdbc(pg_url, "dim_date", properties=pg_properties)
fact_sales_df = spark.read.jdbc(pg_url, "fact_sales", properties=pg_properties)


# In[14]:


print("dim_customer:", dim_customer_df.count())
print("dim_seller:", dim_seller_df.count())
print("dim_store:", dim_store_df.count())
print("dim_supplier:", dim_supplier_df.count())
print("dim_product:", dim_product_df.count())
print("dim_date:", dim_date_df.count())
print("fact_sales:", fact_sales_df.count())


# In[30]:
from pyspark.sql.functions import col

sales_full_df = (
    fact_sales_df.alias("f")
    .join(dim_customer_df.alias("c"), "customer_id", "left")
    .join(dim_seller_df.alias("s"), "seller_id", "left")
    .join(dim_store_df.alias("st"), "store_id", "left")
    .join(dim_supplier_df.alias("sp"), "supplier_id", "left")
    .join(dim_product_df.alias("p"), "product_id", "left")
    .join(dim_date_df.alias("d"), "date_id", "left")
    .select(
        col("f.date_id"),
        col("f.customer_id"),
        col("f.seller_id"),
        col("f.product_id"),
        col("f.store_id"),
        col("f.supplier_id"),
        col("f.sale_quantity"),
        col("f.sale_total_price"),

        col("c.first_name").alias("customer_first_name"),
        col("c.last_name").alias("customer_last_name"),
        col("c.email").alias("customer_email"),
        col("c.country").alias("customer_country"),
        col("c.postal_code").alias("customer_postal_code"),

        col("s.first_name").alias("seller_first_name"),
        col("s.last_name").alias("seller_last_name"),
        col("s.email").alias("seller_email"),
        col("s.country").alias("seller_country"),
        col("s.postal_code").alias("seller_postal_code"),

        col("st.store_name"),
        col("st.store_location"),
        col("st.store_city"),
        col("st.store_state"),
        col("st.store_country"),
        col("st.store_phone"),
        col("st.store_email"),

        col("sp.supplier_name"),
        col("sp.supplier_contact"),
        col("sp.supplier_email"),
        col("sp.supplier_phone"),
        col("sp.supplier_address"),
        col("sp.supplier_city"),
        col("sp.supplier_country"),

        col("p.product_name"),
        col("p.product_category"),
        col("p.product_brand"),
        col("p.product_price"),
        col("p.product_quantity"),
        col("p.product_weight"),
        col("p.product_color"),
        col("p.product_size"),
        col("p.product_material"),
        col("p.product_description"),
        col("p.product_rating"),
        col("p.product_reviews"),
        col("p.product_release_date"),
        col("p.product_expiry_date"),
        col("p.pet_category"),

        col("d.full_date"),
        col("d.day"),
        col("d.month"),
        col("d.year"),
        col("d.quarter")
    )
)


# In[31]:


sales_full_df.printSchema()


# In[32]:


sales_full_df.show(5, truncate=False)


# In[33]:


sales_full_df.count()


# In[34]:


from pyspark.sql.functions import sum, max, avg, col, count, dense_rank
from pyspark.sql.window import Window


# In[35]:


product_sales_df = (
    sales_full_df
    .groupBy(
        "product_id",
        "product_name",
        "product_category",
        "product_brand"
    )
    .agg(
        sum("sale_quantity").alias("total_sales_qty"),
        sum("sale_total_price").alias("total_revenue"),
        count("*").alias("sales_count"),
        avg("product_rating").alias("avg_rating"),
        max("product_reviews").alias("total_reviews")
    )
)


# In[36]:


category_revenue_df = (
    sales_full_df
    .groupBy("product_category")
    .agg(
        sum("sale_total_price").alias("category_revenue")
    )
)


# In[37]:


product_sales_report_df = (
    product_sales_df
    .join(category_revenue_df, on="product_category", how="left")
)


# In[38]:


product_rank_window = Window.orderBy(col("total_sales_qty").desc())

product_sales_report_df = (
    product_sales_report_df
    .withColumn("product_rank_by_sales", dense_rank().over(product_rank_window))
)


# In[39]:


product_sales_report_df.printSchema()


# In[40]:


product_sales_report_df.show(10, truncate=False)


# In[41]:


product_sales_report_df.orderBy(col("total_sales_qty").desc()).show(10, truncate=False)


# In[42]:


import os
os.listdir('/home/jovyan/work/jars')


# In[43]:


product_sales_report_df.write.jdbc(
    url=ch_url,
    table="report_product_sales",
    mode="overwrite",
    properties=ch_properties
)


# In[45]:


customer_sales_df = (
    sales_full_df
    .groupBy(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country"
    )
    .agg(
        sum("sale_total_price").alias("total_spent"),
        count("*").alias("orders_count"),
        avg("sale_total_price").alias("avg_check")
    )
)


# In[46]:


country_customer_df = (
    dim_customer_df
    .groupBy(col("country").alias("customer_country"))
    .agg(count("*").alias("country_customer_count"))
)


# In[47]:


customer_sales_report_df = (
    customer_sales_df
    .join(country_customer_df, on="customer_country", how="left")
)


# In[48]:


customer_rank_window = Window.orderBy(col("total_spent").desc())

customer_sales_report_df = (
    customer_sales_report_df
    .withColumn("customer_rank_by_spent", dense_rank().over(customer_rank_window))
)


# In[49]:


customer_sales_report_df.show(10, truncate=False)


# In[50]:


customer_sales_report_df.write.jdbc(
    url=ch_url,
    table="report_customer_sales",
    mode="overwrite",
    properties=ch_properties
)


# In[51]:


time_sales_report_df = (
    sales_full_df
    .groupBy("year", "month", "quarter")
    .agg(
        count("*").alias("orders_count"),
        sum("sale_quantity").alias("total_sales_qty"),
        sum("sale_total_price").alias("total_revenue"),
        avg("sale_quantity").alias("avg_order_size"),
        avg("sale_total_price").alias("avg_order_value")
    )
    .orderBy("year", "month")
)


# In[52]:


time_sales_report_df.printSchema()


# In[53]:


time_sales_report_df.show(20, truncate=False)


# In[54]:


time_sales_report_df.write.jdbc(
    url=ch_url,
    table="report_time_sales",
    mode="overwrite",
    properties=ch_properties
)


# In[55]:


store_sales_df = (
    sales_full_df
    .groupBy(
        "store_id",
        "store_name",
        "store_city",
        "store_country"
    )
    .agg(
        sum("sale_total_price").alias("total_revenue"),
        count("*").alias("orders_count"),
        sum("sale_quantity").alias("total_sales_qty"),
        avg("sale_total_price").alias("avg_check")
    )
)


# In[56]:


city_revenue_df = (
    sales_full_df
    .groupBy("store_city")
    .agg(
        sum("sale_total_price").alias("city_revenue")
    )
)


# In[57]:


country_revenue_df = (
    sales_full_df
    .groupBy("store_country")
    .agg(
        sum("sale_total_price").alias("country_revenue")
    )
)


# In[58]:


store_sales_report_df = (
    store_sales_df
    .join(city_revenue_df, on="store_city", how="left")
    .join(country_revenue_df, on="store_country", how="left")
)


# In[59]:


store_rank_window = Window.orderBy(col("total_revenue").desc())

store_sales_report_df = (
    store_sales_report_df
    .withColumn("store_rank_by_revenue", dense_rank().over(store_rank_window))
)


# In[60]:


store_sales_report_df.printSchema()


# In[61]:


store_sales_report_df.show(10, truncate=False)


# In[62]:


store_sales_report_df.orderBy(col("total_revenue").desc()).show(5, truncate=False)


# In[63]:


store_sales_report_df.write.jdbc(
    url=ch_url,
    table="report_store_sales",
    mode="overwrite",
    properties=ch_properties
)


# In[64]:


supplier_sales_df = (
    sales_full_df
    .groupBy(
        "supplier_id",
        "supplier_name",
        "supplier_country"
    )
    .agg(
        sum("sale_total_price").alias("total_revenue"),
        count("*").alias("orders_count"),
        sum("sale_quantity").alias("total_sales_qty"),
        avg("product_price").alias("avg_product_price")
    )
)


# In[65]:


supplier_country_revenue_df = (
    sales_full_df
    .groupBy("supplier_country")
    .agg(
        sum("sale_total_price").alias("country_revenue")
    )
)


# In[66]:


supplier_sales_report_df = (
    supplier_sales_df
    .join(supplier_country_revenue_df, on="supplier_country", how="left")
)


# In[67]:


supplier_rank_window = Window.orderBy(col("total_revenue").desc())

supplier_sales_report_df = (
    supplier_sales_report_df
    .withColumn("supplier_rank_by_revenue", dense_rank().over(supplier_rank_window))
)


# In[68]:


supplier_sales_report_df.printSchema()


# In[69]:


supplier_sales_report_df.show(10, truncate=False)


# In[70]:


supplier_sales_report_df.orderBy(col("total_revenue").desc()).show(5, truncate=False)


# In[71]:


supplier_sales_report_df.write.jdbc(
    url=ch_url,
    table="report_supplier_sales",
    mode="overwrite",
    properties=ch_properties
)


# In[72]:


product_quality_df = (
    sales_full_df
    .groupBy(
        "product_id",
        "product_name",
        "product_category",
        "product_brand",
        "product_rating",
        "product_reviews"
    )
    .agg(
        sum("sale_quantity").alias("total_sales_qty"),
        sum("sale_total_price").alias("total_revenue")
    )
)


# In[73]:


sales_rating_correlation = product_quality_df.stat.corr("product_rating", "total_sales_qty")
sales_rating_correlation


# In[74]:


rating_desc_window = Window.orderBy(col("product_rating").desc())
rating_asc_window = Window.orderBy(col("product_rating").asc())
reviews_desc_window = Window.orderBy(col("product_reviews").desc())

product_quality_report_df = (
    product_quality_df
    .withColumn("rating_rank_desc", dense_rank().over(rating_desc_window))
    .withColumn("rating_rank_asc", dense_rank().over(rating_asc_window))
    .withColumn("reviews_rank_desc", dense_rank().over(reviews_desc_window))
)


# In[75]:


from pyspark.sql.functions import lit

product_quality_report_df = (
    product_quality_report_df
    .withColumn("sales_rating_correlation", lit(sales_rating_correlation))
)


# In[76]:


product_quality_report_df.printSchema()


# In[77]:


product_quality_report_df.show(10, truncate=False)


# In[78]:


product_quality_report_df.orderBy(col("product_rating").desc()).show(10, truncate=False)


# In[79]:


product_quality_report_df.orderBy(col("product_rating").asc()).show(10, truncate=False)


# In[80]:


product_quality_report_df.orderBy(col("product_reviews").desc()).show(10, truncate=False)


# In[81]:


product_quality_report_df.write.jdbc(
    url=ch_url,
    table="report_product_quality",
    mode="overwrite",
    properties=ch_properties
)


# In[ ]:




