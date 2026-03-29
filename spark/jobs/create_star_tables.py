#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Lab2 ETL Raw to Star") \
    .config("spark.jars", "/home/jovyan/work/jars/postgresql-42.7.3.jar") \
    .getOrCreate()


# In[2]:


jdbc_url = "jdbc:postgresql://postgres:5432/lab2_db"

connection_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


# In[3]:


raw_df = spark.read.jdbc(
    url=jdbc_url,
    table="mock_data",
    properties=connection_properties
)


# In[4]:


raw_df.printSchema()


# In[5]:


raw_df.show(5, truncate=False)


# In[6]:


raw_df.count()


# In[7]:


from pyspark.sql.functions import col, trim, lower, to_date
from pyspark.sql.types import IntegerType, DecimalType


# In[8]:


clean_df = (
    raw_df
    .withColumn("customer_email", trim(lower(col("customer_email"))))
    .withColumn("seller_email", trim(lower(col("seller_email"))))
    .withColumn("store_email", trim(lower(col("store_email"))))
    .withColumn("supplier_email", trim(lower(col("supplier_email"))))
    
    .withColumn("sale_date_parsed", to_date(col("sale_date"), "M/d/yyyy"))
    .withColumn("product_release_date_parsed", to_date(col("product_release_date"), "M/d/yyyy"))
    .withColumn("product_expiry_date_parsed", to_date(col("product_expiry_date"), "M/d/yyyy"))
    
    .withColumn("customer_age", col("customer_age").cast(IntegerType()))
    .withColumn("product_quantity", col("product_quantity").cast(IntegerType()))
    .withColumn("sale_quantity", col("sale_quantity").cast(IntegerType()))
    .withColumn("product_reviews", col("product_reviews").cast(IntegerType()))
    
    .withColumn("product_price", col("product_price").cast(DecimalType(10, 2)))
    .withColumn("sale_total_price", col("sale_total_price").cast(DecimalType(10, 2)))
    .withColumn("product_weight", col("product_weight").cast(DecimalType(10, 2)))
    .withColumn("product_rating", col("product_rating").cast(DecimalType(3, 1)))
)


# In[9]:


clean_df.printSchema()


# In[10]:


clean_df.select(
    "customer_email",
    "seller_email",
    "store_email",
    "supplier_email",
    "sale_date",
    "sale_date_parsed",
    "product_release_date",
    "product_release_date_parsed",
    "product_expiry_date",
    "product_expiry_date_parsed",
    "product_price",
    "sale_total_price"
).show(10, truncate=False)


# In[11]:


clean_df.select(
    "sale_date",
    "sale_date_parsed"
).show(10, truncate=False)


# In[12]:


clean_df.filter(col("sale_date_parsed").isNull()).count()


# In[13]:


from pyspark.sql.window import Window
from pyspark.sql.functions import row_number


# In[14]:


customer_window = Window.orderBy("email")

dim_customer = (
    clean_df
    .select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_age").alias("age"),
        col("customer_email").alias("email"),
        col("customer_country").alias("country"),
        col("customer_postal_code").alias("postal_code")
    )
    .dropna(subset=["email"])
    .dropDuplicates(["email"])
    .withColumn("customer_id", row_number().over(customer_window))
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "age",
        "email",
        "country",
        "postal_code"
    )
)


# In[15]:


dim_customer.printSchema()


# In[16]:


dim_customer.show(10, truncate=False)


# In[17]:


dim_customer.count()


# In[18]:


from pyspark.sql.functions import count

dim_customer.groupBy("email").agg(count("*").alias("cnt")).filter(col("cnt") > 1).show()


# In[19]:


seller_window = Window.orderBy("email")

dim_seller = (
    clean_df
    .select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_country").alias("country"),
        col("seller_postal_code").alias("postal_code")
    )
    .dropna(subset=["email"])
    .dropDuplicates(["email"])
    .withColumn("seller_id", row_number().over(seller_window))
    .select(
        "seller_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "postal_code"
    )
)


# In[20]:


dim_seller.printSchema()


# In[21]:


dim_seller.show(10, truncate=False)


# In[22]:


dim_seller.count()


# In[23]:


dim_seller.groupBy("email").count().filter(col("count") > 1).show()


# In[24]:


store_window = Window.orderBy("store_email")

dim_store = (
    clean_df
    .select(
        col("store_name"),
        col("store_location"),
        col("store_city"),
        col("store_state"),
        col("store_country"),
        col("store_phone"),
        col("store_email")
    )
    .dropna(subset=["store_email"])
    .dropDuplicates(["store_email"])
    .withColumn("store_id", row_number().over(store_window))
    .select(
        "store_id",
        "store_name",
        "store_location",
        "store_city",
        "store_state",
        "store_country",
        "store_phone",
        "store_email"
    )
)


# In[25]:


dim_store.printSchema()


# In[26]:


dim_store.show(10, truncate=False)


# In[27]:


dim_store.count()


# In[28]:


dim_store.groupBy("store_email").count().filter(col("count") > 1).show()


# In[29]:


supplier_window = Window.orderBy("supplier_email")

dim_supplier = (
    clean_df
    .select(
        col("supplier_name"),
        col("supplier_contact"),
        col("supplier_email"),
        col("supplier_phone"),
        col("supplier_address"),
        col("supplier_city"),
        col("supplier_country")
    )
    .dropna(subset=["supplier_email"])
    .dropDuplicates(["supplier_email"])
    .withColumn("supplier_id", row_number().over(supplier_window))
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_contact",
        "supplier_email",
        "supplier_phone",
        "supplier_address",
        "supplier_city",
        "supplier_country"
    )
)


# In[30]:


dim_supplier.printSchema()


# In[31]:


dim_supplier.show(10, truncate=False)


# In[32]:


dim_supplier.count()


# In[33]:


dim_supplier.groupBy("supplier_email").count().filter(col("count") > 1).show()


# In[34]:


product_window = Window.orderBy(
    "product_name",
    "product_category",
    "product_brand",
    "product_color",
    "product_size"
)

dim_product = (
    clean_df
    .select(
        col("product_name"),
        col("product_category"),
        col("product_price"),
        col("product_quantity"),
        col("product_weight"),
        col("product_color"),
        col("product_size"),
        col("product_brand"),
        col("product_material"),
        col("product_description"),
        col("product_rating"),
        col("product_reviews"),
        col("product_release_date_parsed").alias("product_release_date"),
        col("product_expiry_date_parsed").alias("product_expiry_date"),
        col("pet_category")
    )
    .dropDuplicates([
        "product_name",
        "product_category",
        "product_brand",
        "product_color",
        "product_size"
    ])
    .withColumn("product_id", row_number().over(product_window))
    .select(
        "product_id",
        "product_name",
        "product_category",
        "product_price",
        "product_quantity",
        "product_weight",
        "product_color",
        "product_size",
        "product_brand",
        "product_material",
        "product_description",
        "product_rating",
        "product_reviews",
        "product_release_date",
        "product_expiry_date",
        "pet_category"
    )
)


# In[35]:


dim_product.printSchema()


# In[36]:


dim_product.show(10, truncate=False)


# In[37]:


dim_product.count()


# In[38]:


dim_product.groupBy(
    "product_name",
    "product_category",
    "product_brand",
    "product_color",
    "product_size"
).count().filter(col("count") > 1).show()


# In[39]:


from pyspark.sql.functions import dayofmonth, month, year, quarter


# In[40]:


date_window = Window.orderBy("full_date")

dim_date = (
    clean_df
    .select(col("sale_date_parsed").alias("full_date"))
    .dropna(subset=["full_date"])
    .dropDuplicates(["full_date"])
    .withColumn("day", dayofmonth(col("full_date")))
    .withColumn("month", month(col("full_date")))
    .withColumn("year", year(col("full_date")))
    .withColumn("quarter", quarter(col("full_date")))
    .withColumn("date_id", row_number().over(date_window))
    .select(
        "date_id",
        "full_date",
        "day",
        "month",
        "year",
        "quarter"
    )
)


# In[41]:


dim_date.printSchema()


# In[42]:


dim_date.show(10, truncate=False)


# In[43]:


dim_date.count()


# In[44]:


dim_date.groupBy("full_date").count().filter(col("count") > 1).show()


# In[45]:


f = clean_df.alias("f")
c = dim_customer.alias("c")
s = dim_seller.alias("s")
st = dim_store.alias("st")
sp = dim_supplier.alias("sp")
p = dim_product.alias("p")
d = dim_date.alias("d")


# In[46]:


fact_sales = (
    f
    .join(c, col("f.customer_email") == col("c.email"), "left")
    .join(s, col("f.seller_email") == col("s.email"), "left")
    .join(st, col("f.store_email") == col("st.store_email"), "left")
    .join(sp, col("f.supplier_email") == col("sp.supplier_email"), "left")
    .join(
        p,
        (col("f.product_name") == col("p.product_name")) &
        (col("f.product_category") == col("p.product_category")) &
        (col("f.product_brand") == col("p.product_brand")) &
        (col("f.product_color") == col("p.product_color")) &
        (col("f.product_size") == col("p.product_size")),
        "left"
    )
    .join(d, col("f.sale_date_parsed") == col("d.full_date"), "left")
    .select(
        col("d.date_id").alias("date_id"),
        col("c.customer_id").alias("customer_id"),
        col("s.seller_id").alias("seller_id"),
        col("p.product_id").alias("product_id"),
        col("st.store_id").alias("store_id"),
        col("sp.supplier_id").alias("supplier_id"),
        col("f.sale_quantity").alias("sale_quantity"),
        col("f.sale_total_price").alias("sale_total_price")
    )
)


# In[47]:


fact_sales.printSchema()


# In[48]:


fact_sales.show(10, truncate=False)


# In[49]:


fact_sales.count()


# In[50]:


from pyspark.sql.functions import sum as spark_sum

fact_sales.select(
    spark_sum(col("date_id").isNull().cast("int")).alias("null_date_id"),
    spark_sum(col("customer_id").isNull().cast("int")).alias("null_customer_id"),
    spark_sum(col("seller_id").isNull().cast("int")).alias("null_seller_id"),
    spark_sum(col("product_id").isNull().cast("int")).alias("null_product_id"),
    spark_sum(col("store_id").isNull().cast("int")).alias("null_store_id"),
    spark_sum(col("supplier_id").isNull().cast("int")).alias("null_supplier_id")
).show()


# In[51]:


dim_customer.write.jdbc(
    url=jdbc_url,
    table="dim_customer",
    mode="overwrite",
    properties=connection_properties
)


# In[52]:


dim_seller.write.jdbc(
    url=jdbc_url,
    table="dim_seller",
    mode="overwrite",
    properties=connection_properties
)


# In[53]:


dim_store.write.jdbc(
    url=jdbc_url,
    table="dim_store",
    mode="overwrite",
    properties=connection_properties
)


# In[54]:


dim_supplier.write.jdbc(
    url=jdbc_url,
    table="dim_supplier",
    mode="overwrite",
    properties=connection_properties
)


# In[55]:


dim_product.write.jdbc(
    url=jdbc_url,
    table="dim_product",
    mode="overwrite",
    properties=connection_properties
)


# In[56]:


dim_date.write.jdbc(
    url=jdbc_url,
    table="dim_date",
    mode="overwrite",
    properties=connection_properties
)


# In[57]:


fact_sales.write.jdbc(
    url=jdbc_url,
    table="fact_sales",
    mode="overwrite",
    properties=connection_properties
)


# In[ ]:




