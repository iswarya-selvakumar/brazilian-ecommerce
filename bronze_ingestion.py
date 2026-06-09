# Databricks notebook source
# MAGIC %md
# MAGIC #  Bronze Layer - Customer data Ingestion

# COMMAND ----------

# DBTITLE 1,Read customer data from s3
customer_df=spark.read.format("csv").options(header=True,inferSchema=True).load("s3://ish-ecommerce-project/raw/customers/")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(customer_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Validate ADLS connectivity
dbutils.fs.ls("abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/")

# COMMAND ----------

# DBTITLE 1,Write customer data to ADLS bronze
customer_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/customer").saveAsTable("ecomdb_catalog.bronze.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # Order data Ingestion

# COMMAND ----------

# DBTITLE 1,Read order data from s3
order_df=spark.read.format("csv").options(header=True,inferSchema=True).load("s3://ish-ecommerce-project/raw/orders/")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(order_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write order data to ADLS bronze
order_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/order").saveAsTable("ecomdb_catalog.bronze.order")

# COMMAND ----------

# MAGIC %md
# MAGIC # Products data Ingestion

# COMMAND ----------

# DBTITLE 1,Read product data from s3
product_df=spark.read.format("csv").options(header=True,inferSchema=True).load("s3://ish-ecommerce-project/raw/products/")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(product_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write data to ADLS bronze
product_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/product").saveAsTable("ecomdb_catalog.bronze.product")

# COMMAND ----------

# MAGIC %md
# MAGIC # Order item data Ingestion

# COMMAND ----------

# DBTITLE 1,Read order_item_data from s3
order_item_df=spark.read.format("csv").options(header=True,inferSchema=True)\
                .load("s3://ish-ecommerce-project/raw/order_items/")


# COMMAND ----------

# DBTITLE 1,Data Preview
display(order_item_df.limit(10))

# COMMAND ----------

# DBTITLE 1,write order_item data to ADLS
order_item_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/order_item").saveAsTable("ecomdb_catalog.bronze.order_item")

# COMMAND ----------

# MAGIC %md
# MAGIC # Payment data Ingestion

# COMMAND ----------

# DBTITLE 1,Read payment data from S3
payment_df=spark.read.format("csv").options(header=True,inferSchema=True)\
                .load("s3://ish-ecommerce-project/raw/payments/")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(payment_df.limit(10))

# COMMAND ----------

# DBTITLE 1,write payment data to ADLS
payment_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/payment").saveAsTable("ecomdb_catalog.bronze.payment")

# COMMAND ----------

# MAGIC %md
# MAGIC # Reviews data Ingestion

# COMMAND ----------

# DBTITLE 1,Read reviews data from S3
review_df=spark.read.format("csv").option("header",True).option("inferSchema",True)\
                .option("multiline",True)\
                .option("escape","\"")\
                .load("s3://ish-ecommerce-project/raw/reviews/")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(review_df.limit(10))

# COMMAND ----------

# DBTITLE 1,write review data to ADLS
review_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/review").saveAsTable("ecomdb_catalog.bronze.review")

# COMMAND ----------

# MAGIC %md
# MAGIC # product_category translation data Ingestion

# COMMAND ----------

# DBTITLE 1,Read data from S3
category_translation_df=spark.read.format("csv").options(header=True,inferSchema=True)\
                .load("s3://ish-ecommerce-project/raw/translations/")


# COMMAND ----------

# DBTITLE 1,Data Preview
display(category_translation_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Write data to ADLS
category_translation_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/bronze/category_translation").saveAsTable("ecomdb_catalog.bronze.category_translation")