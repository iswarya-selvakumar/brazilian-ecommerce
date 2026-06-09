# Databricks notebook source
# DBTITLE 1,Read silver tables
customer_df=spark.read.table("ecomdb_catalog.silver.customer")
order_df=spark.read.table("ecomdb_catalog.silver.order")
product_df=spark.read.table("ecomdb_catalog.silver.product")
order_item_df=spark.read.table("ecomdb_catalog.silver.order_item")
payment_df=spark.read.table("ecomdb_catalog.silver.payment")
review_df=spark.read.table("ecomdb_catalog.silver.review")
category_name_df=spark.read.table("ecomdb_catalog.silver.category_name")

# COMMAND ----------

# DBTITLE 1,Data preview
display(customer_df.limit(10))
display(order_df.limit(10))
display(product_df.limit(10))
display(order_item_df.limit(10))
display(payment_df.limit(10))
display(review_df.limit(10))
display(category_name_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Customer Dimension Table
# MAGIC

# COMMAND ----------

dim_customer = customer_df.select("customer_id","customer_unique_id","customer_zip_code_prefix","customer_city","customer_state").dropDuplicates(["customer_id"])

# COMMAND ----------

# DBTITLE 1,data preview
display(dim_customer)

# COMMAND ----------

# DBTITLE 1,Data validation
dim_customer.groupBy("customer_id").count().filter("count > 1").show()

# COMMAND ----------

# DBTITLE 1,save dim_customer table
dim_customer.write.format("delta").mode("overwrite").option("overwriteschema",True).option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/dim_customer").saveAsTable("ecomdb_catalog.gold.dim_customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Product Dimension Table

# COMMAND ----------

product=product_df.join(category_name_df,product_df.product_category_name==category_name_df.category_name,"left")
display(product.limit(10))

# COMMAND ----------

dim_products=product.select("product_id","product_category_name","category_name_en","product_name_lenght","product_description_lenght","product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm","product_volume_cm3","is_product_complete").dropDuplicates(["product_id"])
display(dim_products.limit(10))

# COMMAND ----------

# DBTITLE 1,Null check
from pyspark.sql.functions import col
for c in dim_products.columns:
  print(c, dim_products.filter(col(c).isNull()).count())

# COMMAND ----------

# DBTITLE 1,null replace
dim_product = dim_products.fillna({ 
    "product_category_name": "Unknown",
    "category_name_en": "Unknown Category"
})

# COMMAND ----------

# DBTITLE 1,data validation
dim_product.groupBy("product_id").count().filter("count > 1").show()

# COMMAND ----------

# DBTITLE 1,Save dim_product table
dim_product.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/dim_product").saveAsTable("ecomdb_catalog.gold.dim_product")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Review Dimension Table

# COMMAND ----------

dim_review = review_df.select("review_id","order_id","review_score","review_sentiment","review_creation_date").dropDuplicates(["order_id"])


# COMMAND ----------

# DBTITLE 1,Data preview
display(dim_review)

# COMMAND ----------

# DBTITLE 1,Data validation check
dim_review.groupBy("order_id").count().filter("count > 1").show()

# COMMAND ----------

# DBTITLE 1,save dim_review table
dim_review.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/dim_review").saveAsTable("ecomdb_catalog.gold.dim_review")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Date Dimension Table

# COMMAND ----------

# DBTITLE 1,data preview
display(order_df.limit(10))


# COMMAND ----------

from pyspark.sql.functions import*
dim_date = order_df.select(to_date("order_purchase_timestamp").alias("date"))\
    .withColumn("year",year("date"))\
    .withColumn("month",month("date"))\
    .withColumn("day",dayofmonth("date"))\
    .withColumn("quarter",quarter("date"))\
    .withColumn("week",weekofyear("date")).dropDuplicates(["date"])

# COMMAND ----------

# DBTITLE 1,Data preview
display(dim_date)


# COMMAND ----------

dim_date.groupBy("date").count().filter("count > 1").show()

# COMMAND ----------

# DBTITLE 1,Validation
dim_date.filter(col("date").isNull()).count()

# COMMAND ----------

# DBTITLE 1,Save dim_date table
dim_date.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/dim_date").saveAsTable("ecomdb_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Sales Fact Table

# COMMAND ----------

joined_df = order_item_df.join(order_df,"order_id","left")
display(joined_df.limit(10))


# COMMAND ----------

payment_summary = payment_df.groupBy("order_id").agg(
    round(sum("payment_value"),2).alias("total_payment_value")
)
display(payment_summary)

# COMMAND ----------

sale = joined_df.join(payment_summary,"order_id","left")
display(sale.limit(10))

# COMMAND ----------

fact_sale = sale.withColumn(
    "order_date",
    to_date("order_purchase_timestamp")
)
display(fact_sale.limit(10))

# COMMAND ----------

fact_sales = fact_sale.select(
    "order_id",
    "order_item_id",
    "customer_id",
    "product_id",
    "seller_id",
    "price",
    "freight_value",
    "total_revenue",
    "total_payment_value",
    "order_status",
    "status_group",
    "order_date",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "delivery_days",
    "delivery_delay_days",
    "is_time_sequence_valid",
    "data_quality_issue"
)

# COMMAND ----------

display(fact_sales)

# COMMAND ----------

# DBTITLE 1,Schema validation
fact_sales.printSchema()

# COMMAND ----------

# DBTITLE 1,Validation
fact_sales.filter(col("order_id").isNull()).count()

# COMMAND ----------

fact_sales.join(payment_df, "order_id", "left_anti").count()

# COMMAND ----------

fact_sales.join(customer_df,"customer_id","left_anti").count()

# COMMAND ----------

# DBTITLE 1,Save fact_sales table
fact_sales.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/fact_sales").saveAsTable("ecomdb_catalog.gold.fact_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Payment Dimension Table

# COMMAND ----------

dim_payment = payment_df.select(
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
    "is_payment_valid"
).dropDuplicates(["order_id"])

# COMMAND ----------

dim_payment.groupBy("order_id").count().filter("count > 1").count()

# COMMAND ----------

display(dim_payment)

# COMMAND ----------

dim_payment.printSchema()

# COMMAND ----------

# DBTITLE 1,save dim_payment table
dim_payment.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/gold/dim_payment").saveAsTable("ecomdb_catalog.gold.dim_payment")

# COMMAND ----------

# MAGIC %md
# MAGIC # Final check

# COMMAND ----------

spark.sql("SHOW TABLES IN ecomdb_catalog.gold;").display()