# Databricks notebook source
# MAGIC %md
# MAGIC # Customer table

# COMMAND ----------

# DBTITLE 1,Read bronze customer table
customer_df=spark.read.table("ecomdb_catalog.bronze.customer")

# COMMAND ----------

# DBTITLE 1,Data preview
display(customer_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Check data quality
customer_df.printSchema()

# COMMAND ----------

# DBTITLE 1,remove Duplicates
from pyspark.sql.functions import*
clean_customer_df=customer_df.dropDuplicates(["customer_unique_id"])

# COMMAND ----------

# DBTITLE 1,Compare before & after count
bronze_count=customer_df.count()
silver_count=clean_customer_df.count()
print("bronze_count:",bronze_count)
print("silver_count:",silver_count)


# COMMAND ----------

# DBTITLE 1,Column Standardize
cleaned_customer_df=clean_customer_df.withColumn("customer_city",lower(trim(col("customer_city"))))\
    .withColumn("customer_state",upper(trim(col("customer_state"))))\
     .withColumn("customer_zip_code_prefix",trim(col("customer_zip_code_prefix")))\
    .withColumn("customer_id",trim(col("customer_id")))

display(cleaned_customer_df)

# COMMAND ----------

# DBTITLE 1,check null
cleaned_customer_df.filter(col("customer_id").isNull() |
        col("customer_unique_id").isNull()|
        col("customer_zip_code_prefix").isNull()|
        col("customer_city").isNull()|
        col("customer_state").isNull()
         ).display()

# COMMAND ----------

# DBTITLE 1,Write to ADLS
cleaned_customer_df.write.format("delta").mode("overwrite").option("overwriteSchema","true").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/customer").saveAsTable("ecomdb_catalog.silver.customer")

# COMMAND ----------

# MAGIC %md
# MAGIC # order table

# COMMAND ----------

# DBTITLE 1,read order table
order_df=spark.read.table("ecomdb_catalog.bronze.order")

# COMMAND ----------

# DBTITLE 1,data preview
display(order_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Schema validation
order_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Remove duplicate
clean_order_df=order_df.dropDuplicates(["order_id"])

# COMMAND ----------

# DBTITLE 1,compare before and after count
bronze_count=order_df.count()
silver_count=clean_order_df.count()
print("bronze_count:",bronze_count)
print("silver_count:",silver_count)

# COMMAND ----------

# DBTITLE 1,column standardize
from pyspark.sql.functions import*
cleaned_order_df=clean_order_df.withColumn("order_status",lower(trim(col("order_status"))))\
    .withColumn("order_id",trim(col("order_id")))\
    .withColumn("customer_id",trim(col("customer_id")))
display(cleaned_order_df.limit(10))
                          

# COMMAND ----------

# DBTITLE 1,Null check
cleaned_order_df.filter(col("order_id").isNull() |
        col("customer_id").isNull()|
        col("order_status").isNull()|
        col("order_purchase_timestamp").isNull()|
        col("order_approved_at").isNull() |
        col("order_delivered_carrier_date").isNull() |
        col("order_delivered_customer_date").isNull() |
        col("order_estimated_delivery_date").isNull()
         ).display()

# COMMAND ----------

# DBTITLE 1,groupBy order status
cleaned_order_df.groupBy("order_status").count().display()

# COMMAND ----------

# DBTITLE 1,add new column
from pyspark.sql.functions import*
cleaned_order_df = cleaned_order_df.withColumn("status_group",
    when(col("order_status") == "created","CREATED")
    .when(col("order_status").isin("approved","invoiced"),"CONFIRMED")
    .when(col("order_status") == "processing", "PROCESSING")
    .when(col("order_status") == "shipped", "SHIPPED")
    .when(col("order_status") == "delivered", "COMPLETED")
    .when(col("order_status") == "canceled", "CANCELLED")
    .when(col("order_status") == "unavailable", "FAILED")
    .otherwise("UNKNOWN")
)
display(cleaned_order_df)

# COMMAND ----------

# DBTITLE 1,add is_time_sequence_valid
from pyspark.sql.functions import *
orders_df = cleaned_order_df.withColumn(
    "is_time_sequence_valid",
    when(
        (col("order_status") == "delivered") &
        (col("order_delivered_customer_date").isNotNull()) &
        (col("order_delivered_customer_date") >= col("order_purchase_timestamp")),
        True
    )
    .when(
        (col("order_status") == "shipped") &
        (col("order_delivered_carrier_date").isNotNull()) &
        (col("order_delivered_carrier_date") >= col("order_purchase_timestamp")),
        True
    )
    .when(
        col("order_status").isin(
            "canceled", "unavailable", "processing", "created", "approved", "invoiced"
        ),
        True
    )
    .otherwise(False)
)

display(orders_df)

# COMMAND ----------

orders_df.groupBy("is_time_sequence_valid").count().display()

# COMMAND ----------

orders_df.filter(col("is_time_sequence_valid") == False) \
    .select("order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_delivered_carrier_date") \
    .display()

# COMMAND ----------

# DBTITLE 1,add column data_quality_issue
orders_df = orders_df.withColumn(
    "data_quality_issue",
    when(
        (col("order_status") == "delivered") &
        (col("order_delivered_customer_date").isNull()),
        "MISSING_DELIVERY_TIMESTAMP"
    ).when(
        (col("order_status") == "shipped") &
        (col("order_delivered_carrier_date") < col("order_purchase_timestamp")),
        "INVALID_TIMESTAMP_ORDER"
    ).otherwise("OK")
)
display(orders_df)

# COMMAND ----------

# DBTITLE 1,Invalid orders
invalid_orders = orders_df.filter(col("is_time_sequence_valid") == False).display()

# COMMAND ----------

# DBTITLE 1,add new column delivery days
from pyspark.sql.functions import*
orders_df = orders_df.withColumn("delivery_days",
    datediff(col("order_delivered_customer_date"),col("order_purchase_timestamp"))
)

# COMMAND ----------

orders_df = orders_df.withColumn( "delivery_delay_days",
    datediff(col("order_delivered_customer_date"),
            col("order_estimated_delivery_date"))
)

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

orders_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/order").saveAsTable("ecomdb_catalog.silver.order")

# COMMAND ----------

# MAGIC %md
# MAGIC # Products data

# COMMAND ----------

# DBTITLE 1,read product table
product_df=spark.read.table("ecomdb_catalog.bronze.product")

# COMMAND ----------

display(product_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Schema validation
product_df.printSchema()

# COMMAND ----------

# DBTITLE 1,check duplicates
product_df.groupBy("product_id").count().filter("count > 1").display()

# COMMAND ----------

# DBTITLE 1,Null check
product_df.filter(col("product_id").isNull()|
        col("product_category_name").isNull()|
        col("product_name_lenght").isNull()|
        col("product_description_lenght").isNull()|
        col("product_photos_qty").isNull()|
        col("product_weight_g").isNull()|
        col("product_length_cm").isNull()|
        col("product_height_cm").isNull()|
        col("product_width_cm").isNull()
).display()



# COMMAND ----------

# DBTITLE 1,Standardize category names
from pyspark.sql.functions import lower, trim, col
product_df =product_df.withColumn(
    "product_category_name",
    lower(trim(col("product_category_name")))
)\
    .withColumn("product_id",trim(col("product_id")))
display(product_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Cell 36
from pyspark.sql.functions import *
clean_product_df = product_df.withColumn(
    "is_product_complete",
    when(
        (col("product_category_name").isNotNull()) &
        (col("product_name_lenght").isNotNull()) &
        (col("product_description_lenght").isNotNull()) &
        (col("product_photos_qty").isNotNull()),
        True
    ).otherwise(False)
)
display(clean_product_df.groupBy("is_product_complete").count())

# COMMAND ----------

# DBTITLE 1,Cell 38
from pyspark.sql.functions import col
cleaned_product_df =clean_product_df.withColumn(
    "product_volume_cm3",
    col("product_length_cm") *
    col("product_height_cm") *
    col("product_width_cm")
)
display(cleaned_product_df.limit(10))

# COMMAND ----------

cleaned_product_df.printSchema()
cleaned_product_df.count()

# COMMAND ----------

# DBTITLE 1,Write  data to ADLS
cleaned_product_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/product").saveAsTable("ecomdb_catalog.silver.product")

# COMMAND ----------

# MAGIC %md
# MAGIC # order item data

# COMMAND ----------

# DBTITLE 1,read order item table
order_item_df=spark.read.table("ecomdb_catalog.bronze.order_item")


# COMMAND ----------

# DBTITLE 1,data preview
display(order_item_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Schema validation
order_item_df.printSchema()

# COMMAND ----------

# DBTITLE 1,remove duplicates
clean_order_item_df=order_item_df.dropDuplicates(["order_id","order_item_id"])

# COMMAND ----------

# DBTITLE 1,Column Standardize
c_order_item_df = clean_order_item_df.withColumn("order_id",trim(col("order_id")))\
    .withColumn("product_id",trim(col("product_id")))\
        .withColumn("seller_id",trim(col("seller_id")))
display(c_order_item_df)                    

# COMMAND ----------

# DBTITLE 1,compare before and after count
bronze_count=order_item_df.count()
silver_count=c_order_item_df.count()
print("bronze_count:",bronze_count)
print("silver_count:",silver_count)

# COMMAND ----------

# DBTITLE 1,null check
clean_order_item_df.filter(col("order_id").isNull() |
        col("order_item_id").isNull()|
        col("product_id").isNull()|
        col("seller_id").isNull()|
        col("shipping_limit_date").isNull()|
        col("price").isNull()|
        col("freight_value").isNull()
         ).display()

# COMMAND ----------

# DBTITLE 1,add new colunm total_revenue
from pyspark.sql.functions import col,round
cleaned_order_item_df=c_order_item_df.withColumn("total_revenue", round(col("price") + col("freight_value"),2))
display(cleaned_order_item_df)

# COMMAND ----------

# DBTITLE 1,write data to ADLS
cleaned_order_item_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/order_item").saveAsTable("ecomdb_catalog.silver.order_item")

# COMMAND ----------

# MAGIC %md
# MAGIC # Payment data

# COMMAND ----------

# DBTITLE 1,read payment table
payment_df=spark.read.table("ecomdb_catalog.bronze.payment")

# COMMAND ----------

# DBTITLE 1,Data preview
display(payment_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Schema validation
payment_df.printSchema()

# COMMAND ----------

# DBTITLE 1,remove duplicates
clean_payment_df=payment_df.dropDuplicates(["order_id","payment_sequential"])

# COMMAND ----------

# DBTITLE 1,compare before and after count
bronze_count=payment_df.count()
silver_count=clean_payment_df.count()
print("bronze_count:",bronze_count)
print("silver_count:",silver_count)

# COMMAND ----------

# DBTITLE 1,Null check
from pyspark.sql.functions import col
clean_payment_df.filter(col("order_id").isNull() |
        col("payment_sequential").isNull()|
        col("payment_type").isNull()|
        col("payment_installments").isNull()|
        col("payment_value").isNull()
         ).display()

# COMMAND ----------

# DBTITLE 1,Column Standardize
from pyspark.sql.functions import lower, trim, col
cleaned_payment_df = clean_payment_df.withColumn(
    "payment_type",
    lower(trim(col("payment_type"))))\
        .withColumn("order_id",trim(col("order_id")))
display(cleaned_payment_df)

# COMMAND ----------

from pyspark.sql.functions import *
silver_payment_df = cleaned_payment_df.withColumn(
    "is_payment_valid",
    when(
        col("payment_value") > 0,
        True
    ).otherwise(False)
)
display(silver_payment_df.groupBy("is_payment_valid").count())

# COMMAND ----------

silver_payment_df.filter(col("is_payment_valid")=="false").display()


# COMMAND ----------

# MAGIC %md
# MAGIC **Data Quality Observations**
# MAGIC
# MAGIC 9 payment records have payment_value <= 0
# MAGIC Payment types are primarily voucher and not_defined
# MAGIC Records were not deleted because they may represent legitimate business transactions
# MAGIC is_payment_valid flag was added for reporting and Gold-layer filtering if needed

# COMMAND ----------

silver_payment_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/payment").saveAsTable("ecomdb_catalog.silver.payment")

# COMMAND ----------

# MAGIC %md
# MAGIC # Review data

# COMMAND ----------

# DBTITLE 1,read review table
review_df=spark.read.table("ecomdb_catalog.bronze.review")

# COMMAND ----------

# DBTITLE 1,Data Preview
display(review_df.limit(10))

# COMMAND ----------

# DBTITLE 1,Schema Validation
review_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

for c in review_df.columns:
    print(c, review_df.filter(col(c).isNull()).count())

# COMMAND ----------

# DBTITLE 1,remove duplicates
clean_review_df=review_df.dropDuplicates(["review_id","order_id"])

# COMMAND ----------

# DBTITLE 1,Column Standardize
c_review_df = clean_review_df.withColumn("order_id",trim(col("order_id")))\
            .withColumn("review_id",trim(col("review_id")))\
            .withColumn("review_comment_title",trim(col("review_comment_title")))\
            .withColumn("review_comment_message",trim(col("review_comment_message")))
            

# COMMAND ----------

# DBTITLE 0,compare before and after count
bronze_count=review_df.count()
silver_count=c_review_df.count()
print("bronze_count:",bronze_count)
print("silver_count:",silver_count)

# COMMAND ----------

# DBTITLE 1,added new column based on review_score
from pyspark.sql.functions import when, col
silver_review_df = c_review_df.withColumn(
    "review_sentiment",
     when(col("review_score") >= 4, "positive")
    .when(col("review_score") == 3, "neutral")
    .otherwise("negative")
)
display(silver_review_df)

# COMMAND ----------

silver_review_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/review").saveAsTable("ecomdb_catalog.silver.review")

# COMMAND ----------

# MAGIC %md
# MAGIC # Category translation

# COMMAND ----------

# DBTITLE 1,read table
category_translation_df=spark.read.table("ecomdb_catalog.bronze.category_translation")

# COMMAND ----------

# DBTITLE 1,data preview
display(category_translation_df)

# COMMAND ----------

# DBTITLE 1,Schema validation
category_translation_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Null check
category_translation_df.filter(col("product_category_name").isNull()|
        col("product_category_name_english").isNull()
        ).display()

# COMMAND ----------

# DBTITLE 1,Column Standardize
c_translation_df = category_translation_df.withColumn("product_category_name",trim(col("product_category_name")))\
    .withColumn("product_category_name_english",trim(col("product_category_name_english")))

# COMMAND ----------

# DBTITLE 1,Name standardize
silver_prod_categ_df = c_translation_df.withColumnRenamed(
    "product_category_name","category_name"
).withColumnRenamed(
    "product_category_name_english",
    "category_name_en"
)
display(silver_prod_categ_df)

# COMMAND ----------

# DBTITLE 1,write data to ADLS
silver_prod_categ_df.write.format("delta").mode("overwrite").option("path","abfss://ecom-lakehouse@datalakeecom.dfs.core.windows.net/silver/category_name").saveAsTable("ecomdb_catalog.silver.category_name")