# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

catalog_name = 'ecommerce'

order_schema = T.StructType([
    T.StructField("dt",                  T.StringType(), True),
    T.StructField("order_ts",            T.StringType(), True),
    T.StructField("customer_id",         T.StringType(), True),
    T.StructField("order_id",            T.StringType(), True),
    T.StructField("item_seq",            T.StringType(), True),
    T.StructField("product_id",          T.StringType(), True),
    T.StructField("quantity",            T.StringType(), True),
    T.StructField("unit_price_currency", T.StringType(), True),
    T.StructField("unit_price",          T.StringType(), True),
    T.StructField("discount_pct",        T.StringType(), True),
    T.StructField("tax_amount",          T.StringType(), True),
    T.StructField("channel",             T.StringType(), True),
    T.StructField("coupon_code",         T.StringType(), True),
])

# COMMAND ----------

raw_path = '/Volumes/ecommerce/source_data/raw/order_items/landing/*.csv/'

df_orders = spark.read.csv(raw_path,schema=order_schema,header=True)
df_orders = df_orders.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

# COMMAND ----------

display(df_orders.limit(10))

# COMMAND ----------

df_orders.count()

# COMMAND ----------

df_orders.write.saveAsTable(f'{catalog_name}.bronze.brz_order_items',format='delta',mode='overwrite',mergeSchema=True)