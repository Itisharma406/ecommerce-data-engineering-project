# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

catalog_name = 'ecommerce'

df_bronze = spark.table(f'{catalog_name}.bronze.brz_order_items')
display(df_bronze.limit(10))

# COMMAND ----------

df_bronze.select('quantity').distinct().show()

# COMMAND ----------

df_silver = df_bronze.withColumn('quantity',F.when(F.col('quantity')=='Two',2)\
                     .otherwise(F.col('quantity')))
df_silver.select('quantity').distinct().show()

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

df_silver = df_silver.withColumn('discount_pct',F.regexp_replace(F.col('discount_pct'),'%','').cast(T.DoubleType()))

df_silver = df_silver.withColumn('unit_price',F.regexp_replace(F.col('unit_price'),'[$]','').cast(T.DoubleType()))

df_silver = df_silver.withColumn('coupon_code',F.lower(F.trim(F.col('coupon_code'))))
display(df_silver.limit(10))

# COMMAND ----------

df_silver.select('channel').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn('channel',F.when(F.col('channel')=='web','Website')
                                 .when(F.col('channel')=='app','Mobile')
                                 .otherwise(F.col('channel')))
df_silver.select('channel').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn('dt',F.to_date(F.col('dt'),'yyyy-MM-dd'))
df_silver = df_silver.withColumn('order_ts',F.coalesce(
    F.to_timestamp(F.col('order_ts'),'yyyy-MM-dd HH:mm:ss'),
    F.to_timestamp(F.col('order_ts'),'dd-MM-yyyy HH;MM')
    ))

# COMMAND ----------

df_silver = df_silver.withColumn('item_seq',F.col('item_seq').cast(T.IntegerType()))
df_silver = df_silver.withColumn('tax_amount',F.col('tax_amount').cast(T.DoubleType()))

# COMMAND ----------

display(df_silver.limit(10))

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_order_items',mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

