# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

catalog_name = 'ecommerce'

df_silver = spark.table(f"{catalog_name}.silver.slv_order_items")
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.withColumn('gross_amount',F.col('unit_price')*F.col('quantity'))

df_gold = df_gold.withColumn('discount_amount',F.ceil((F.col('discount_pct')/100.0)*
                             F.col('gross_amount')))

df_gold = df_gold.withColumn('sale_amount',F.col('gross_amount')-F.col('discount_amount')+F.col("tax_amount"))

df_gold = df_gold.withColumn('date_id',F.date_format(F.col('dt'),'yyyyMMdd').cast(T.IntegerType()))

df_gold = df_gold.withColumn('coupon_flag',F.when(F.col('coupon_code').isNotNull(),1).otherwise(0))

display(df_gold.limit(5))

# COMMAND ----------

# --- 1) Define your fixed FX rates (as of 2025-10-15, like your PBI note) ---
fx_rates = {
    "INR": 1.00,
    "AED": 24.18,
    "AUD": 57.55,
    "CAD": 62.93,
    "GBP": 117.98,
    "SGD": 68.18,
    "USD": 88.29,
}

rates = [(k, float(v)) for k, v in fx_rates.items()]
print(rates)

# COMMAND ----------


rates_df = spark.createDataFrame(rates, ["currency", "inr_rate"])
rates_df.show()

# COMMAND ----------

df_gold = df_gold.join(
    rates_df,rates_df['currency']==F.upper(F.trim(F.col('unit_price_currency'))),'left')

display(df_gold.limit(10))

# COMMAND ----------

df_gold = df_gold.withColumn('sale_amount_inr',F.ceil(F.col('sale_amount')*F.col('inr_rate')))
display(df_gold.limit(10))

# COMMAND ----------

df_gold_final = df_gold.select(
    F.col("date_id"),
    F.col("dt").alias("transaction_date"),
    F.col("order_ts").alias("transaction_ts"),
    F.col("order_id").alias("transaction_id"),
    F.col("customer_id"),
    F.col("item_seq").alias("seq_no"),
    F.col("product_id"),
    F.col("channel"),
    F.col("coupon_code"),
    F.col("coupon_flag"),
    F.col("unit_price_currency"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("gross_amount"),
    F.col("discount_pct").alias("discount_percent"),
    F.col("discount_amount"),
    F.col("tax_amount"),
    F.col("sale_amount").alias("net_amount"),
    F.col("sale_amount_inr").alias("net_amount_inr")
)

# COMMAND ----------

df_gold_final.limit(10).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ecommerce.gold.gld_order_items;

# COMMAND ----------

df_gold_final.write.saveAsTable(f"{catalog_name}.gold.gld_fact_order_items",mode = 'overwrite', format = 'delta', mergeSchema = True)

# COMMAND ----------

spark.sql(f'Select count(*) from {catalog_name}.gold.gld_fact_order_items').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE OR REPLACE VIEW ecommerce.gold.fact_transactions_denorm AS(
# MAGIC
# MAGIC Select i.*,
# MAGIC d.date,
# MAGIC d.year,
# MAGIC d.month_name,
# MAGIC d.day_name,
# MAGIC d.is_weekend,
# MAGIC d.quarter,
# MAGIC d.week,
# MAGIC p.sku,
# MAGIC p.category_code,
# MAGIC p.category_name,
# MAGIC p.brand_code,
# MAGIC p.brand_name,
# MAGIC p.color,
# MAGIC p.size,
# MAGIC p.rating,
# MAGIC c.country,
# MAGIC c.state,
# MAGIC c.phone,
# MAGIC c.region,
# MAGIC extract(HOUR from i.transaction_ts) as hour_of_day
# MAGIC from ecommerce.gold.gld_fact_order_items as i
# MAGIC join ecommerce.gold.gld_dim_date as d
# MAGIC on i.date_id = d.date_id
# MAGIC join ecommerce.gold.gld_dim_products as p
# MAGIC on i.product_id = p.product_id
# MAGIC join ecommerce.gold.gld_dim_customers as c
# MAGIC on i.customer_id = c.customer_id
# MAGIC );

# COMMAND ----------

