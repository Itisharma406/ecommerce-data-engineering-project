# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Row

# COMMAND ----------

catalog_name = 'ecommerce'

# COMMAND ----------

df_products = spark.table(f'{catalog_name}.silver.slv_products')
df_category = spark.table(f'{catalog_name}.silver.slv_category')
df_brand = spark.table(f'{catalog_name}.silver.slv_brands')

# COMMAND ----------

display(df_products.limit(5))

# COMMAND ----------

df_products.createOrReplaceTempView('v_products')
df_category.createOrReplaceTempView('v_category')
df_brand.createOrReplaceTempView('v_brand')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from v_products limit 5;
# MAGIC

# COMMAND ----------

spark.sql(f"USE CATALOG {catalog_name}")

# COMMAND ----------

display(df_brand.limit(5))

# COMMAND ----------

display(df_category.limit(5))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.gld_dim_products AS
# MAGIC
# MAGIC WITH brand_categories AS(
# MAGIC   Select b.brand_code, b.brand_name, c.category_code, c.category_name
# MAGIC   From v_brand b
# MAGIC   Inner Join v_category c
# MAGIC   on b.category_code = c.category_code
# MAGIC )
# MAGIC Select
# MAGIC   p.product_id,
# MAGIC   p.sku,
# MAGIC   p.category_code,
# MAGIC   COALESCE(bc.category_name, 'Not Available') as category_name,
# MAGIC   p.brand_code,
# MAGIC   COALESCE(bc.brand_name, 'Not Available') as brand_name,
# MAGIC   p.color,
# MAGIC   p.size,
# MAGIC   p.material,
# MAGIC   p.weight_grams,
# MAGIC   p.length_cm,
# MAGIC   p.width_cm,
# MAGIC   p.height_cm,
# MAGIC   p.rating,
# MAGIC   p._source_file,
# MAGIC   p._ingested_at
# MAGIC FROM v_products p
# MAGIC LEFT JOIN brand_categories bc
# MAGIC on p.brand_code = bc.brand_code;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Customers

# COMMAND ----------

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "East", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  


# COMMAND ----------

country_state_map

# COMMAND ----------

# 1 Flatten country_state_map into a list of Rows
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]       

# COMMAND ----------

# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)

# Optional: show mapping
df_region_mapping.show(truncate=False)

# COMMAND ----------

df_silver = spark.table(f'{catalog_name}.silver.slv_customer')
display(df_silver.limit(5))

# COMMAND ----------

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})

display(df_gold.limit(5))

# COMMAND ----------

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.gold.gld_dim_customers")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date/Calendar

# COMMAND ----------

df_date = spark.table(f'{catalog_name}.silver.slv_date')
df_date.show(5)

# COMMAND ----------

df_gold = df_date.withColumn('date_id',F.date_format(F.col('date'),'yyyyMMdd').cast('int'))
df_gold = df_gold.withColumn('month_name',F.date_format(F.col('date'),'MMMM'))
df_gold = df_gold.withColumn(
    'is_weekend',F.when(F.col('day_name').isin('Saturday','Sunday'),1)
                  .otherwise(0))

display(df_gold.limit(5))

# COMMAND ----------

desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# COMMAND ----------

df_gold.write.saveAsTable(f'{catalog_name}.gold.gld_dim_date',format='delta',mode='overwrite',mergeSchema=True)

# COMMAND ----------

