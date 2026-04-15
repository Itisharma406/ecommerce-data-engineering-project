# Databricks notebook source
import pyspark.sql.types as T
import pyspark.sql.functions as F

# COMMAND ----------

catalog_name = 'ecommerce'

brand_schema = T.StructType([
    T.StructField('brand_code',T.StringType(),False),
    T.StructField('brand_name',T.StringType(),True),
    T.StructField('category_code',T.StringType(),True)
                ])


# df = spark.read.csv('/Volumes/ecommerce/source_data/raw/brands/brands.csv',header=True,inferSchema=True)
# df.show(3)

# COMMAND ----------

raw_data_path = '/Volumes/ecommerce/source_data/raw/brands/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=brand_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

display(df.limit(3))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write.saveAsTable(f"{catalog_name}.bronze.brz_brands",mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Drop table if exists ecommerce.bronze.brz_category;

# COMMAND ----------

category_schema = T.StructType([
    T.StructField('category_code',T.StringType(),False),
    T.StructField('category_name',T.StringType(),True),
    ])

raw_data_path = '/Volumes/ecommerce/source_data/raw/category/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=category_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

display(df.limit(3))


# COMMAND ----------

df.write.saveAsTable(f"{catalog_name}.bronze.brz_category",mode='overwrite',format='delta',mergeSchema=True)


# COMMAND ----------

customer_schema = T.StructType([
    T.StructField('customer_id',T.StringType(),False),
    T.StructField('phone',T.StringType(),True),
    T.StructField('country_code',T.StringType(),True),
    T.StructField('country',T.StringType(),True),
    T.StructField('state',T.StringType(),True),
    ])

raw_data_path = '/Volumes/ecommerce/source_data/raw/customers/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=customer_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

display(df.limit(3))

df.write.saveAsTable(f"{catalog_name}.bronze.brz_customer",mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

customer_schema = T.StructType([
    T.StructField('customer_id',T.StringType(),False),
    T.StructField('phone',T.StringType(),True),
    T.StructField('country_code',T.StringType(),True),
    T.StructField('country',T.StringType(),True),
    T.StructField('state',T.StringType(),True),
    ])

raw_data_path = '/Volumes/ecommerce/source_data/raw/customers/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=customer_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

display(df.limit(3))

df.write.saveAsTable(f"{catalog_name}.bronze.brz_customer",mode='overwrite',format='delta',mergeSchema=True)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ecommerce.bronze.brz_products

# COMMAND ----------

product_schema = T.StructType([
    T.StructField('product_id',T.StringType(),False),
    T.StructField('sku',T.StringType(),True),
    T.StructField('category_code',T.StringType(),True),
    T.StructField('brand_code',T.StringType(),True),
    T.StructField('color',T.StringType(),True),
    T.StructField('size',T.StringType(),True),
    T.StructField('material',T.StringType(),True),
    T.StructField('weight_grams',T.StringType(),True),
    T.StructField('length_cm',T.StringType(),True),
    T.StructField('width_cm',T.FloatType(),True),
    T.StructField('height_cm',T.FloatType(),True),
    T.StructField('rating',T.IntegerType(),True)
    ])

raw_data_path = '/Volumes/ecommerce/source_data/raw/products/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=product_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
    .withColumn('_ingested_at',F.current_timestamp())

display(df.limit(3))

df.write.saveAsTable(f"{catalog_name}.bronze.brz_products",mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

date_schema = T.StructType([
    T.StructField('date',T.StringType(),True),
    T.StructField('year',T.IntegerType(), True),
    T.StructField('day_name',T.StringType(), True),
    T.StructField('quarter',T.IntegerType(), True),
    T.StructField('week_of_year',T.IntegerType(), True)
])

raw_data_path = '/Volumes/ecommerce/source_data/raw/date/*.csv'

df = spark.read.csv(raw_data_path,header=True,schema=date_schema)

df = df.withColumn('_source_file',F.col('_metadata.file_path'))\
       .withColumn('_ingested_at',F.current_timestamp())

df.write.saveAsTable(f"{catalog_name}.bronze.brz_date",mode='overwrite',format='delta',mergeSchema=True)