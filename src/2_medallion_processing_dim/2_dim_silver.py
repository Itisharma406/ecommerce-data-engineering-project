# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.types as T

# COMMAND ----------

# MAGIC %md
# MAGIC #### Brand Table Cleanup

# COMMAND ----------

catalog_name = 'ecommerce'

df_bronze = spark.table(f"{catalog_name}.bronze.brz_brands")

df_bronze.show(10)

# COMMAND ----------

df_silver = df_bronze.withColumn('brand_name', F.trim(F.col('brand_name')))

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('brand_code',F.regexp_replace('brand_code',r'[^A-Za-z0-9]',''))

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

df_silver.select('category_code').distinct().show()

# COMMAND ----------

anomalies = {
    'GROCERY':'GRCY',
    'BOOKS':'BKS',
    'TOYS':'TOY'
}

df_silver = df_silver.replace(anomalies,subset='category_code')

df_silver.select('category_code').distinct().show()

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_brands',mode='overwrite',format='delta', mergeSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Category Table Cleanup

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_category')
df_bronze.show(10)

# COMMAND ----------

df_bronze.groupBy('category_code').count().filter(F.col('count')>1).show()

# COMMAND ----------

df_silver = df_bronze.dropDuplicates(['category_code'])
df_silver.groupBy('category_code').count().show()

# COMMAND ----------

df_silver = df_silver.withColumn('category_code',F.upper(F.col('category_code')))
df_silver.show()

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_category',mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Customer Table

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_customer')
display(df_bronze.limit(10))

# COMMAND ----------

df_bronze.select([F.sum(F.col(c).isNull().cast(T.IntegerType())).alias(c) for c in df_bronze.columns]).show()

# COMMAND ----------

df_silver = df_bronze.dropna(subset=['customer_id'])
df_silver.select([F.sum(F.col(c).isNull().cast(T.IntegerType())).alias(c) for c in df_bronze.columns]).show()

# COMMAND ----------

df_silver = df_silver.fillna('Not Available',subset=['phone'])
df_silver.select([F.sum(F.col(c).isNull().cast(T.IntegerType())).alias(c) for c in df_bronze.columns]).show()

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_customer',mode='overwrite',format='delta',mergeSchema=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Products Table

# COMMAND ----------

catalog_name = 'ecommerce'
df_bronze = spark.table(f'{catalog_name}.bronze.brz_products')
display(df_bronze.limit(5))

# COMMAND ----------

row_count,col_count = df_bronze.count(),len(df_bronze.columns)
print(row_count)
print(col_count)

# COMMAND ----------

df_silver = df_bronze.withColumn('category_code',F.upper(F.col('category_code'))).withColumn('brand_code',F.upper(F.col('brand_code')))

# COMMAND ----------

display(df_silver.limit(5))

# COMMAND ----------

df_silver = df_silver.withColumn('weight_grams',F.regexp_replace('weight_grams','g',''))
display(df_silver.limit(5))

# COMMAND ----------

df_silver = df_silver.withColumn('weight_grams',F.col('weight_grams').cast(T.FloatType()))


# COMMAND ----------

df_silver.select('weight_grams').show(5)

# COMMAND ----------

df_silver = df_silver.withColumn('length_cm',F.regexp_replace('length_cm',',','.').cast(T.FloatType()))
df_silver.select('length_cm').show(5)

# COMMAND ----------

df_silver.select('material').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn('material',
                                 F.when(F.col('material')=='Coton','Cotton')
                                  .when(F.col('material')=='Ruber','Rubber')
                                  .when(F.col('material')=='Alumium','Aluminium')
                                  .otherwise(F.col('material'))
)

# COMMAND ----------

df_silver.select('material').distinct().show()

# COMMAND ----------

df_silver = df_silver.withColumn('rating',F.when(F.col('rating')<0, F.abs(F.col('rating')))
                                            .otherwise(F.col('rating')))
display(df_silver.limit(5))

# COMMAND ----------

null_count = df_silver.select([F.sum(F.col(c).isNull().cast(T.IntegerType())).alias(c) for c in df_silver.columns])
null_count.display()

# COMMAND ----------

df_silver.select('weight_grams','length_cm','rating','category_code','material','brand_code').show(10)

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_products',format='delta',mergeSchema=True,overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Clean Date Table

# COMMAND ----------

df_bronze = spark.table(f'{catalog_name}.bronze.brz_date')
df_bronze.show(10)

# COMMAND ----------

df_bronze.printSchema()

# COMMAND ----------

df_silver = df_bronze.withColumn('date',F.to_date(F.col('date'),"dd-MM-yyyy"))
df_silver.show(10)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver.groupBy('date').count().filter(F.col('count')>1).show()

# COMMAND ----------

df_silver = df_silver.dropDuplicates(['date'])
df_silver.groupBy('date').count().filter(F.col('count')>1).show()

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('day_name',F.initcap(F.col('day_name')))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('week_of_year',F.abs(F.col('week_of_year')))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('quarter',F.concat(F.lit('Q'),F.col('quarter'),F.lit('-'),F.col('year')))
df_silver.show(10)

# COMMAND ----------

df_silver = df_silver.withColumn('week_of_year',F.concat(F.lit('Week'),F.col('week_of_year'),F.lit('-'),F.col('year')))
df_silver.show(10)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver = df_silver.withColumnRenamed('week_of_year','week')

# COMMAND ----------

df_silver.show(10)

# COMMAND ----------

df_silver.write.saveAsTable(f'{catalog_name}.silver.slv_date',format='delta',mode='overwrite',mergeSchema=True)

# COMMAND ----------

