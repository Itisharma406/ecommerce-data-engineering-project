# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show databases from ecommerce;

# COMMAND ----------

# MAGIC %md
# MAGIC