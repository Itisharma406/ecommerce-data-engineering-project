This project demonstrates an end-to-end data engineering pipeline built using Databricks and PySpark, following the Medallion Architecture (Bronze → Silver → Gold).
The pipeline processes raw e-commerce data and transforms it into analytics-ready datasets for reporting and business insights.

Since this project only covers the use of medallion architecture and Spark to transform the raw data into analytical-ready OLAP tables, the raw data is directly uploaded to the Databricks Volume.

**Architecture **
Raw Data (CSV, Raw Ingestion into Volume) -> Bronze Layer (Schema fix) -> Silver Layer (Cleaning & Transformation) -> Gold Layer (Business Models / Star Schema) -> Analytics / BI (Power BI / SQL)


