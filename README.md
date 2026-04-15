This project demonstrates an end-to-end data engineering pipeline built using Databricks and PySpark, following the Medallion Architecture (Bronze → Silver → Gold).
The pipeline processes raw e-commerce data and transforms it into analytics-ready datasets for reporting and business insights.

Since this project only uses medallion architecture and Spark to transform raw data into analytical-ready OLAP tables, the raw data is uploaded directly to the Databricks Volume.

**Architecture**

Raw Data (CSV) -> Bronze Layer (Raw Data Ingestion) -> Silver Layer (Cleaning & Transformation) -> Gold Layer (Business Models / Star Schema) -> Analytics / BI (Power BI / SQL)

**Data Layers**

🥉 **Bronze Layer – Raw Ingestion**
- Ingests raw CSV data into Delta tables
- Preserves original structure
- Adds metadata columns:
    - _source_file
    - _ingested_at
- No transformations applied

🥈 **Silver Layer – Data Cleaning & Standardisation**
- Handles missing values
- Removes duplicates
- Standardises formats (dates, text, numeric fields)
- Fixes inconsistent values (e.g., category mappings, material names)
- Applies data quality checks

**🥇 Gold Layer – Business Transformation**
- Builds dimension tables:
- Customers
- Products
- Date
**Builds fact table:**
- Order Items
**Applies business logic:**
- Currency conversion to INR
- Region mapping (country → region)
- Derived metrics (gross, discount, net amount)
- Creates a denormalised view for analytics
