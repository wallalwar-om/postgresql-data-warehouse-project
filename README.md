# PostgreSQL Data Warehouse Project

This project is a **PostgreSQL-based implementation** of a modern data warehouse using the **Medallion Architecture** (Bronze → Silver → Gold), inspired by [Baraa’s SQL Server Data Warehouse Project](https://github.com/DataWithBaraa/sql-data-warehouse-project).

---
## Prerequisites
- PostgreSQL 15+ installed and running
- `psql` command-line tool available in PATH
- Windows (for batch file) or ability to run SQL scripts manually
- CSV files placed in the correct datasets/ folders

##  Project Structure
```
datasets/
├── source_crm/
│ ├── cust_info.csv
│ ├── prd_info.csv
│ └── sales_details.csv
└── source_erp/
├── CUST_AZ12.csv
├── LOC_A101.csv
└── PX_CAT_G1V2.csv

scripts/
├── create_schema.sql
├── drop_create.sql
├── setup_all.sql
├── bronze/
│ ├── ddl_bronze.sql
│ └── proc_load_bronze.sql
├── silver/
│ ├── ddl_silver.sql
│ └── proc_load_silver.sql
└── gold/
└── ddl_gold.sql

tests/
└── (testing scripts if applicable)

README.md
init_db.bat (Batch file to run all SQL scripts in sequence)
```

##  Project Overview

This project demonstrates the design and implementation of a **layered data warehouse** using PostgreSQL. It comprises:

- **Bronze Layer** — Ingests raw CSV data from ERP and CRM sources with minimal transformations for traceability.
- **Silver Layer** — Cleans, standardizes, and applies business logic (e.g., formatting, normalization).
- **Gold Layer** — Delivers business-ready, analytics-optimized data via a star schema, including fact and dimension tables.

---

/*
===============================================================================
Data Warehouse Setup Script – Medallion Architecture
===============================================================================
This script sets up a Data Warehouse using the **Medallion Architecture**,
a layered data design pattern used in modern data engineering pipelines.

Layers:
1. Bronze Layer:
    - Stores raw, unprocessed data.
    - Acts as the single source of truth.
    - Ingested directly from source systems.
    - DDL: scripts/bronze/ddl_bronze.sql
    - Procedures: scripts/bronze/proc_load_bronze.sql

2. Silver Layer:
    - Stores cleaned, transformed, and standardized data.
    - Integrates and enriches Bronze data.
    - Used for downstream analytics.
    - DDL: scripts/silver/ddl_silver.sql
    - Procedures: scripts/silver/proc_load_silver.sql

3. Gold Layer:
    - Stores business-ready data (facts and dimensions in Star Schema).
    - Optimized for BI dashboards and reporting.
    - DDL: scripts/gold/ddl_gold.sql

Execution Order:
\i scripts/drop_create.sql
\i scripts/create_schema.sql
\i scripts/bronze/ddl_bronze.sql
\i scripts/bronze/proc_load_bronze.sql
\i scripts/silver/ddl_silver.sql
\i scripts/silver/proc_load_silver.sql
\i scripts/gold/ddl_gold.sql
===============================================================================
*/

##  Naming Conventions

**Table Naming:**

- **Bronze & Silver Layers:** `sourcesystem_entity`  
  - Example: `crm_customer_info`  
- **Gold Layer (Modelled):** `<category>_<entity>`  
  - `dim_customers`, `fact_sales`, `agg_sales_monthly`

**Column Naming:**

- **Surrogate Keys (in dimensions):** `<table_name>_key`  
  - Example: `customer_key` in `dim_customers`  
- **Technical Columns:** `dwh_<attribute>`  
  - Example: `dwh_load_date` — indicates when the row was loaded

**Stored Procedures:**

- Use the pattern `load_<layer>` for ETL routines.  
  - Examples: `load_bronze`, `load_silver`, or `load_gold`

---

##  How It Works

1. **Schema Initialization**  
   Run `init_db.bat` (Windows) or execute `setup_all.sql` directly:  
   - Runs `drop_create.sql`  
   - Creates schema via `create_schema.sql`

2. **Data Ingestion (Bronze Layer)**  
   - Executed using `ddl_bronze.sql` & `proc_load_bronze.sql`

3. **Cleansing & Transformation (Silver Layer)**  
   - Executed using `ddl_silver.sql` & `proc_load_silver.sql`

4. **Modeling & Analytics (Gold Layer)**  
   - Executed via `ddl_gold.sql` to establish fact and dimension tables

5. **ETL Automation**  
   - A batch script, `init_db.bat`, sequences and automates all steps for a full pipeline run in one go.

---

##  What I Learned

- Migrated a complete ETL pipeline design from SQL Server to PostgreSQL.
- Built stored procedures with exception handling for reliable ETL execution.
- Modeled fact and dimension tables for optimized analytical queries.
- Automated end-to-end pipeline execution using batch scripting.



---

##  Execution

```bash
# On Windows
init_db.bat

# Or manually in PostgreSQL shell

\i scripts/drop_create.sql
\c your_database_name  -- reconnect to the newly created DB
\i scripts/create_schema.sql

\i scripts/bronze/ddl_bronze.sql
\i scripts/bronze/proc_load_bronze.sql
\i scripts/silver/ddl_silver.sql
\i scripts/silver/proc_load_silver.sql
\i scripts/gold/ddl_gold.sql

# Using vscode
Run the init_db.bat file.
```

### Credits
Inspired by Baraa’s SQL Server Data Warehouse Project. This version is a fully re-implemented PostgreSQL pipeline—built from scratch with custom logic and improvements.




