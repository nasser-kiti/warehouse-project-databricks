# Data Warehousing Project (Databricks)

This project demonstrates how to build a comprehensive data warehousing solution using Databricks with Apache Airflow for pipeline orchestration and DBT for data trasnformation.

## Table of Contents

- [Project Overview](#project-overview)
- [Project Requirements](#project-requirements)
- [Data Architecture](#data-architecture)
- [Technology Stack](#technology-stack)
- [Getting Started](#getting-started)
- [Data Loading Pipeline](#data-loading-pipeline)
- [Data Quality Checks](#data-quality-checks)

---

## Project Overview

This project involves:

- **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture with Bronze, Silver, and Gold layers in Databricks
- **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
- **Data Quality**: Implementing comprehensive data quality checks at each layer of the pipeline.

---

## Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective

Developing a modern data warehouse with SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

##### Business Rules

- Business started on 1990-01-01
- All sales data should always be positive; no negatives, zeros or nulls.
  - If the sales figure is null, negative or 0, derive it using quantity and price
  - If the price is 0 or null, derive it using sales and quantity
  - If the price is negative, convert it to a positive value

#### Specifications

- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues before analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### Data Architecture

The architecture of choice was the Medallion Architecture, split into three layers: Bronze, Silver, and Gold layers.

- **The Bronze Layer**: This layer stores raw data from the source systems in its original form. The data is ingested from CSV Files into a SQL Server Database
- **Silver Layer**: The data cleansing, standardisation, and normalisation processes are done in this layer to prepare data for analysis.
- **Gold Layer**: This layer is where business-ready data resides. The data is modelled into a star schema as required for reporting and analytics.

![Data architecture diagram](assets/docs/data_architecture_diagram.png)

#### Technology Stack

- **OS**: macOS
- **IDE**: VS Code
- **Database**: SQL Server / Azure SQL Edge
- **Containerization**: Docker Desktop
- **Data Ingestion**: CSV files from source systems

#### Docker Setup Instructions

To create and run an Azure SQL Edge container with data persistence:

```bash
# Create a persistent volume for SQL Server data
docker volume create sqlserver_data

# Run the Azure SQL Edge container
docker run -e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=Your_Passw0rd" \
  -p 1433:1433 --name sqlserver \
  -v sqlserver_data:/var/opt/mssql \
  -v /Users/example/path/to/your/data:/var/opt/source \
  -d mcr.microsoft.com/azure-sql-edge:latest

# Start the container after it's been stopped
docker start sqlserver
```

> **Note**: Replace `/Users/example/path/to/your/data` with the actual path to your CSV data files.

---

## Getting Started

### Prerequisites

- Docker Desktop installed and running
- SQL Server connection tool (e.g., SQL Server Management Studio, VS Code SQL Extension)
- Access to the source CSV files in `assets/data/`

### Setup Steps

1. **Initialize Docker Container**

   ```bash
   docker start sqlserver
   ```

2. **Create the Database**

   ```sql
   -- Execute init_database.sql to create the warehouse database
   ```

3. **Initialize Warehouse Schema**

   ```sql
   -- Execute init_warehouse.sql to set up the schema
   ```

4. **Load Data to Bronze Layer**

   ```sql
   -- Execute bronze/data_loading.sql to ingest raw CSV data
   ```

5. **Transform Data to Silver Layer**

   ```sql
   -- Execute silver/data_loading.sql to cleanse and standardise data
   ```

6. **Build Gold Layer**

   ```sql
   -- Execute gold/dimension_customers.sql
   -- Execute gold/dimension_products.sql
   -- Execute gold/fact_sales.sql
   ```

---

## Data Loading Pipeline

The ETL pipeline follows the Medallion Architecture pattern:

### Bronze Layer (Raw Ingestion)

- **Purpose**: Store raw data from source systems in its original form
- **Process**: CSV files are bulk-loaded into bronze tables without transformation
- **Files**:
  - `bronze/ddl_bronze.sql`: Creates raw data tables
  - `bronze/data_loading.sql`: Loads CSV data into bronze tables

### Silver Layer (Cleansing & Standardisation)

- **Purpose**: Clean, standardise, and normalise data for downstream analysis
- **Process**: Apply business rules and data quality transformations
- **Key Transformations**:
  - Handle null, negative, or zero sales values
  - Standardise data types and formats
  - Resolve duplicate and conflicting records
- **Files**:
  - `silver/ddl_silver.sql`: Creates cleansed data tables
  - `silver/data_loading.sql`: Main transformation logic
  - `silver/data_exploration.sql`: EDA and validation queries
  - Subfolders for each data source (CRM and ERP)

### Gold Layer (Analytics-Ready)

- **Purpose**: Create business-ready data models for reporting and analytics
- **Schema**: Star schema with dimensions and facts
- **Tables**:
  - `dimension_customers`: Customer master data
  - `dimension_products`: Product master data
  - `fact_sales`: Transactional sales facts
- **Files**: `gold/dimension_customers/`, `gold/dimension_products/`, `gold/fact_sales/`

---

## Data Quality Checks

Quality assurance is built into each layer of the pipeline:

- **Bronze Quality Checks**: Validate row counts, detect schema mismatches
- **Silver Quality Checks**:
  - Verify data completeness (no unexpected nulls)
  - Validate business rules (e.g., positive sales amounts)
  - Check for data consistency between sources
- **Gold Quality Checks**: Ensure dimensional integrity and fact consistency

Quality check scripts are located in each data source subfolder:

- `quality_check_bronze.sql`: Validates bronze data
- `quality_check_silver.sql`: Validates silver transformations

---

## Data Sources

### CRM Source System

- **cust_info.csv**: Customer information
- **prd_info.csv**: Product information
- **sales_details.csv**: Orders/Sales information

### ERP Source System

- **CUST_AZ12.csv**:  Customer biodata
- **LOC_A101.csv**: Customer location data
- **PX_CAT_G1V2.csv**: Product categorisations

---

## Business Rules

The warehouse enforces the following business rules:

- **Business Start Date**: 1990-01-01
- **Sales Data Validation**: All sales amounts must be positive
  - If the sales figure is null, negative, or zero, then derive its value from quantity × price
  - If price is null or zero, then derive its value from sales ÷ quantity
  - If price is negative, convert it to a positive value
- **Data Scope**: Only the latest dataset is loaded; historization is not required
- **Time Dimension**: Focus on current-state analytics

---

## Documentation

### Data Model & Standards

- [Data Naming Conventions](assets/docs/naming_conventions.md): Field and table naming standards
- [Data Catalog](gold/docs/data_catalog.md): Business glossary and data model documentation

### Diagrams & Visual Guides

- [Architecture Diagram](assets/docs/data_architecture_diagram.png): Visual representation of the medallion architecture
- [Integration Diagram](silver/docs/data_integration_diagram.png): System integration and data source connections
- Data Flow Diagrams: End-to-end data movement and transformation flow: [bronze layer](bronze/docs/bronze_dataflow_diagram.png), [silver layer](silver/docs/silver_data_flow_diagram.png), [gold layer](gold/docs/gold_data_flow_diagram.png)
- [Data Model](gold/docs/star_schema_data_model_diagram.png)

---

## Troubleshooting

### Docker Volume & File Access Issues

If you encounter issues with bulk inserting CSV files:

1. **Problem**: Files not accessible from the container
   - **Solution**: Mount the data directory as a volume (see setup above)
   - Replace the path in the Docker command with your actual data location

2. **Problem**: Container won't start

   ```bash
   # Check container status
   docker ps -a

   # View logs
   docker logs sqlserver

   # Remove and recreate if needed
   docker rm sqlserver
   ```

3. **Problem**: Connection refused
   - Verify the container is running: `docker ps`
   - Check that port 1433 is available and not blocked
   - Verify credentials match in connection string
