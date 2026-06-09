# Brazilian E-Commerce Lakehouse

## Project Overview

This project demonstrates the implementation of an end-to-end Azure Databricks Lakehouse solution using the Medallion Architecture (Bronze, Silver, Gold).

The solution ingests Brazilian E-Commerce data from AWS S3, processes and transforms data using PySpark in Azure Databricks, stores Delta tables in Azure Data Lake Storage Gen2, manages governance through Unity Catalog, orchestrates pipelines using Databricks Workflows, serves data through Databricks SQL Warehouse, and visualizes insights using Power BI.

---

## Architecture Diagram
![Architecture](archietecture.png)


### Architecture Flow

```text
AWS S3
   ↓
Bronze Layer (Raw Data)
   ↓
Silver Layer (Cleaned & Enriched Data)
   ↓
Gold Layer (Star Schema & Business Metrics)
   ↓
Databricks SQL Warehouse
   ↓
Power BI Dashboard
```

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Source | AWS S3 |
| Cloud Platform | Microsoft Azure |
| Processing | Azure Databricks |
| Storage | Azure Data Lake Storage Gen2 |
| Data Format | Delta Lake |
| Governance | Unity Catalog |
| Orchestration | Databricks Workflows |
| Serving Layer | Databricks SQL Warehouse |
| Visualization | Power BI |
| Programming | PySpark, SQL |

---

## Dataset

Brazilian E-Commerce Public Dataset containing:

- Customers
- Orders
- Products
- Order_items
- Payments
- Reviews
- Product_Category_translation

---

## Medallion Architecture

### Bronze Layer

Purpose:
Store raw source data without modification.

Activities:

- Data ingestion from AWS S3
- Schema preservation
- Delta table creation

Output:

- Raw Delta Tables

---

### Silver Layer

Purpose:
Clean, standardize, and enrich data.

Activities:

- Data cleansing
- Null handling
- Data type conversions
- Duplicate removal
- Sentiment analysis on customer reviews
- Business rule implementation

Output:

- Cleaned and enriched Delta Tables

---

### Gold Layer

Purpose:
Provide business-ready analytical datasets.

Activities:

- Star schema implementation
- Fact and dimension modeling
- KPI preparation

Output:

- Analytics-ready Delta Tables

---

## Data Model

The Gold layer follows a Star Schema design optimized for analytical workloads and reporting.

### Star Schema
![Data Model] (<img width="581" height="536" alt="Screenshot 2026-06-04 223728" src="https://github.com/user-attachments/assets/22ac17c3-423b-415d-b9da-9ad264fa296d" />)


### Fact Table

- fact_sales

### Dimension Tables

- dim_customer
- dim_product
- dim_payment
- dim_review
- dim_date

---

## Databricks Workflow

The complete pipeline is orchestrated using Databricks Workflows.

Workflow Sequence:

```text
Bronze Ingestion
      ↓
Silver Transformation
      ↓
Gold Modeling
```
![Workflow](<img width="1200" height="600" alt="Screenshot 2026-06-06 193859" src="https://github.com/user-attachments/assets/73687e8a-9326-46e0-a420-3d985fd95576" />)

---

## Databricks SQL Warehouse

Databricks SQL Warehouse serves as the analytics layer for reporting and dashboarding.

Features:

- High-performance SQL querying
- Unity Catalog integration
- Power BI connectivity

<![SQL Warehouse](img width="1365" height="600" alt="Screenshot 2026-06-06 221149" src="https://github.com/user-attachments/assets/f1a7d38a-854c-4a0f-9c3c-2959c5a0ac33" />)


---

## Power BI Dashboard

### Executive Sales Overview

Features:

- Gross Revenue
- Delivered Revenue
- Total Orders
- Delivered Orders
- Cancelled Orders
- Average Order Value
- Revenue Trend
- Revenue by State
- Revenue by Payment Type

![Dashboard Page 1](<img width="1365" height="647" alt="Screenshot 2026-06-09 135614" src="https://github.com/user-attachments/assets/3562f90b-9bbd-4e1a-883c-9b520ebc5165" />
)

---

### Order & Fulfillment Analysis

Features:

- Average Delivery Days
- Delivery Delays
- Average Review Score
- Orders by Status
- Product Category Analysis
- Review Score Distribution

![Dashboard Page 2](<img width="1364" height="657" alt="Screenshot 2026-06-09 135639" src="https://github.com/user-attachments/assets/9681bfcf-44f9-4de7-a056-1f781a32398f" />
)

---

## Key Business Metrics

- Gross Revenue
- Delivered Revenue
- Total Orders
- Delivered Orders
- Cancelled Orders
- Average Order Value
- Average Delivery Days
- Average Review Score

---

## Key Learnings

- Implemented Medallion Architecture
- Built Delta Lake tables using PySpark
- Managed governance with Unity Catalog
- Developed ETL pipelines in Azure Databricks
- Automated workflows using Databricks Workflows
- Created Star Schema dimensional models
- Connected Databricks SQL Warehouse to Power BI
- Built business intelligence dashboards

---

## Repository Structure

```text
Brazilian-Ecommerce-Lakehouse/
│
├── Architecture/
│   └── architecture.png
│
├── Bronze/
├── Silver/
├── Gold/
│
├── Workflow/
│   └── workflow.png
│
├── Dashboard/
│   ├── page1.png
│   └── page2.png
│
├── Screenshots/
│   └── sql_warehouse.png
│
└── README.md
```

---

## Author

Iswarya Selvakumar

Azure Databricks | PySpark | Delta Lake | Data Engineering
