# Data Warehouse Governance for Financial Controllership

##  Overview
This repository contains the implementation of a governed **Data Warehouse architecture** designed to support financial controllership analytics. The project integrates heterogeneous data sources such as **Excel files, ERP systems, and CRM platforms**, enabling consistent, auditable, and scalable financial analysis.

Due to data sensitivity, this repository exposes **only code, pipeline structures, and orchestration logic**, without any real or sample data.

---

## Objectives
- Integrate financial and operational data from multiple sources  
- Support **budget vs. actual (planned vs. realized)** analysis  
- Enable **cost Pareto analysis** for financial optimization  
- Build **predictive financial models** based on the Income Statement (DRE) using time series  
- Ensure governance, traceability, and reproducibility of analytical pipelines  

---

## Architecture Overview
The platform is structured around **modular, orchestrated pipelines**, covering the full analytics lifecycle:

1. Data ingestion (Excel, ERP, CRM)
2. Data validation and standardization
3. Dimensional modeling and Data Warehouse construction
4. Feature engineering for analytical models
5. Model training and retraining (time series forecasting)
6. Delivery of curated datasets for BI and analytics layers

All workflows are orchestrated using **Prefect**, ensuring observability, reusability, and controlled execution.

---

## Orchestration Strategy
- Each data domain is handled by an independent pipeline  
- Pipelines are versioned and aligned with the **controllership schema**  
- Execution logic supports incremental loads and reprocessing  
- Model retraining pipelines are integrated into the orchestration layer  

---

## Analytics & Modeling
The project includes analytical pipelines for:
- Budget vs. actual variance analysis  
- Cost concentration and Pareto analysis  
- Financial projections using **time series models** based on DRE structure  

Models are designed to be retrained periodically as new data becomes available.

---

## Tech Stack
- **Programming:** Python  
- **Orchestration:** Prefect  
- **Data Processing:** SQL, Pandas  
- **Modeling:** Time Series Forecasting  
- **Architecture:** Data Warehousing, Dimensional Modeling  

---

##  Repository Structure
```text
governanca_dw_controladoria/
│
├── prefect_flows/     # Orchestrated data and modeling pipelines
├── docs/diagrams/     # Architectural documentation (no data)
├── README.md          # Project documentation
├── CHANGELOG.md       # Pipeline evolution tracking
└── .gitignore
