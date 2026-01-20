# Fibex Telecom - Data Pipeline & Analytics

**Author:** Jose Arturo Pérez

## 📌 Project Overview
This project implements an automated **ETL pipeline** to process, clean, and consolidate operational data regarding card deliveries and branch traffic (afluencia).

It replaces manual Excel workflows with a robust Python-based architecture, reducing processing time and enabling daily updates for Power BI dashboards.

## 🛠 Tech Stack
* **Language:** Python 3.12
* **Transformation:** Pandas & Custom ETL Scripts
* **Storage/Querying:** DuckDB (Local Data Warehouse)
* **Visualization:** Power BI
* **Version Control:** Git

## 📂 Project Structure
* `ETLs/`: Individual extraction and transformation scripts for each data source.
* `main.py`: Orchestrator script that runs the full pipeline.
* `config.py`: Configuration variables (paths, constants).