# Fibex Telecom - Data Pipeline & Analytics

**Author:** Jose Arturo Pérez

## 📌 Project Overview
This project implements an automated **ETL pipeline** to process, clean, and consolidate operational data for the **Fibex Data Lake**. It centralizes critical business domains including:
* 💰 **Financial:** Collections (Recaudación) & Payment Hours.
* 🛠️ **Technical:** Failure Index (Índice de Falla - IdF) & Claims.
* 📈 **Commercial:** Sales, Status, and Branch Traffic (Afluencia).

It replaces manual Excel workflows with a robust Python-based architecture, reducing processing time and enabling daily updates for Power BI dashboards.

## 🛠 Tech Stack
* **Language:** Python 3.12
* **Engines:** `pandas` & `python-calamine` (Rust-based Excel reading for high performance).
* **Transformation:** Custom Python ETLs & **dbt** (Data Build Tool) for SQL modeling.
* **Storage/Querying:** DuckDB (Local Data Warehouse) & Parquet files.
* **Visualization:** Power BI (Parquet connectors).
* **Version Control:** Git

## 📂 Project Structure
* `ETLs/`: Extraction scripts. Key modules:
    * `idf.py`: **[NEW]** Iterative processing for Failure Index with cross-date filtering logic.
    * `recaudacion.py`: Financial data consolidation (integrates Payment Hour logic).
* `fibex_analytics/`: **[NEW]** dbt project folder for SQL transformations and data lineage.
* `main.py`: Orchestrator script. Organizes execution order (Dimensions -> Facts) to ensure referential integrity.
* `utils.py`: Helper functions (e.g., `limpiar_nulos_powerbi`, `obtener_rango_fechas`).
* `config.py`: Configuration variables (paths, constants).

## 🚀 Key Features & Logic

### 1. Optimized Excel Reading
Uses the **Calamine** engine to read heavy Excel files, reducing load times from minutes to seconds compared to standard `openpyxl`.

### 2. Smart Date Parsing (IdF)
The pipeline automatically extracts date metadata (e.g., "ENE Q1") from filenames to apply specific business logic filters for the *Índice de Falla*.

### 3. Power BI Compatibility
All pipelines include a cleaning layer (`utils.limpiar_nulos_powerbi`) that converts `NaN`, `NaT`, and `"nan"` strings into `None`, ensuring Power BI interprets them correctly as `(Blank)`.