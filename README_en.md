# Fibex Telecom - Data Pipeline & Analytics

🇪🇸 *[Leer en Español](README.md)*

**Author:** Jose Arturo Pérez

## 📌 Project Overview
This project implements an automated **ETL pipeline** to process, clean, and consolidate operational data for the **Fibex Data Lake**. It centralizes critical business domains including:
* 💰 **Financial:** Collections (Recaudación) & Payment Hours.
* 🛠️ **Technical:** Failure Index (Índice de Falla - IdF) & Claims.
* 📈 **Commercial:** Sales, Status, and Branch Traffic (Afluencia).

It replaces manual Excel workflows with a robust Python-based architecture, reducing processing time (thanks to Out-of-Core memory execution) and enabling daily updates for Power BI dashboards.

## 🛠 Tech Stack
* **Language:** Python 3.12
* **RPA Automation:** `playwright` (Unattended web data extraction).
* **Processing & Transformation:** `polars` (Ultra-fast Out-of-Core execution), `pandas`, and `python-calamine` (Rust-based Excel reading).
* **Storage / SQL Querying:** DuckDB (Local Data Warehouse) & Parquet files.
* **Visualization:** Power BI (Native Parquet connectors).
* **Version Control:** Git

## ⚙️ Installation and Setup
To deploy this project on a new machine, follow these steps:
1. Clone the repository and create a virtual environment (`python -m venv venv`).
2. Install dependencies: `pip install -r requirements.txt`.
3. **CRITICAL - Install RPA Browsers:** Run `playwright install chromium` so the web scrapers can navigate the SAE Plus portal.
4. **Credentials and Rules:** Check the `config.py` file to ensure the Data Lake paths (`RUTA_BASE`) point to the correct drive on the new server.

## 🚀 How to Run (Orchestration)
All extraction is centralized. Run `python extraccion/main.py`. An interactive menu will appear in the console (built with `rich`). You can choose to run the entire suite of robots or individual modules. For unattended execution (e.g., Windows Task Scheduler at 3 AM), use the flag: `python main.py --auto`.

## 📂 Project Structure
* `extraccion/`: RPA automation scripts (Web Scraping with Playwright) and its dedicated `main.py` orchestrator to download reports from SAE Plus.
* `transformacion/ETLs/`: Data cleaning, consolidation, and modeling scripts using Polars, Pandas, and DuckDB.
* `main.py`: Master transformation orchestrator. Controls the execution flow (Raw -> Bronze -> Silver -> Gold).
* `utils.py` & `extraccion/scraper_utils.py`: Modular helper functions (null cleaning, incremental ingestion, web selectors, etc.).
* `config.py` & `reglas_negocio.json`: Configuration variables, dynamic paths, and business rules editable without touching Python code.

## 🚀 Key Features & Logic

### 1. Optimized Excel Reading
Uses the **Calamine** engine to read heavy Excel files, reducing load times from minutes to seconds compared to the standard `openpyxl` library.

### 2. Smart Date Parsing
The pipeline automatically extracts date metadata (e.g., "ENE Q1") from filenames to apply specific business logic filters (such as for the *Índice de Falla*).

### 3. Native Power BI Compatibility
All pipelines include a cleaning layer (`utils.limpiar_nulos_powerbi`) that converts texts like `NaN`, `NaT`, and `"nan"` into real `None` objects, ensuring Power BI interprets them correctly as `(Blank)`.

### 4. Medallion Architecture & Incremental Ingestion
The pipeline strictly follows the **Medallion Architecture pattern (Raw -> Bronze -> Silver -> Gold)**. Using Polars and DuckDB, it performs incremental ingestion by processing only newly added files. The Bronze layer serves as an immutable copy of the RAW data, ensuring long-term historical preservation and optimizing downstream compute times.

### 5. Slowly Changing Dimensions (SCD Type 2)
The data model supports *Slowly Changing Dimensions* to maintain a historical record of entity changes over time. Currently implemented in the employee master (`trans_empleados.py`) to automatically track office transfers or promotions, with the architectural foundation ready for its upcoming deployment in the customer dimension (`Dim_Cliente`).

## 🗺️ Roadmap & Next Steps (Journey to the Cloud)
Currently, the Data Lake resides locally using DuckDB and Parquet. However, the project's architecture is fully prepared to evolve into a Cloud-Native solution during its final Load phase:
* **Advanced Partitioning:** Implement logical partitioning of `.parquet` files (e.g., by `year/month` or `branch`) to maximize query performance.
* **Cloud Storage (Buckets):** Migrate the Data Lake folder structures into cloud object storage services (such as AWS S3, Google Cloud Storage, or Azure Blob Storage).
* **Data Warehouse Centralization:** Ingest the clean and consolidated Gold layer tables into a corporate Cloud Data Warehouse (such as Snowflake, BigQuery, or Databricks), democratizing data access across the entire organization.

## 🔧 Maintenance and Troubleshooting
* **SAE Plus Changes:** If the SAE portal changes its graphical interface (buttons or menus), the scrapers will fail with a *Timeout*. Updates must be made in `extraccion/scraper_utils.py` (where all CSS selectors and navigation logic live).
* **Business Rules:** There's no need to modify Python code to add new "Sellers", "Statuses", or "Exclusions". Simply edit the `reglas_negocio.json` file, and the pipeline will adopt the new rules in the next execution.
* **Gold Layer (Power BI):** The BI team should connect directly to the `gold_data` folder by importing the `.parquet` files. The data model is designed as a Star Schema, using integer surrogate IDs (e.g., `Cliente_SK`) for ultra-fast relationships, discarding the use of heavy strings.

## 🤝 Acknowledgments
A special thanks to Mr. **Jonattan Sotillo**, my direct manager, who gave me the opportunity, the space, and the trust to join this team and build this project. Thank you for your constant support, vision, and great sense of humor!