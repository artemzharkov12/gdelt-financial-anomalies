# 📈 Automated Financial Anomaly Reporting

Welcome to the project repository! 

Our goal is to build an **Event-driven Medallion Architecture** that automatically ingests market data, detects price anomalies (spikes/crashes), and dynamically queries the GDELT News API *only* for those anomalous days. 

This approach minimizes compute costs and provides the business with an automated Root-Cause Analysis (RCA) dataset, correlating market behavior with news hype and specific keywords.

---

## 🏗️ 1. Workspace Directory Structure

Our code is strictly organized into four logical layers (Medallion Architecture). Every new notebook must be placed in its respective folder. We use an alphabetical suffix system to separate different asset pipelines:
* `a` = Bitcoin (Artem)
* `b` = GDELT News Data
* `c` = NVIDIA Stocks (Team Member 2)
* `d` = Oil Futures (Team Member 3)

### Folders:
* **`01_landing/`**: Scripts for fetching data from external APIs and saving raw dumps (JSON/CSV/ZIP) into Databricks Volumes. 
    * *Example:* `01c_landing_nvidia`
* **`02_bronze/`**: Incremental data ingestion using Databricks Auto Loader (`cloudFiles`). Reads raw files, adds metadata (`ingest_timestamp`, `source_filename`),and schema enforcement and performs an append-only write to Delta tables.
* **`03_silver/`**: Data cleansing layer. Handles deduplication, `NULL` value treatment, timestamp formatting, .
* **`04_gold/`**: Business logic and aggregation. Detects daily price anomalies and triggers the GDELT news filtering based on asset-specific keywords.

---

## 🗄️ 2. Unity Catalog Naming Conventions

Since we are working in a Shared Workspace, strict naming conventions are mandatory to avoid overwriting each other's data.

### Schemas (Databases)
Create schemas bound to your first name and the specific layer. (we work in our shamas which were created in Lab 2 just for convenience. That's where you store your raw data and all the transformations. And in the code, if necessary, you can pull tables from your colleagues' schemas. for example gdelt_history_silve in artemzharkov10_silver schema )
* **Format:** `[username]_[layer]`
* **Examples:** `pawelnowak2004_bronze`, `pawelnowak2004_silver`, `pawelnowak2004_gold` 


### Tables
Keep table names descriptive and standardized across layers.
* **Format:** `[asset]_[description]_[layer]`
* **Examples:** `finance_nvidia_bronze`, `gdelt_history_silver`, `nvidia_news_gold`

---

## 🚀 3. Tasks for Lab №3 (Team Expectations)

Each team member is responsible for building an End-to-End (E2E) pipeline for their assigned streaming asset. 

**Your Action Plan:**
1.  **Landing:** Find a streaming/historical API for your asset (NVIDIA or another). Ingest raw data into your Volume.
2.  **Bronze:** Use Auto Loader (`readStream.format("cloudFiles")` with `.trigger(availableNow=True)`) to incrementally load new files into your Bronze schema without duplicates.
3.  **Silver:** Clean the data (drop duplicates, cast correct data types).
4.  **Gold (The Core Logic) (it's for future for now it doesn't need) :** 
    * Calculate  price deviations .
    * If an anomaly is detected, query the GDELT silver table for that specific date.
    * Filter the news using the `.contains()` function with keywords specific to your asset (e.g., for NVIDIA: `AI`, `GPU`, `Jensen`, `chips`).
    * Save the final correlation (Date, Price, Top News URL, Hype Score) into your Gold table.

---
## 🔄 4. Git Workflow (How We Collaborate)

To maintain a stable codebase and meet strict lab requirements, **NO direct commits to the `main` branch are allowed.**

1.  **Isolated Branching:** Everyone develops their own pipeline in a separate branch. Before writing code, always create a new branch from `main`.
    * **Branch Format:** `feature/[firstname]-[task-description]`
    * *Example:* `feature/valeriy-nvidia-landing`
2.  **Committing:** Commit and push your changes using the Databricks Git UI.
3.  **Pull Requests & Mandatory Approval:** Once your code is ready, open a Pull Request (PR) against the `main` branch. **As per the lab requirements, merging is strictly prohibited without an explicit approval from another team member.** 
4.  **Scheduled Team Merges:** We do not merge PRs randomly. We will hold scheduled sync sessions (at an agreed-upon time) to jointly review the code, approve the PRs, and merge them into `main`. After the merge is complete, always pull the latest `main` branch into your Databricks environment before starting a new task.