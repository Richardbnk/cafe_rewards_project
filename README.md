# Cafe Rewards Data Engineering Pipeline

## Overview

This repository implements a robust **data engineering pipeline** for the Café Rewards Offer dataset. The solution is organized in a layered structure (Raw → Trusted → Refined), using **PySpark** for scalable data processing and **Google Cloud Platform (GCP)** as the data lakehouse backend.

The project follows clean architecture principles, focusing on modularity, reproducibility, and maintainability. It answers business-relevant analytical questions with production-ready, well-structured code.

---

**Description:**
- **Raw Layer:** Ingests source CSVs from Kaggle.
- **Trusted Layer:** Cleans, joins, and validates data.
- **Refined Layer:** Computes business metrics and analytical views.
- Data is persisted in **BigQuery** for downstream consumption.

---

## Design Choices

- **PySpark** for parallelized data transformation.
- **BigQuery** as the data warehouse.
- **Separation of concerns:** Each layer is handled by a dedicated script.
- **Modular execution:** The pipeline can be run end-to-end or by layer.
- **Reproducible:** All dependencies listed in `requirements.txt`; single entrypoint for pipeline execution.

---

## Setup & Run

1. **Clone this repo**
    ```bash
    git clone https://github.com/yourusername/cafe_rewards_project.git
    cd cafe_rewards_project
    ```

2. **Create & activate a virtual environment**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

4. **Set up GCP credentials**
    - Download your service account key and place it in `credentials/gcp-project.json`.
    - Set the environment variable:
      ```bash
      export GOOGLE_APPLICATION_CREDENTIALS="credentials/gcp-project.json"
      ```

5. **Place the raw Kaggle data in `data/raw/`**

6. **Run the full pipeline**
    ```bash
    python run_pipeline.py
    ```

---

## Pipeline Flow

- **Raw Layer** (`src/ingest_raw_data.py`):  
  Loads CSVs into Spark DataFrames, writes raw tables.
- **Trusted Layer** (`src/trusted_tables.py`):  
  Cleans, joins, and processes data into trusted business tables.
- **Refined Layer** (`src/refined_tables.py`):  
  Aggregates and computes metrics for analytics.

---

## Analytical Questions

**1. Which marketing channel is the most effective in terms of offer completion rate?**  
→ Output in the refined layer via `offer_completion_rate.sql`.

**2. How is the age distribution of customers who completed offers compared to those who did not?**  
→ Output in the refined layer via `age_distribution.sql`.

**3. What is the average time taken by customers to complete an offer after receiving it?**  
→ Computed in the refined layer, available as a table or plot.

> _Re-run the pipeline to regenerate all answers. Outputs are visible in your BigQuery tables or can be printed to console._

---

## Clean Architecture

- **Layered:** Each step is in a separate script (Raw, Trusted, Refined).
- **Separation of concerns:** Each layer has a clear responsibility.
- **Reproducible:** Run everything from `run_pipeline.py`.

