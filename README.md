# Price Data Validation Pipeline 

## Overview
This project is a Python-based **ETL (Extract, Transform, Load) pipeline** designed to validate financial position and price data. It joins disparate datasets, applies fallback logic for missing market data, calculates portfolio exposure, and statistically detects price outliers using Z-scores.

The solution includes:
1.  **Backend Pipeline (`data_validation.py`):** A modular, production-ready Python script handling the core ETL logic.
2.  **Interactive Front-End (`app.py`):** A Streamlit web application allowing users to upload files and visualize results dynamically.

## Features
* **Robust Data Ingestion:** Joins Position, Current Price, and Previous Price datasets using `LEFT JOIN` logic to ensure data integrity.
* **Resilient Fallback Logic:** Automatically fills missing price data with historical values (`prices_prev.csv`) to prevent downstream calculation errors.
* **Financial Math:** Computes `Exposure = Quantity * Final Price`.
* **Outlier Detection:** Implements Z-score analysis ($Z = \frac{x - \mu}{\sigma}$) to flag price anomalies where $|Z| > 3$.
* **Enterprise Logging:** Uses Python's `logging` module for traceability instead of standard print statements.

## Project Structure
```text
/Lord_Abbett_Case_Study
├── app.py                 # Streamlit Front-End Application
├── data_validation.py     # Core ETL Logic (Extract, Transform, Load)
├── requirements.txt       # Python dependencies
├── run.bat                # Windows automation script (Makefile alternative)
├── positions.csv          # Input: Portfolio Holdings
├── prices.csv             # Input: Current Market Prices
├── prices_prev.csv        # Input: Historical Backup Prices
└── README.md              # Project Documentation