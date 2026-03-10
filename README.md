# From Raw Data to Investment Insights

This project explores the relationship between macroeconomic factors and Brazilian Stock Exchange (B3) assets, specifically focusing on how external economic variables influence tickers in the financial sector.

## 1. Description

A technical deep-dive into the Brazilian stock market (B3) to analyze the impact of macroeconomic shifts. By leveraging automated data pipelines, this project identifies correlations between national economic indicators and stock performance, turning raw financial data into actionable investment insights.

## 2. Project Objectives

* **Quantify Interference:** Demonstrate how macroeconomic factors directly affect papers within the B3 financial sector.
* **Pattern Recognition:** Explore and identify recurring patterns in the relationship between the economy and market prices.
* **Data Automation:** Build a robust pipeline for data extraction, transformation, and storage.

## 3. Data Sources

The project integrates data from three main pillars:

* **`yfinance`**: Real-time and historical market data for stock tickers.
* **`bcb`**: A Python interface for the **Central Bank of Brazil (BCB)** Open Data API, used to collect macroeconomic indicators (e.g., SELIC, IPCA).
* **`fundamentus`**: Comprehensive fundamentalist indicators for all companies listed on the B3.

> [!NOTE]
> For more granular details on the variables used, please refer to the `data/data_dictionary.md` file.

## 4. Methodology and Technologies

The workflow is designed around a **Data Lake** concept to minimize API calls and ensure performance:

1. **Ingestion:** Data is fetched via APIs and persisted as `.parquet` files (**Raw**) to allow for offline processing.
2. **Transformation:** Data is cleaned and refined using modular pipelines, then saved into a processed layer (**Processed**).
3. **Documentation:** Detailed process workflows can be found in the `/docs` directory (e.g., `macro_data_pipeline(BC).md`, `master_data_lake_pipeline.md`).

**Technology Stack:**

* **Language:** Python 3.11.8
* **Package Manager:** Poetry
* **Storage Format:** Apache Parquet
* **Analysis:** Pandas, Jupyter Notebooks
* **Dashboard:** Streamlit (via `b3_analytics`)

## 5. Repository Structure

```text
├── data/               # Raw and processed datasets
│    ├── processed/     # Cleaned data ready for analysis
│    └── raw/           # Original data from APIs
├── docs/               # Technical documentation and dictionaries
├── notebooks/          # Jupyter notebooks for EDA and modeling
├── src/                # Modular Python source code
│      └── b3_analytics/ # Streamlit dashboard and analysis logic
├── requirements.txt    # Dependency list
├── pyproject.toml      # Poetry configuration
├── poetry.lock         # Poetry lockfile
├── .gitignore
└── README.md

```

## 6. Installation and Configuration

**Requirements:**

* Python 3.11.8
* [Poetry](https://python-poetry.org/)

**Environment Setup:**

1. Clone the repository and navigate to the folder.
2. Initialize the environment:
```bash
poetry env use python
poetry install

```


3. Register the kernel for Jupyter:
```bash
poetry run python -m ipykernel install --user --name b3-project --display-name "Python (B3-Poetry)"

```



## 7. How to Run

Execute the extraction and processing scripts via CMD in the following sequence:

1. **Macro Extraction:** `poetry run python src/b3_analytics/data/01_bcb_macro_extraction.py`
2. **Market Extraction:** `poetry run python src/b3_analytics/data/02_yfinance_data_extraction.py`
3. **Fundamentalist Extraction:** `poetry run python src/b3_analytics/data/03_cvm_elite_extraction.py`
4. **Processing:** `poetry run python src/b3_analytics/data/04_process_market_data.py`

After running the pipeline, you can open the notebooks in the `/notebooks` folder to generate charts and view the analysis.

## 8. Contribution

Contributions are welcome! To contribute:

1. **Fork** the repository.
2. Create a new **Branch** (`git checkout -b feature/new-analysis`).
3. Commit your changes (`git commit -m 'Add new feature'`).
4. Push to the branch (`git push origin feature/new-analysis`).
5. Open a **Pull Request**.

If you find a bug, please open an **Issue**.

## 9. Contact / Authors

| Name | Email | LinkedIn | GitHub |
| --- | --- | --- | --- |
| **Your Name** | your.email@example.com | [LinkedIn](https://linkedin.com/in/your-profile) | [@your-github](https://www.google.com/search?q=https://github.com/your-username) |