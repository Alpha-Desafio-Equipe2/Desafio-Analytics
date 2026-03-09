
## 🧠 Master Data Lake Builder (B3 Analytics)

The **Master Data Lake Builder** is the final stage of the B3 Analytics project.

This pipeline integrates multiple financial data sources and produces a **single analytical dataset** containing:

-   market data    
-   macroeconomic indicators
-   corporate events  
-   fundamental financial statements
-   quantitative indicators
-   technical indicators
-   risk metrics

The result is a **machine-learning-ready financial dataset** covering the Brazilian stock market.

----------

# 🎯 Pipeline Objective

The goal of this pipeline is to build a **consolidated financial dataset** that enables:

-   quantitative finance research
-   financial modeling
-   factor investing analysis
-   machine learning experiments

The pipeline transforms raw datasets into a **feature-rich market dataset** ready for analytics.

----------

# 🎲 Data Sources

The pipeline integrates data from multiple sources:

| Source                  | Description                                   |
| ----------------------- | --------------------------------------------- |
| Yahoo Finance           | Historical market prices and corporate events |
| Banco Central do Brasil | Macroeconomic indicators                      |
| CVM Open Data           | Financial statements of Brazilian companies   |

----------

# 🧩 Pipeline Architecture

```
Raw Market Data (Yahoo Finance)
        ↓
Macroeconomic Data (BCB)
        ↓
Company Metadata
        ↓
Corporate Events (Dividends / Splits)
        ↓
CVM Financial Statements
        ↓
Feature Engineering
        ↓
Master Financial Dataset

```

----------

## 🔧 Pipeline Steps

### 1️⃣ Load Market Prices

Historical stock prices are loaded from:

```
data/raw/02_yfinance_precos_raw.csv

```

Data processing includes:

-   column normalization 
-   timestamp cleaning 
-   ticker standardization 

The adjusted close column is removed to avoid redundancy.

----------

### 2️⃣ Inject Macroeconomic Indicators

Macroeconomic indicators from Banco Central are merged into the dataset.

File used:

```
data/raw/01_bcb_indicadores_economicos.csv

```

Indicators include:

-   SELIC interest rate
-   IPCA inflation index
-   USD/BRL exchange rate

Values are forward-filled to match daily market observations.

----------

### 3️⃣ Integrate Company Information

Company metadata is loaded from:

```
data/raw/03_yfinance_info_raw.csv

```

Key attributes include:

-   sector
-   industry
-   market capitalization
-   book value
-   return on equity

These attributes provide **static company-level features**.

----------

### 4️⃣ Corporate Events Integration

Corporate actions are integrated from:

```
data/raw/02_yfinance_eventos_raw.csv

```

Events include:

-   dividends
-   stock splits

Missing values are replaced with zero.

----------

### 5️⃣ CVM Financial Statements Integration

Historical financial statements from CVM are loaded:

```
data/raw/05_CVM_Historico_Focado.csv

```

The merge uses **merge_asof** to attach the most recent financial statement  
available prior to each trading day.

Financial indicators added:

-   EBIT
-   Net Income
-   Equity proxy

----------

### 6️⃣ Feature Engineering

The pipeline generates advanced financial indicators using the  
`finance_utils` module.

#### Quantitative Indicators

-   daily returns
-   rolling volatility
-   moving averages
-   momentum indicators

#### Technical Indicators

-   RSI
-   Bollinger Bands
-   MACD

#### Risk Metrics

-   maximum drawdown
-   volatility statistics

#### Fundamental Indicators

-   profitability ratios
-   financial trends

#### Performance Metrics

-   rolling Sharpe ratio

----------

### 7️⃣ Dataset Export

The final dataset is exported in two formats:

```
data/processed/01_market_data_processed.parquet
data/processed/01_market_data_processed.csv

```

Additionally, the dataset is partitioned by **economic sector**:

```
data/processed/setores/*.parquet

```

This improves performance for analytics workflows.

----------

## 📦 Generated Outputs

| File                             | Description                   |
| -------------------------------- | ----------------------------- |
| 01_market_data_processed.parquet | Main analytical dataset       |
| 01_market_data_processed.csv     | CSV version for compatibility |
| setores/*.parquet                | Sector-partitioned datasets   |

----------

## 🏃 Running the Pipeline

To build the Master Data Lake:

```bash
python 04_process_market_data.py

```
or
```bash
poetry run python 04_process_market_data.py

```

The pipeline will automatically:

-   load raw datasets
-   integrate financial sources
-   generate features
-   export the final dataset
    
----------

# ⚠️ Notes

-   Financial statements are merged using the nearest historical date.
-   All timestamps are normalized to avoid timezone inconsistencies.
-   Numeric columns are safely converted to avoid parsing errors.
-   Missing values in financial indicators may occur due to reporting frequency differences.