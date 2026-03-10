## 📊 Market Data Pipeline (B3)

This pipeline collects historical and fundamental data for all companies
listed on the Brazilian stock exchange (B3) using the Yahoo Finance API.

The process builds a raw data layer that can later be used for analytics, modeling and financial research.

----------

### 🎲 Data Sources

The pipeline relies on the following data providers:

- Yahoo Finance (via yfinance)
- Fundamentus (for B3 ticker universe discovery)

----------

### 🎯 Pipeline Steps

The pipeline is composed of four extraction stages:

1. **Price Extraction**
   - Historical OHLCV prices
   - Adjusted close values
   - 10-year history

2. **Corporate Events**
   - Dividends
   - Stock splits

3. **Company Metadata**
   - Sector and industry
   - Market capitalization
   - Financial ratios
   - Risk metrics (beta)

4. **Financial Statements**
   - Annual income statement
   - Revenue
   - Net income
   - Operating metrics

----------

### 📑 Generated Datasets

The pipeline generates the following raw datasets:

| File                         | Description                                 |
| ---------------------------- | ------------------------------------------- |
| 02_yfinance_precos_raw.csv   | Historical price data (OHLCV)               |
| 03_yfinance_eventos_raw.csv  | Corporate actions (dividends and splits)    |
| 04_yfinance_info_raw.csv     | Company metadata and fundamental indicators |
| 05_yfinance_balancos_raw.csv | Annual financial statements                 |

----------

### 📁 Project Data Structure
```
### 📁 Project Data Structure
```
📁 data/
   📁 raw/
     📄 01_bcb_indicadores_economicos.csv
     📄 02_yfinance_precos_raw.csv
     📄 03_yfinance_eventos_raw.csv
     📄 04_yfinance_info_raw.csv
     📄 05_yfinance_balancos_raw.csv
```


----------

### 🏃‍♂️Running the Pipeline

To execute the data collection pipeline:

```bash
python 02_yfinance_data_extraction.py
```
or
```bash
poetry run python 02_yfinance_data_extraction.py
```

The pipeline will automatically:

 - Discover all B3 tickers
 - Download market data
 - Generate the raw datasets