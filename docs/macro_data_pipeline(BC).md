## 🌎 Macroeconomic Data Pipeline (Banco Central)

This pipeline collects Brazilian macroeconomic indicators from the Banco Central do Brasil
using the python-bcb API.

These indicators provide macroeconomic context that can later be combined with market data
for financial analysis, modeling and research.

----------

### 🎲 Data Sources

The pipeline relies on the following data providers:

- Banco Central do Brasil – SGS API (via python-bcb)
  
----------

### 📊 Collected Indicators

The following economic indicators are collected:

| Indicator | Description                  |
| --------- | ---------------------------- |
| SELIC     | Brazilian base interest rate |
| IPCA      | Official inflation index     |
| USD/BRL   | Dollar exchange rate         |

The pipeline retrieves **up to 10 years of historical data** by default.

---------

### 📑 Generated Dataset

| File                              | Description                                              |
| --------------------------------- | -------------------------------------------------------- |
| 01_bcb_indicadores_economicos.csv | Macroeconomic indicators from the Brazilian Central Bank |

----------

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
python 01_bcb_macro_extraction.py
```
or
```bash
poetry run python 01_bcb_macro_extraction.py
```

The pipeline will automatically:

- Define the analysis period (last 10 years)
- Query the Banco Central API
- Download the indicators
- Generate the raw dataset