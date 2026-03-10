## 📑 CVM Financial Statements Pipeline (DRE)

This pipeline collects and processes **historical financial statements (Income Statement / DRE)** for selected companies listed on the Brazilian stock exchange (B3).

The data is obtained directly from the **Comissão de Valores Mobiliários (CVM)** open data portal and refined to include only a predefined set of target companies.

The resulting dataset contains **annual financial metrics for each company**, enabling financial analysis, modeling, and long-term market research.

----------

### 🎯 Pipeline Objective

The purpose of this pipeline is to build a **clean historical dataset of financial statements** for a curated set of B3 companies.

The pipeline focuses on a **target universe of tickers (TICKERS_ALVO)**, avoiding unnecessary processing of hundreds of companies that are not relevant for the project.

This approach significantly reduces:

* memory usage
* processing time
* dataset size

----------

### 🎲 Data Source

The pipeline relies on the following public data providers:

| Source               | Description                                        |
| -------------------- | -------------------------------------------------- |
| CVM Open Data Portal | Financial statements of Brazilian public companies |

Official portal:

https://dados.cvm.gov.br

Datasets used:

* **FCA (Formulário Cadastral)** → used to map B3 tickers to CVM company codes
* **DFP (Demonstrações Financeiras Padronizadas)** → annual financial statements

----------

### 🧠 Pipeline Architecture

The pipeline is composed of **three main stages**:

```
Target Tickers
      ↓
CVM Mapping (FCA dataset)
      ↓
Financial Statements Extraction (DFP dataset)
      ↓
Data Cleaning & Transformation
      ↓
Final Dataset
```

----------


### 🧩 Pipeline Steps

#### 1️⃣ CVM Code Resolution

The pipeline first builds a **mapping between B3 tickers and CVM company codes**.

This step uses the **FCA dataset** published annually by CVM.

Process:

-   Download FCA dataset 
-   Identify ticker symbols
-   Map them to **CVM registration codes**
-   Filter only companies in `TICKERS_ALVO`
    
Result:

```
CVM_CODE → TICKER
```

Example:

| CVM Code | Ticker |
| -------- | ------ |
| 9512     | PETR4  |
| 4170     | VALE3  |
| 19348    | ITUB4  |

----------

#### 2️⃣ Financial Statements Extraction

For each year in the selected period, the pipeline downloads the **DFP dataset** from CVM.

Files processed:

```
dfp_cia_aberta_{year}.zip
```

Inside each ZIP file the pipeline reads:

```
dfp_cia_aberta_DRE_con_{year}.csv
```

Only records that match the **target CVM company codes** are kept.

This reduces the dataset from **thousands of companies to only the relevant subset**.

----------

#### 3️⃣ Financial Metrics Selection

The pipeline extracts the following **Income Statement accounts (DRE)**:

| Account Code | Metric                |
| ------------ | --------------------- |
| 3.01         | Net Revenue           |
| 3.03         | Gross Profit          |
| 3.05         | Operating EBIT        |
| 3.06         | Financial Result      |
| 3.09         | Earnings Before Taxes |
| 3.11         | Net Income            |

----------

#### 4️⃣ Data Transformation

The dataset is then transformed into a **tabular analytical structure**.

Transformations include:

-   filtering last fiscal exercise (`ORDEM_EXERC = ÚLTIMO`)
-   pivoting financial accounts into columns
-   converting values from **thousands to Brazilian reais**
-   generating fiscal year columns
-   attaching the corresponding B3 ticker
    

----------

### 📑 Generated Dataset

| File                        | Description                                               |
| --------------------------- | --------------------------------------------------------- |
| 06_CVM_Historico_Focado.csv | Historical financial statements for selected B3 companies |

----------

### 📊 Dataset Structure

Each row represents **one company in one fiscal year**.

| Column                         | Description                   |
| ------------------------------ | ----------------------------- |
| DENOM_CIA                      | Company name                  |
| CD_CVM                         | CVM company registration code |
| Ticker_Alvo                    | B3 ticker                     |
| Data_Referencia                | Fiscal year end date          |
| Ano_Exercicio                  | Fiscal year                   |
| 01_Receita_Liquida_R$          | Net revenue                   |
| 02_Lucro_Bruto_R$              | Gross profit                  |
| 03_EBIT_Operacional_R$         | Operating EBIT                |
| 04_Resultado_Financeiro_R$     | Financial result              |
| 05_Lucro_Antes_Impostos_EBT_R$ | Earnings before taxes         |
| 06_Lucro_Liquido_R$            | Net income                    |

All monetary values are expressed in **Brazilian reais (R$)**.

----------

### 📁 Output Location

The generated dataset is stored in:

```
data/raw/06_CVM_Historico_Focado.csv
```

----------

### 🏃‍♂️ Running the Pipeline

To execute the CVM financial extraction pipeline:

``` bash
python 03_cvm_elite_extration.py
```
or 
``` bash
poetry run python 03_cvm_elite_extration.py
```

The pipeline will automatically:

-   map B3 tickers to CVM codes 
-   download historical DFP datasets
-   filter relevant companies
-   extract key financial metrics
-   generate the final dataset
    
----------

# ⚠️ Notes

-   CVM datasets are published annually and may contain missing years.
-   The pipeline includes retry mechanisms to handle temporary network failures.
-   Only companies included in `TICKERS_ALVO` are processed.
-   Financial values from CVM are reported in thousands and are converted to full Brazilian reais.
    
----------

# 💡 Use Cases

The generated dataset can be used for:

-   financial ratio analysis
-   valuation models
-   factor investing research
-   macro-market correlations
-   machine learning models for stock analysis