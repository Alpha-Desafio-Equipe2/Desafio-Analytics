# 🚀 Comando B3 Analytics: Sistema Institucional de Quant Analysis

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![Polars](https://img.shields.io/badge/polars-blazing%20fast-orange.svg)
![XGBoost](https://img.shields.io/badge/machine%20learning-XGBoost-blueviolet)
![Streamlit](https://img.shields.io/badge/dashboard-Streamlit-FF4B4B)
![Status](https://img.shields.io/badge/status-production_ready-success.svg)

**Comando B3 Analytics** é um ecossistema avançado de análise quantitativa e preditiva focado no mercado financeiro brasileiro. Desenvolvido para superar as limitações de arquiteturas monolíticas baseadas em Pandas/CSV, a aplicação opera através de uma revolucionária engenharia **On-Demand (Sob Demanda)**, utilizando **Polars** (Rust) para cruzamentos de alta performance e **Apache Parquet** para armazenamento otimizado.

---

## 🎯 Visão Executiva: Arquitetura e Engenharia de Dados

A arquitetura on-demand foi projetada para eliminar sobrecargas de memória, lendo gigabytes de dados históricos de forma inteligente, particionada e preguiçosa (_Lazy Loading_). O pipeline consiste em três pilares fundamentais:

1. **Fundação Macroeconômica e Dados da CVM (`coleta_macro.py`)**  
   Conecta-se à API do Banco Central e ao Fundamentus para extrair taxas críticas (*Selic, IPCA, Dólar*) e dados fundamentalistas das +900 empresas listadas na B3 (*P/L, ROE, Dívida*). Os dados são guardados de forma comprimida.
2. **Motor Analítico On-Demand (`processamento_dados.py`)**  
   Utilizando motor multi-thread do **Polars**, cruza séries financeiras da Bolsa, recalibra datas com a Selic (`forward_fill`) e apura indicadores técnicos complexos (Bandas de Bollinger, RSI, Médias Móveis) numa fração do tempo gasto pelas bibliotecas tradicionais.
3. **Orquestração Visual e IA (`dashboard_squad3.py`)**  
   A camada de front-end em Streamlit onde as requisições acontecem em tempo real. Se o ativo não estiver cacheado localmente, a extração via `yfinance` é engatilhada sob demanda apenas para aquele ativo.

---

## 🧠 Machine Learning & Otimização de Portfólio (Robo-Advisor)

Mais do que cruzar dados passados, o sistema possui **Inteligência Direcional e Gestão Estocástica** de ponta a ponta:

*   **XGBoost (Extreme Gradient Boosting):** O algoritmo analisa métricas macroeconômicas aliadas a indicadores técnicos para projetar padrões ocultos e ditar probabilidades autorregressivas de preço para os próximos 7 dias úteis.
*   **Otimizador Markowitz (Actionable):** Um motor NumPy efetua execuções de Monte Carlo, avaliando milhares de alocações paralelas e emitindo ordens matemáticas exatas (*Boletas de Compra* com cotas exatas "np.floor") conforme o seu perfil de risco (Agressivo ou Defensivo).
*   **Radar de Value Investing:** Filtros automatizados de Benjamin Graham monitorizam o mercado para recomendar e gerar gráficos de teia com os ativos mais atrativos segundo rácios fundamentalistas.

---

## ⚙️ Quick Start (Setup & Execução)

As instruções abaixo unem as necessidades de desenvolvimento e produção para rodar o radar analítico imediatamente.

### 1. Pré-Requisitos e Instalação
Garanta que você possui Python instalado. Clone este repositório e ative o seu ambiente virtual.
```bash
# Instalação das dependências
pip install -r requirements.txt
```

### 2. Inicialização do Catálogo (Run Once ou Diário)
Para abastecer o sistema com o universo de nomes da B3 e dados vitais do Banco Central:
```bash
python coleta_macro.py
```
*(Este script criará a diretoria `data/raw` populada com os alicerces .parquet)*

### 3. Execução da Aplicação (Dashboard Interativo)
Inicie o servidor Streamlit. O dashboard responderá `On-Demand` aos tickers pesquisados:
```bash
python -m streamlit run dashboard_squad3.py
```
Acesse `http://localhost:8501` e comece a testar ativos (ex: `VALE3`, `PETR4`, `WEGE3`). 

---

## 📚 Documentação Técnica Aprofundada

Para engenheiros de dados, cientistas de dados quantitativos ou desenvolvedores interessados em entender a fundo os cálculos estocásticos, a refatoração On-Demand e as decisões arquiteturais, visite a nossa documentação dedicada na pasta `docs/`:

- 📖 **[O Manual Técnico Definitivo](docs/DOCUMENTACAO_PROJETO.md)**: Explicação analítica detalhada linha a linha das funções núcleo, tratamento de falhas em APIs, e táticas de *Lazy Loading*.
- 🚀 **[Arquitetura e Bibliotecas](docs/DOCUMENTACAO_TECNICA.md)**: Estudo completo sobre a migração de Pandas para Polars, o modelo XGBoost e a Teoria de Portfólio de Markowitz aplicada.
- 🗃️ **[Dicionário de Dados](docs/data_dictionary.md)**: Estrutura completa das features derivadas nos pipelines `.parquet`.
- 🔍 **Pipelines Secundários e Extratos Específicos**:
  - [Pipeline CVM](docs/financial_statements_pipeline(CVM).md)
  - [Pipeline BCB Macro](docs/macro_data_pipeline(BC).md)
  - [Pipeline Yahoo Finance](docs/market_data_pipeline(YFinance).md)

---

*Desenhado com foco em resiliência, baixa latência e processamento institucional quantitativo de alto padrão.*
