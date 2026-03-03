# 🚀 Comando B3 - Arquitetura Institucional On-Demand

Bem-vindo ao Comando B3, um ecossistema completo de análise de dados do mercado financeiro brasileiro, desenhado para lidar com todo o catálogo da B3 de forma escalável, ultra-rápida (powered by Polars) e livre de sobrecarga de memória (Lazy Loading).

---

## 🏗️ 1. Arquitetura do Sistema e Funcionamento

O projeto foi refatorado para uma arquitetura "On-Demand" (Sob Demanda), substituindo antigos processos em lote que gastavam gigabytes de RAM. A arquitetura divide-se em 3 pilares:

### Pilar 1: Fundação Macroeconómica e Catálogo (`coleta_macro.py`)

Este script é a base imutável que deverá correr (por exemplo, diariamente numa cronjob). Ele não baixa cotações pesadas.

- **Dados do Banco Central:** Utiliza a biblioteca `python-bcb` (Sistema Gerenciador de Séries Temporais do BCB) para capturar a essência da economia: **Taxa Selic (11)**, **Dólar PTAX (1)** e **IPCA (433)**. O custo de oportunidade (Selic) é matematicamente integrado a todos os retornos.
- **Fundamentos da Empresa:** Utiliza a API `fundamentus` estruturada sobre os relatórios da CVM (Comissão de Valores Mobiliários) para puxar um catálogo de todas as +900 ações, e armazena os Rácios Fundamentais como **P/L** (Preço/Lucro), **ROE** (Retorno sobre Património) e **Dívida Bruta/Património**.
- **Armazenamento:** Salva estes dados como `.parquet` leves na pasta `data/raw/`.

### Pilar 2: Motor de Engenharia de Dados On-Demand (`processamento_dados.py`)

Aqui é onde o verdadeiro poder matemático do sistema reside. Em vez de ler centenas de CSVs usando Pandas (que colapsaria o servidor), utiliza `polars` em Rust.

- **Indicadores Técnicos:** Calcula Médias Móveis (MM21, MM200), Bandas de Bollinger, MACD, Volatilidade em janelas rolantes e Índice de Força Relativa (RSI).
- **Data Joining:** Funde num único frame tridimensional o Preço com o catálogo da Empresa e com os dados Macroeconómicos exatos daquele dia (`forward_fill` para ajustar fins-de-semana).
- **Fração do Tempo:** O processador materializa (`collect()`) tudo num ficheiro particionado final (`data/processed/*_processed.parquet`) para evitar recálculos no futuro.

### Pilar 3: Orquestrador Visual e Machine Learning (`dashboard_squad3.py`)

A ponta do Iceberg, onde o utilizador interage num ambiente Streamlit.

- **Lazy Fetching (yfinance):** Se procurares "WEGE3" na barra de pesquisa (oriunda do catálogo Fundamentus), e o sistema não tiver ainda os dados no servidor, o Dashboard avisa o script On-Demand. A aplicação usa `yfinance` e extrai retroativamente 30 anos de bolsa apenas dessa ação, injetando-a num instante (2 a 3 segundos no máximo) no nosso Motor Polars e construindo o ficheiro local na mosca.
- **Polars Lazy API (`scan_parquet`):** Se já existem dados, o Polars analisa o ticket físico sem encher a RAM, lê **só as datas** que pediste no slider do dashboard e constrói imediatamente os gráficos complexos (Drawdown, Desempenho Face à Selic).

---

## 🤖 2. Machine Learning Híbrida: XGBoost e Robo-Advisor

O nosso motor de IA utiliza o famoso **XGBoost (Extreme Gradient Boosting)** aliado à **Computação Estocástica (Numpy)**. O diferencial enorme do nosso projeto não é apenas "adivinhar o preço usando o preço de ontem", mas sim cruzar contextos: **O Padrão Técnico + O Padrão Institucional Multivariado**.

### Features (Variáveis) que a Máquina Aprende:

A máquina é literalmente injetada com sabedoria institucional:

- **Indicadores de Pressão (CVM):** A ação está muito esticada face ao lucro atual (`P/L`) ou face ao capital investido (`ROE`)?
- **Indicadores Momentâneos (Técnicos):** Como está a inclinação do `MACD`, a dinâmica da Banda `Bollinger` Inferior e a volatilidade matemática dos últimos 21 dias?
- **Custo de Capital (Macro BCB):** O Governo mudou a `Selic` hoje? A evolução do Câmbio `Dólar` está a beneficiar o exportador?

### O que os 2 Modelos Fazem Realmente?

Isto não é apenas análise de risco académica; a aplicação foca em dois domínios reais de produção de capital:

1. **A Inteligência Direcional (XGBoost):** Esqueça as linhas lisas que erram no amanhã. O **XGBoost** executa "Super-Regressão" em Janelas Contínuas e produz uma "Probabilidade Percentual de Ganho", convertendo as matemáticas difíceis num Parecer Cognitivo de Leitura fácil na aba de Inteligência Artificial usando um _Candlestick_ visual e simulador de Monte Carlo.
2. **O Gestor Criptográfico (Markowitz Actionable):** Em vez de cuspir "Alocação 30% / 70%", o Otimizador permite que o investidor escolha ser "Agressivo" ou "Defensivo". Lendo os preços exatos atuais do banco de dados `Polars`, ele calcula a **Boleta de Compra Comercial** (ex: Compre 100 cotas, custará R$ 2.400 e sobrarão R$ 15). De forma audaz, atrela ainda a Máquina do Tempo (_Backtester_), simulando o enriquecimento base-100 desta carteira no histórico provando o Alpha.

## Como Executar Todo o Fluxo num Novo Servidor

1.  **Dependências:** Instala as bibliotecas necessárias executando `pip install -r requirements.txt` no Terminal.
2.  **Fundação:** Corre no Terminal `python coleta_macro.py`. Isto baixará todos os nomes das empresas à face da terra do Brasil e a Taxa Básica de Juros desde 1995. Demorará cerca de 2 segundos.
3.  **O Show:** Corre no Terminal `python -m streamlit run dashboard_squad3.py`.
4.  **On-Demand:** Abre sempre o endereço no browser (ex: `http://localhost:8501`), digita qualquer ativo que desponte no teu radar, como "VALE3" ou "BBDC4", e deixa o motor ir ao cofre financeiro do Yahoo, processar com ferramentas do Polars as colunas inteiras da nossa arquitetura baseada no que tu precisas no exato segundo.

Documento concebido meticulosamente por _Antigravity AI Agent_.
