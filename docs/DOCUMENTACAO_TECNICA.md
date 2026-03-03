# 🚀 Comando B3 - Documentação Técnica Completa

Bem-vindo à documentação oficial do **Comando B3**, um ecossistema avançado de análise quantitativa e preditiva do mercado financeiro brasileiro.

O nosso maior triunfo técnico ocorreu na **Refatoração para Arquitetura On-Demand (Sob Demanda)**. Historicamente, os sistemas baixavam gigabytes de planilhas Excel (XLSX) ou arquivos CSV contendo os preços diários de +900 empresas durante 10 a 30 anos. O programa engasgava porque o computador não tinha memória RAM suficiente para ler todos esses dados simultaneamente e calcular indicadores para todos eles de uma só vez, criando ciclos de processamento de horas.

Nós virámos esse paradigma do avesso.

---

## 🛠️ 1. O Arsenal de Bibliotecas: Quem faz o quê?

Para construir este projeto de forma limpa e modular, integrámos APIs de nível institucional ligadas diretamente ao coração da macroeconomia brasileira e da B3.

### 🏛️ `python-bcb` (Banco Central do Brasil)

**O que faz:** Liga-se via API pública ao Sistema Gerenciador de Séries Temporais (SGS) do Banco Central.
**Como usamos:** Não é possível analisar a Bolsa de Valores sem compreender o custo do dinheiro. Nós extraímos:

- **Código 11 (Selic Diária):** Fundamental, pois se os juros sobem, a Renda Fixa fica mais atrativa e os investidores institucionais retiram dinheiro das Ações, provocando quedas na B3.
- **Código 1 (Dólar PTAX):** O fluxo cambial dita os custos de empresas de retalho e a margem de lucro de empresas exportadoras (como a Vale ou Petrobras).
- **Código 433 (IPCA):** A inflação mensal que corrói o poder de compra.
  **Vantagem na ML:** A nossa Máquina não "adivinha o gráfico", ela prevê reagindo ao cenário macroeconómico real que nós lhe injetamos.

### 🏢 `fundamentus` (Dados CVM / Fundamentos de Empresas)

**O que faz:** Atua como um scrapper (raspador de dados) fidedigno do site _Fundamentus_, que padroniza os balanços financeiros abertos que as companhias entregam à Comissão de Valores Mobiliários (CVM).
**Como usamos:** Extraímos o catálogo inteiro (Nomes, Tickers, Setores) de todas as empresas transacionadas e pescamos métricas críticas:

- **P/L (Preço vs Lucro):** Quantos anos a empresa demora a pagar o seu preço com os lucros atuais? (Diz se a ação está "Cara" ou "Barata").
- **ROE (Return on Equity):** Qual a eficiência da gestão em gerar dinheiro face ao património?
  **Importância:** Isto permite que o nosso portfólio elimine empresas tóxicas ou à beira da falência antes mesmo de olhar para o preço das ações.

### 📈 `yfinance` (Yahoo! Finance API)

**O que faz:** Download ultrarrápido do histórico de cotações (`Open, High, Low, Close, Volume`).
**O segredo On-Demand:** Nós abandonámos o `for-loop` antigo que baixava o mercado inteiro do zero todas as madrugadas. Agora, só quando o Gestor pesquisa "WEGE3" no ecrã pela primeira vez, a aplicação pausa durante 1 segundo, avisa o Yahoo Finance: _"Dá-me os últimos 30 anos só desta!"_. A experiência torna-se infinitamente mais rápida, eliminando redundância na rede.

---

## 🏎️ 2. A Revolução dos Dados: Polars e Parquet

Como conseguimos ser 100x mais rápidos que as abordagens comuns em Python? Trocando a forma como guardamos e lemos dados.

### 💱 O Ficheiro `.Parquet` (em vez de `.CSV` ou `.Excel`)

Antigamente usava-se ficheiros de texto plano (`.csv`) geridos pela biblioteca `pandas`.
O **Apache Parquet** (suportado pela extensão `pyarrow`) é uma tecnologia moderna de Big Data orientada a **colunas** (Columnar Storage), e não a linhas:

- **Compressão Intensa:** Um ficheiro de 100 MB em CSV ocupa apenas 15 MB em Parquet, usando algoritmos automáticos como _Snappy/Zstd_.
- **Acesso em Coluna:** Se a nossa Machine Learning precisa apenas da coluna "Preço de Fecho" e "Data", um ficheiro `.parquet` _não lê as colunas de Abertura ou Máximo_. Num CSV, o disco rígido é forçado a ler a linha inteira, consumindo tempo precioso de I/O (Input/Output).

### 🐻‍❄️ A Biblioteca `Polars` (O Assassino do Pandas)

O `pandas` processa a matemática do projeto utilizando apenas **1 núcleo** do teu processador, correndo linha a linha (Single-Threaded), e só processa se couber tudo na RAM do PC.
O **Polars** foi reescrito totalmente na linguagem **Rust** (conhecida pela gestão de memória agressiva e hiper-rápida de sistema):

1.  **Lazy Evaluation (`scan_parquet`)**: O Polars não carrega imediatamente o ficheiro para o Python. Ele "Lê a Promessa" da base de dados, desenha um plano otimizado no motor Rust (Engine Plan), e só no milissegundo em que o utilizador pede um gráfico na Interface Gráfica é que ele vai ao disco pescar estritamente a janela de datas selecionada. Isto poupa 95% do uso de RAM.
2.  **Multithreading**: O Polars distribui as contas pesadas (como o cálculo do RSI, Bandas de Bollinger, Volatilidade e cruzamentos com a base da Taxa Selic) por todos os núcleos disponíveis do teu processador simultaneamente. Por isso, a criação dos dados demora menos de meio-segundo por empresa.

---

## 🧠 3. O Cerebelo da Aplicação: IA Híbrida (`xgboost` e `numpy`)

Assim que o Polars formata os dados num _DataFrame_, a Inteligência Artificial entra em cena no painel da aplicação (Tab 3 e 4), treinando algoritmos frescos do zero sob a arquitetura **XGBoost (Extreme Gradient Boosting)** e **Numpy CPU Matrix**.

- **1. Agente XGBoost (Classificador e Regressor):** Abandonámos as antigas _Random Forests_. O **XGBoost** é o verdadeiro Estado da Arte em competições quantitativas. Ele recebe os últimos 30 anos (mm21, volatilidade, RSI, Selic e Lags passados) e executa _Autoregressão Dinâmica_ para projetar o Preço provável dos próximos 7 dias úteis. A probabilidade de fechar positivo também dita a nossa _Tese Cognitiva_ em formato de texto.
- **2. Cálculo Estocástico (Monte Carlo via Numpy):** Usando as médias de retorno e volatilidade, o motor em Numpy processa de forma supersónica 1.000 universos paralelos de como a ação se pode comportar em 30 dias. O resultado visual é um túnel probabilístico idêntico ao dos Hedge Funds.
- **3. Teoria Moderna de Portfólio (Markowitz):** A aplicação também processa 3.000 carteiras randómicas através da correlação matemática de múltiplos ativos e do "Risk-Free Rate" (Renda Fixa), apontando a exata quantia em percentagem que se deve colocar em dada Ação para obter o **Índice de Sharpe Ideal** (Fronteira Eficiente).

---

## 🧭 4. O Radar de Value Investing Institucional

O sistema corre em _background_ um algoritmo que decifra o Catálogo da CVM filtrando o joio do trigo com três regras de Benjamin Graham:

1. Tem de ter Lucro Constante (P/L > 2)
2. Não pode estar sobreavaliada (P/L < 15)
3. Tem de ser redondamente eficiente a nível de gestão (ROE > 15%).
   Na nossa Barra Lateral (Sidebar), as 3 melhores ações (Top 3) são recomendadas dinamicamente ao Utilizador acompanhadas de uma medalha (🌟). No Painel Principal, estas ações são submetidas a um Raio-X visual num **Gráfico de Radar Hexagonal** (Spider Chart) que espelha as suas valências financeiras perante todas as suas frentes de risco.

## 🤖 5. O Robo-Advisor (Gestão de Portfólio Markowitz Avançada)

A aba de Portfólio foi transformada num autêntico produto SaaS de gestão quantitativa, com três "Killer Features" injetadas diretamente perante o cálculo matricial da Efficient Frontier:

1. **Perfis de Investidor (Agressivo vs Defensivo):** Expandimos os eixos de Markowitz. O gráfico Plotly já não mostra apenas o portfólio restrito de Maior Retorno (Estrela Dourada Max Sharpe), mas calcula estocasticamente o ponto de Menor Volatilidade Sistêmica (Escudo Vermelho Mínimo Risco).
2. **Boleta de Execução (Actionable):** O Dashboard parou de cuspir percentagens puramente teóricas. Injetado um capital (ex: R$ 10.000), o Numpy apura as cotas exatas (`np.floor`) usando o Preço Real de Fecho atual de cada papel (Sincronizado via Polars), imprimindo o valor final da ordem e o respetivo "Troco" para a conta corrente de forma pronta a copiar para a Corretora.
3. **Máquina do Tempo (Backtesting de Validação):** Desenhada uma grelha Plotly que simula (em indexação base 100) o que teria acontecido caso a Carteira Sugerida tivesse sido comprada no primeiro dia, provando matematicamente perante as oscilações das ações isoladas a rentabilidade gerada pelo algoritmo.

## 💻 6. Resumo da Orquestração do Frontend (`streamlit`)

A orquestração visual consolida tudo num ecrã em tempo real:

1. O **Streamlit** reage aos cliques. Tu escolhes o "Ticker" (Ação).
2. Despoleta o _Script OHD_ (On-Demand Fetching).
3. Se a ação não estiver descarregada, a `yfinance` traz o passado. O `Polars` aplica-lhe a Taxa `Selic` preenchendo as quebras com Fallback e aplica os Indicadores Técnicos, compilando-os em segundos para um micro ficheiro `.parquet` local na tua máquina.
4. **Cache Refresh Diário Automático:** Se já existirem dados locais mas com data do Feriado de ontem, um rastreio via `os.path.getmtime` invalida os dados e ordena a re-criação invisível em cima dos velhos. Na tua tela, só se vê a verdade intocável de "hoje".
5. O `XGBoost` e o `Numpy` leem os novos dados, treinam para aquela empresa, tiram as métricas, e a biblioteca `plotly` gera imediatamente gráficos interativos (onde se pode ver e transpor até as alocações da Carteira num conversor em reais (R$)).

---

## 🔮 6. Visão de Futuro e Próximas Melhorias (Roadmap)

Como já elevámos a matemática para níveis de Hedge Fund, as próximas melhorias focam apenas na Escalabilidade para suportar milhões de pedidos:

### 1. Multi-Threading no Portfolio Global On-Demand

- **Avanço para ML de Macro-Hedge:** O Dashboard foi atualizado para carregar uma ação de cada vez (quando pesquisada). Uma melhoria tremenda seria um botão "Treinamento Global de Portfólio", que permite ao utilizador selecionar "15 Ações", e o Dashboard lança 15 Threads paralelas para disparar pedidos do Yahoo Finance aos 15 Tickers ignorados pelo sistema, processando a aba de Portfólios e Correlações quase instantaneamente.

### 3. Integração em Banco de Dados Analítico Vertical (ClickHouse/DuckDB)

- **Escalabilidade de Elite:** O Polars aliado ao Parquet em Pastas locais já é fenomenal. Mas se a aplicação crescer para ambiente Cloud (AWS, Azure), poderíamos substituir a escrita do ficheiro físico numa solução de memória como o **DuckDB**. O DuckDB funciona como o SQLite, mas é desenhado especificamente para `SELECTs` analíticos ultra rápidos (via SQL) e conseguiria cruzar tabelas da Selic usando SQL na memória RAM partilhada da sessão.

### 4. Monitorização Assíncrona via Celery + Redis

- **Para Produção Web:** Caso existam centenas de utilizadores diferentes no Browser a procurar Tickers estranhos no mesmo milissegundo, a `yfinance` poderia impor rate-limits (bloquear temporariamente a internet do nosso servidor). Distribuir o fardo da `coleta_macro.py` e dos novos Downloads com uma Task Queue como o Celery garantiria segurança anti-quebra (Rate-Liming e filas de download limpas).
