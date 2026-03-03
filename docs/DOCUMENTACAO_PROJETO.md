# 📖 Documentação Técnica Detalhada: Códigos B3 Analytics

Este documento é o **Manual Técnico Definitivo** focado em explicar _linha a linha_, de forma didática, como os códigos de extração, processamento e o dashboard funcionam. Ideal para colegas programadores ou analistas de dados entenderem os fundamentos matemáticos e lógicos implementados.

---

## 1. Módulo de Coleta (`coleta_macro.py`)

Responsável pela **Extração em Massa (Extract)** de todo o ecossistema financeiro via APIs governamentais e mercados privados. Embora o código seja curto, a "magia" que ocorre em background é extremamente poderosa:

### 🤫 A Blindagem de Erros (O "Silenciador" do Yahoo)

Mesmo no topo do nosso código, vão ver as linhas `logging.getLogger('yfinance').setLevel(logging.CRITICAL)`. O que é isto?

- Pela CVM vêm os nomes de +900 empresas, mas algumas (ex: MMXM3 do Eike Batista) estão falidas, mudaram de nome ou fundiram-se. Quando a aplicação tenta procurar o gráfico destas "empresas zumbis", a Yahoo desata aos "gritos" com mensagens de erro vermelhas gigantes ("_possibly delisted_") na tela preta. Esse código atua literalmente como uma fita adesiva na boca da biblioteca: "Se não encontrares a ação, não fales nada. Salta calado para a próxima!".

### 🏢 `gerar_catalogo_b3(pasta_base)`

- **O que faz:** Vai à internet "raspar" (scraping) a saúde financeira fundamental das empresas listadas no Brasil.
- **O grande truque:** O código não entra na Bolsa de Valores. Ele entra na biblioteca `fundamentus` (que lê os balanços abertos oficiais que as +900 empresas entregam à CVM). Ele faz o download de todo o "Sangue" da empresa em milissegundos: Lucros, Dívidas e ROE de todas as ações listadas, e devolve isso compactado no formato `.parquet` (10x mais pequeno que Excel). Isto forma o cardápio central do nosso projeto!

### 🏛️ `coletar_macroeconomia(pasta_base, ano_inicio)`

- **O que faz:** Entra em contato oficial com os servidores do **Banco Central do Brasil (`bcb.sgs`)**.
- **A Tática da Salsicha (Como contorna bloqueios):** Os portais do governo odeiam que um computador lhes tente arrancar 30 anos de inflação (IPCA) e da Taxa Selic de uma só vez (resulta no famoso "Erro 406 _Not Acceptable_"). Então a nossa função é genial: Ela pega no ano de 1995 e envia os pedidos fatiados em "lotes de 5 em 5 anos" através de um laço (`for`). No fim, junta todos os pedacinhos num bloco massivo e limpo usando o comando de cola (`pd.concat()`). É invencível a quedas de servidor!

### 🚀 `rodar_pipeline_diaria_macro()`

- **O Maestro Orchestrador:** Para nós não termos de chamar cada coisinha à vez, ativamos esta última função de um "só clique" que comanda todas as anteriores, criando logo a pastinha `data/raw` no disco e metendo os 3 Pilares cruciais lá dentro, prontos a serem digeridos pela Inteligência Artificial.

---

---

## 2. Módulo de Processamento (`processamento_dados.py`)

A "Cozinha" Matemática do projeto. O utilizador comum do computador usa a biblioteca _Pandas_ para ler tabelas, mas nós trocámos isso pelo motor **Polars**. Porquê? Porque o Pandas lê as linhas de Excel uma a uma, enquanto o Polars usa uma tecnologia nova em _Rust_ que cruza colunas aos milhões em paralelo.

### 🧠 Como a Função Processar Funciona (A Mágica do Polars)

- **O Lazy Loading (Carregamento Preguiçoso):** A maior dor de cabeça de ler dados da bolsa é que 30 anos de dados destroem a memória RAM do teu PC. A função `pl.scan_parquet()` é mágica: Ela não abre o ficheiro. Ela apenas "Cria um Plano" de como vai abrir. Só quando chega o comando final `.collect()` (que atua como um gatilho) é que o PC vai buscar à pasta exatamente o que tu precisas, poupando 95% do esforço.
- **O Engenheiro Matemático (A Transformação):**
  - **Médias Móveis (`rolling_mean`):** A máquina soma o preço dos últimos 21 dias e 200 dias para perceber se estamos a subir ou descer perante um "histórico longo".
  - **Índice de Força Relativa (RSI de 14 dias):** É a balança. Se a ação subir muitos dias seguidos, o RSI dispara para valores de "Sobreaquecimento". Indica que em breve todos vão querer vender para meter os lucros ao bolso.
  - **Bandas de Bollinger:** Desenha um "Tubo" em volta da ação (Desvio Padrão \* 2). Funciona como a gravidade. Se a ação fura a linha de cima, o mercado corrige-a para baixo.

### 🔗 O "Join" Tridimensional

A genialidade do cruzamento. A função `q.join()` atua como uma fita adesiva, colando três dados que antes viviam separados:

1. No eixo do Preço (`.SA`), cola a **Taxa Selic**. Como a Selic não sai aos sábados nem feriados comerciais, a máquina usa a função ninja `.forward_fill()`. O que ela faz? Pega no valor de Sexta-feira da Selic e "arrasta" para os buracos invisíveis de Sábado e Domingo. A bolsa passa a estar calibrada pelo Governo!
2. Depois, cola lado-a-lado a saúde do balanço da **CVM/Fundamentus**. Constrói uma tabela mestra unificada, compactada no formato `.parquet` (10x mais pequeno e veloz que o CSV).

---

## 3. Módulo de Front-End e Inteligência (O Robô `dashboard_squad3.py`)

Este é o **Palco** da aplicação, construído num ambiente _Streamlit_. Tudo o que o código faz até agora desagua num Visualizador interativo e de nível Institucional.

### 🎭 A Aba de Inteligência Direcional (Motor XGBoost)

Em vez de desenhar gráficos básicos (como toda a gente faz no Pandas), nós introduzimos Matemática Avançada (Machine Learning):

- **A Memória de Curto Prazo (`Lags`):** Não se consegue adivinhar o amanhã olhando só para a vela de hoje. A máquina cria linhas "fantasma" do passado que decoram como estava a inflação e a bolsa há 1 dia, 2 dias e 3 dias atrás.
- **A Previsão Autoregressiva:** É aqui que atua o cérebro do algoritmo chamado **XGBoost (Decision Trees)**. A máquina devora os 30 anos inteiros e identifica padrões que os humanos não veem ("Ah, sempre que a Selic subia num dia em que o RSI batia em 70, a ação caía durante 3 dias!"). E projeta no gráfico do Dashboard um **Candlestick Tradicional** e uma linha de tendência laranja e pontilhada predizendo a oscilação probabilística da próxima semana de forma totalmente autóctona da Cloud.

### ⚖️ A Aba de Otimização e Execução (Markowitz Robo-Advisor)

Foi aqui que o projeto deixou de ser académico para ser um produto comercial para Investidores Reais. O Professor Markowitz fundou a "Teoria Moderna de Portfólio":

- **Matriz Numérica de 3.000 Portfólios:** No Streamlit, tu selecionas por exemplo _Itaú_ e _Petrobras_. O processador `numpy` corre 3.000 universos paralelos na memória atirando dinheiro ao ar (50%-50%, 80%-20%, 3%-97%).
- **O Ficheiro da "Boleta" Actionable:** Se disseres "Eu tenho R$ 10.000" e clicares para a máquina desenhar o teu Perfil Conservador (Defensivo / Escudo), o NumPy puxa o `Math.Floor` (Arredonda por baixo) num segundo invisível. Ele ignora teóricas percentuais, pega no preço da ação e escreve expressamente: "Compre _exatamente_ 120 cotas de VALE3, o montante total vai ser R$ X e sobrarão na sua conta corrente R$ 40 em Troco." Isto é um espelho formidável do comportamento de uma corretora!

### ⏳ A Máquina do Tempo (Backtest Matemático Criptográfico)

A prova de excelência para professores céticos de que não é vudu. A linha preta final na área de Portfólio (`fig_bt`) gera um "Backtest" à moda antiga. Indexa a tua carteira simulada perfeita por Markowitz à base 100 no dia de anos passados e prova que, usando o algoritmo com proteção de Risco (Selic) que atrai, a tua carteira vence historicamente as cotações nervosas sozinhas!

**Tudo isto está blindado (Anti-Errors), limpo de avisos irritantes e a correr sem tocar na API do Google Gemini, 100% num modelo de arquitetura _Local-Offline_ de alto sigilo e segurança!**
