# 📓 Documentação: 05_investimento_value_investing_insights.ipynb

Este diretório contém o Jupyter Notebook focado em **Value Investing Quantitativo** e **Deep Analytics**. O seu objetivo é transcender a análise clássica de preços e tentar encontrar "anomalias" no mercado, cruzando a cotação diária com os fundamentos divulgados pelas empresas.

Abaixo encontra-se a explicação detalhada do que acontece em cada secção e célula do notebook.

---

## 🏗️ 1. Preparação e Engenharia de Dados (O Motor)

### Carregamento de Dados (Célula 3)

O notebook começa puxando dois _datasets_ vitais:

1. **Preços Diários** (`01_yfinance_precos_raw.csv`): Histórico de fecho ajustado, abertura, máxima, mínima, dividendos e volume.
2. **Balanços Financeiros** (`04_yfinance_balancos_raw.csv`): O "Net Income" (Lucro Líquido) com a data exata da sua respectiva divulgação oficial.

### O "Grand Merge" Temporal (Célula 5)

A magia da análise quantitativa acontece aqui, dentro da função `preparar_dados_historicos(ticker)`.
Como o preço muda todos os dias, mas o lucro só é revelado a cada 3 meses, o código usa a função `pd.merge_asof(direction='backward')`. Isto significa que a cada dia de cotacão, a empresa "carrega" o último lucro que era público e conhecido no mercado naquele exato momento, até à divulgação do balanço seguinte.
Aqui também são geradas métricas técnicas avançadas:

- **Normalização Base 100:** Preço e Lucro são convertidos para o mesmo ponto de partida (100) para permitir que sejam sobrepostos na mesma escala.
- **Drawdown:** A queda percentual do preço desde o último pico máximo histórico.
- **Indicadores de Volume:** Volume Financeiro e a sua respetiva Média Móvel de 20 dias (`Vol_MA_20`).
- **Análise Técnica:** Médias Móveis de Preço (`SMA_50` e `SMA_200`) e a volatilidade contínua a 30 dias.
- **Valuation Bands:** A média histórica do múltiplo de Valuation, acompanhada por bandas superior e inferior (1 Desvio Padrão).
- **Yield TTM:** O Dividend Yield acumulado dinâmico de 12 meses.

---

## 📊 2. Dashboard Dinâmico de Ações (O Analista Micro)

As Células 7 e 8 geram o primeiro _dashboard_ interativo (`ipywidgets.Dropdown`), focado numa análise unitária profunda de um único Ticker. Quando o utilizador escolhe uma ação, 5 gráficos são desenhados instantaneamente:

1. **O Grande Espelho (Divergência de Lucros):** O gráfico mãe do Value Investing. Procura assimetrias onde a ação caiu de preço, mas os lucros continuaram a subir ao longo dos anos. A anomalia de juros de 2022 está destacada a amarelo.
2. **Termómetro de Valuation:** Um gráfico de dispersão com limites estatísticos. Se a linha roxa afunda abaixo da zona cinzenta (-1 Desvio Padrão), a empresa está matematicamente num ponto de desconto extremo em relação à sua própria história de preços.
3. **Teste Cardíaco & Turbulência do Mercado:** Um eixo duplo cruzando o Drawdown (dor no bolso) com a Volatilidade a 30 dias (pânico de curto prazo). Útil para encontrar pontos de estresse sistémico.
4. **Análise Técnica e Institucional (Smart Money):** Um gráfico autêntico de _Candlesticks_ com cruzamento de médias institucionais (Golden Cross e Death Cross), além do rastreio de barras de volume diárias no painel inferior.
5. **Sustentabilidade dos Dividendos:** Destrói a "Armadilha do Yield". Mostra em barra o pagamento de dividendo, em linha verde o Yield disparando, e em linha tracejada o lucro base. Se o Yield dispara, mas o lucro colapsou... é uma armadilha, a cotação vai cair logo a seguir.

---

## 🔬 3. Mega Prompt: Advanced Quantitative Analytics (O Gestor Macro)

As últimas três células contêm o motor para gerar relatórios de **Fronteira Eficiente e Análise Multivariada**.

Em vez de analisar uma ação, o utilizador usa o `ipywidgets.SelectMultiple` para escolher várias ações ao mesmo tempo (`Ctrl + Clique`) e gerar uma visão de portefólio.

### O DataFrame Master (`df_master`)

Uma função dedicada reconstrói toda a história das ações selecionadas num único formato longo (Long Format), injetando:

- Retorno Logarítmico Contínuo (`log_return`)
- Retorno Acumulado Composto Diário (`retorno_acumulado`)
- Volatilidade Anualizada a 252 dias (`volatilidade_252d`)
- Proxies temporárias para o ROE.

### Os 5 Gráficos de Inteligência Quantitativa

1. **Evolução Real (Retorn Acumulado):** Em vez de comparar gráficamente o preço (que tem valores absolutos diferentes), compara a percentagem ganha partindo todos do zero (mesma corrida monetária).
2. **Fronteira de Markowitz (Risco vs Retorno):** O gráfico seminal da teoria moderna de portefólios. Revela se o investidor está a ser adequadamente compensado com Retorno (Eixo Y) para o nível de Risco (Eixo X) que assume.
3. **Heatmap de Correlação de Seaborn (Diversificação):** Analisa a matriz de correlação de Pearson entre todos os ativos. Ativos cuja correlação seja menor do que 0.3 ou próxima de 0, reduzem brutalmente o risco de ruína de um portefólio, visto que não caem juntos.
4. **Cisnes Negros (Boxplots de Cauda Gorda):** Revela a distribuição real dos retornos em `log`. Outliers muito afastados das caixas para baixo representam choques sistémicos, "flash crashes" e o nível de robustez frágil que a ação teve.
5. **Radar de Value Investing (P/L vs ROE):** Procura a zona ótima do mercado. O quadrante ideal a tentar encontrar é na metade da Esquerda (Baixo P/L = Barato) e na metade de Cima (Alto ROE = Altamente Rentável/Eficiente).

---

_Desenvolvido pela pipeline de Extração e Masterização CVM/YFinance_
