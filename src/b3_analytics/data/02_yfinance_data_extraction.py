import yfinance as yf
import pandas as pd
import fundamentus
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import logging
from b3_analytics.utils.paths import RAW_DIR as PASTA_DESTINO

# =====================================================================
# CONFIGURAÇÕES E BLINDAGEM DE ERROS
# =====================================================================
warnings.filterwarnings('ignore')
# Silencia erros do YFinance para não inundar o terminal quando uma ação não existir
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Usado em caso de falha de download
FALLBACK_TICKERS = [
    "PETR4.SA",
    "VALE3.SA",
    "ITUB4.SA",
    "BBDC4.SA",
    "BBAS3.SA",
    "WEGE3.SA",
]

# =====================================================================
# 0. DESCOBERTA DE TODOS OS TICKERS DA B3
# =====================================================================
def obter_todos_tickers_b3() -> list:
    """
    Obtém a lista de todas as ações atualmente negociadas na B3.

    A função utiliza a biblioteca `fundamentus` para consultar o site
    Fundamentus e identificar os ativos listados na bolsa brasileira.
    Os tickers retornados são convertidos para o formato utilizado pelo
    Yahoo Finance, adicionando o sufixo ".SA".

    Returns
    -------
    list
        Lista contendo os tickers das ações no formato compatível com
        o Yahoo Finance (ex: "PETR4.SA", "VALE3.SA").

    Notes
    -----
    Caso ocorra algum erro na consulta (ex.: falha de conexão, mudança
    na estrutura do site ou indisponibilidade do serviço), a função
    retorna uma lista reduzida de tickers amplamente negociados como
    mecanismo de salvaguarda para permitir a continuidade do pipeline.

    Examples
    --------
    >>> obter_todos_tickers_b3()
    ['PETR4.SA', 'VALE3.SA', 'ITUB4.SA', ...]
    """
    print("🌍 [0/4] A mapear todo o universo de ações da B3...")
    try:
        # get_resultado() traz um DF onde o index é o nome da ação (ex: PETR4)
        df_ativos = fundamentus.get_resultado()
        
        # Extrai os nomes e adiciona o sufixo '.SA' exigido pelo Yahoo Finance
        tickers = [str(ticker) + ".SA" for ticker in df_ativos.index.tolist()]
        print(f"   🎯 Sucesso! Encontradas {len(tickers)} empresas ativas na B3.")
        return tickers
    except Exception as e:
        print(f"   ❌ Erro ao buscar lista de ativos: {e}")
        print("⚠️ Usando lista fallback de tickers.")
        # Retorna uma lista de salvaguarda caso a internet falhe
        return FALLBACK_TICKERS

# =====================================================================
# 1. EXTRAÇÃO DE PREÇOS (O MÁXIMO DE COLUNAS)
# =====================================================================
def extrair_precos_maciamente(tickers: list, data_inicio: str) -> pd.DataFrame:
    """
    Extrai preços históricos de múltiplos ativos da B3 utilizando a API do Yahoo Finance.

    A função realiza o download massivo de dados históricos de mercado
    (OHLCV) para uma lista de tickers, utilizando processamento paralelo
    fornecido pela biblioteca `yfinance`. Os dados retornados são
    reorganizados em formato tabular, com cada linha representando
    um ativo em uma determinada data.

    Parameters
    ----------
    tickers : list
        Lista de tickers das ações a serem consultadas no formato do
        Yahoo Finance (ex: ["PETR4.SA", "VALE3.SA"]).

    data_inicio : str
        Data inicial da coleta dos dados históricos no formato
        'YYYY-MM-DD'.

    Returns
    -------
    pd.DataFrame
        DataFrame contendo os preços históricos no formato OHLCV
        (Open, High, Low, Close, Adjusted Close e Volume), com as
        seguintes colunas principais:

        - Date : data do pregão
        - Ticker : identificador do ativo
        - Open : preço de abertura
        - High : preço máximo
        - Low : preço mínimo
        - Close : preço de fechamento
        - Adj Close : preço ajustado por eventos corporativos
        - Volume : volume negociado no dia

    Notes
    -----
    - O download é executado de forma paralela (`threads=True`) para
      acelerar a coleta quando há muitos ativos.
    - A estrutura retornada pelo Yahoo Finance possui colunas
      hierárquicas (multi-index), sendo necessário reorganizar os
      dados utilizando `stack()` para transformar em formato tabular.
    - Caso ocorra falha no download (ex.: problemas de conexão ou
      indisponibilidade da API), a função retorna um DataFrame vazio
      para evitar quebra do pipeline.

    Examples
    --------
    >>> tickers = ["PETR4.SA", "VALE3.SA"]
    >>> df = extrair_precos_maciamente(tickers, "2015-01-01")
    >>> df.head()
    """
    print(f"\n📉 [1/4] Download massivo de PREÇOS Históricos (10 anos)...")
    print("   ⏳ O download paralelo começou. Isto pode demorar alguns minutos...")
    
    try:
        dados_b3_full = yf.download(tickers, start=data_inicio, threads=True, ignore_tz=True)
        
        # O stack junta as ações em linhas, mantendo Open, High, Low, Close, Adj Close e Volume
        # Nota: future_stack=True removido por incompatibilidade com Pandas < 2.1 utilizado no ambiente
        df_b3_bruto = dados_b3_full.stack(level=1).reset_index()
        df_b3_bruto.rename(columns={'level_1': 'Ticker'}, inplace=True)
        
        print(f"   ✅ Preços extraídos! {len(df_b3_bruto)} linhas de OHLCV salvas.")
        return df_b3_bruto
        
    except Exception as e:
        print(f"   ❌ Erro crítico no download de preços: {e}")
        return pd.DataFrame()

# =====================================================================
# 2. EXTRAÇÃO DE EVENTOS CORPORATIVOS (DIVIDENDOS E SPLITS)
# =====================================================================
def extrair_eventos_corporativos(tickers: list) -> pd.DataFrame:
    """
    Extrai eventos corporativos (dividendos e desdobramentos de ações)
    para uma lista de ativos utilizando a API do Yahoo Finance.

    A função percorre todos os tickers informados e consulta os eventos
    corporativos disponíveis através do objeto `Ticker.actions` da
    biblioteca `yfinance`. Esses eventos incluem pagamentos de dividendos
    e operações de stock split que podem impactar diretamente a série
    histórica de preços das ações.

    Parameters
    ----------
    tickers : list
        Lista de tickers das ações no formato utilizado pelo Yahoo Finance
        (ex: ["PETR4.SA", "VALE3.SA"]).

    Returns
    -------
    pd.DataFrame
        DataFrame contendo os eventos corporativos identificados.
        Cada linha representa um evento associado a um ativo específico.

        Estrutura típica das colunas:

        - Date : data do evento corporativo
        - Dividends : valor do dividendo pago por ação
        - Stock Splits : fator de desdobramento da ação
        - Ticker : identificador do ativo (sem sufixo ".SA")

    Notes
    -----
    - A função realiza consultas sequenciais para cada ticker,
      pois a API do Yahoo Finance não oferece download massivo
      para eventos corporativos.
    - Caso um ativo não possua eventos registrados ou ocorra
      falha na consulta, o erro é ignorado e o processamento
      continua para os demais tickers.
    - Após a consolidação dos dados, a coluna de data é convertida
      para o tipo `datetime` sem timezone para facilitar análises
      temporais e integração com outras bases de preços.

    Examples
    --------
    >>> tickers = ["PETR4.SA", "VALE3.SA"]
    >>> df_eventos = extrair_eventos_corporativos(tickers)
    >>> df_eventos.head()
    """
    print(f"\n🔄 [2/4] A extrair EVENTOS CORPORATIVOS (Dividendos e Splits de {len(tickers)} ações)...")
    df_eventos = pd.DataFrame()
    
    # Progresso visual simples
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        if i % 50 == 0 and i > 0:
            print(f"   ... processadas {i}/{total} ações")
            
        try:
            acao = yf.Ticker(ticker)
            acoes_corp = acao.actions # Traz Dividendos + Stock Splits
            
            if not acoes_corp.empty:
                df_temp = acoes_corp.reset_index()
                df_temp['Ticker'] = ticker.replace('.SA', '')
                df_eventos = pd.concat([df_eventos, df_temp], ignore_index=True)
        except Exception:
            pass # Ignora silenciosamente se não houver dados
            
    if not df_eventos.empty and 'Date' in df_eventos.columns:
        # Força conversão para datetime caso a concatenação tenha convertido os tipos para string genérica
        df_eventos['Date'] = pd.to_datetime(df_eventos['Date'], utc=True)
        df_eventos['Date'] = df_eventos['Date'].dt.tz_localize(None)
        
    print(f"   ✅ Eventos extraídos! {len(df_eventos)} registos de dividendos/splits.")
    return df_eventos

# =====================================================================
# 3. EXTRAÇÃO DE METADADOS E FUNDAMENTOS (.info)
# =====================================================================
def extrair_info_avancada(tickers: list) -> pd.DataFrame:
    """
    Extrai metadados e indicadores fundamentalistas de múltiplas ações
    utilizando a API do Yahoo Finance.

    A função percorre uma lista de tickers e consulta o endpoint
    `Ticker.info` da biblioteca `yfinance`, que retorna um dicionário
    contendo informações cadastrais da empresa, métricas de mercado,
    indicadores fundamentalistas e dados de risco.

    Esses dados podem incluir, entre outros:

    - Informações da empresa (nome, setor, indústria, país)
    - Indicadores fundamentalistas (P/E, P/B, market cap, EBITDA)
    - Métricas de risco e mercado (beta, volatilidade, volume médio)
    - Informações de negociação (exchange, moeda, tipo de ativo)

    Parameters
    ----------
    tickers : list
        Lista de tickers das ações no formato do Yahoo Finance
        (ex: ["PETR4.SA", "VALE3.SA"]).

    Returns
    -------
    pd.DataFrame
        DataFrame contendo os metadados e indicadores fundamentalistas
        extraídos para cada ativo. Cada linha representa uma empresa
        e as colunas correspondem aos diferentes atributos retornados
        pela API do Yahoo Finance.

        A quantidade de colunas pode variar ao longo do tempo, pois o
        Yahoo Finance pode adicionar ou remover campos da resposta.

    Notes
    -----
    - A extração é realizada sequencialmente para cada ticker, pois
      a API do Yahoo Finance não oferece download massivo para esse
      tipo de metadado.
    - Em média, mais de 100 atributos podem ser retornados por ativo.
    - Caso ocorra erro na consulta ou o ticker não possua informações
      disponíveis, o ativo é ignorado e o processamento continua.
    - A coluna `Ticker` é adicionada manualmente ao dataset para
      facilitar a identificação e integração com outras tabelas
      do pipeline.

    Examples
    --------
    >>> tickers = ["PETR4.SA", "VALE3.SA"]
    >>> df_info = extrair_info_avancada(tickers)
    >>> df_info.head()
    """
    print(f"\n🧠 [3/4] A extrair METADADOS E FUNDAMENTOS GLOBAIS (Pode demorar 5-10 minutos)...")
    lista_infos = []
    
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        if i % 50 == 0 and i > 0:
            print(f"   ... extraídas informações de {i}/{total} ações")
            
        try:
            acao = yf.Ticker(ticker)
            info = acao.info
            # Verifica se o dicionário não está vazio e tem dados relevantes
            if info and 'symbol' in info:
                info['Ticker'] = ticker.replace('.SA', '')
                lista_infos.append(info)
        except Exception as e:
            print(f"Erro ao coletar info de {ticker}: {e}")
            
    df_info = pd.DataFrame(lista_infos)
    num_colunas = len(df_info.columns) if not df_info.empty else 0
    print(f"   ✅ Metadados extraídos! Matriz com {num_colunas} colunas brutas capturada para {len(df_info)} ações.")
    return df_info

# =====================================================================
# 4. EXTRAÇÃO DE BALANÇOS (DRE / INCOME STATEMENT)
# =====================================================================
def extrair_balancos_anuais(tickers: list) -> pd.DataFrame:
    """
    Extrai demonstrações financeiras anuais (Income Statement / DRE)
    das empresas listadas utilizando a API do Yahoo Finance.

    A função percorre uma lista de tickers e consulta a propriedade
    `financials` do objeto `Ticker` da biblioteca `yfinance`, que
    retorna a Demonstração do Resultado do Exercício (DRE) anual
    padronizada pelo Yahoo Finance.

    Os dados retornados incluem diversas métricas financeiras,
    como receita, lucro bruto, despesas operacionais e lucro líquido.
    Cada demonstração é transposta para formato tabular para facilitar
    análises e integração com outras bases do pipeline.

    Parameters
    ----------
    tickers : list
        Lista de tickers das ações no formato utilizado pelo Yahoo Finance
        (ex: ["PETR4.SA", "VALE3.SA"]).

    Returns
    -------
    pd.DataFrame
        DataFrame contendo as demonstrações financeiras anuais
        consolidadas para todas as empresas processadas.

        Estrutura típica:

        - Data_Balanco : data de referência da demonstração financeira
        - Ticker : identificador da empresa
        - Demais colunas correspondem às métricas financeiras
          disponibilizadas pelo Yahoo Finance (ex: Total Revenue,
          Gross Profit, Operating Income, Net Income, etc.).

    Notes
    -----
    - Os dados são extraídos individualmente para cada ticker,
      pois a API do Yahoo Finance não oferece download massivo
      de demonstrações financeiras.
    - Nem todas as empresas possuem dados disponíveis; nesses casos
      o ativo é ignorado e o processamento continua.
    - A estrutura das colunas pode variar ao longo do tempo,
      pois o Yahoo Finance pode adicionar ou remover métricas
      financeiras da resposta da API.
    - As demonstrações são retornadas originalmente com as datas
      nas colunas; por isso os dados são transpostos (`.T`) para
      que cada linha represente um período contábil.

    Examples
    --------
    >>> tickers = ["PETR4.SA", "VALE3.SA"]
    >>> df_balancos = extrair_balancos_anuais(tickers)
    >>> df_balancos.head()
    """
    print(f"\n📊 [4/4] A extrair BALANÇOS FINANCEIROS (Income Statement)...")
    df_balancos = pd.DataFrame()
    
    total = len(tickers)
    for i, ticker in enumerate(tickers):
        if i % 50 == 0 and i > 0:
            print(f"   ... extraídos balanços de {i}/{total} ações")
            
        try:
            acao = yf.Ticker(ticker)
            dre = acao.financials 
            
            if not dre.empty:
                dre_transposto = dre.T.reset_index()
                dre_transposto.rename(columns={'index': 'Data_Balanco'}, inplace=True)
                dre_transposto['Ticker'] = ticker.replace('.SA', '')
                
                df_balancos = pd.concat([df_balancos, dre_transposto], ignore_index=True)
        except Exception as e:
            print(f"Erro ao extrair balanço de {ticker}: {e}")
            
    print(f"   ✅ Balanços extraídos! {len(df_balancos)} registos financeiros anuais globais capturados.")
    return df_balancos

# =====================================================================
# MOTOR DE EXECUÇÃO PRINCIPAL
# =====================================================================
def run_market_data_pipeline():
    """
    Executa o pipeline completo de coleta de dados de mercado da B3
    utilizando a API do Yahoo Finance.

    Esta função orquestra todas as etapas do processo de extração de
    dados financeiros, criando um pequeno data lake contendo preços
    históricos, eventos corporativos, metadados das empresas e
    demonstrações financeiras.

    O pipeline realiza as seguintes etapas:

    1. Cria a pasta de destino para armazenamento dos dados brutos.
    2. Define o período de coleta (últimos 10 anos).
    3. Obtém a lista completa de tickers ativos da B3.
    4. Executa quatro níveis de extração de dados:

       - Nível 1: Preços históricos (OHLCV + Adjusted Close)
       - Nível 2: Eventos corporativos (Dividendos e Stock Splits)
       - Nível 3: Metadados e indicadores fundamentalistas das empresas
       - Nível 4: Demonstrações financeiras anuais (Income Statement / DRE)

    Cada conjunto de dados é salvo separadamente em arquivos CSV na
    pasta de destino, seguindo uma numeração sequencial que facilita
    a organização do data lake.

    Files Generated
    ---------------
    - 01_yfinance_precos_raw.csv
        Histórico de preços das ações (OHLCV).
    - 02_yfinance_eventos_raw.csv
        Eventos corporativos como dividendos e desdobramentos.
    - 03_yfinance_info_raw.csv
        Metadados e indicadores fundamentalistas das empresas.
    - 04_yfinance_balancos_raw.csv
        Demonstrações financeiras anuais padronizadas.

    Notes
    -----
    - O pipeline utiliza dados da biblioteca `yfinance`, que depende
      da infraestrutura do Yahoo Finance e pode apresentar eventuais
      limitações ou inconsistências.
    - Caso alguma etapa de extração retorne um DataFrame vazio, o
      respectivo arquivo não será gerado.
    - Os arquivos são exportados em formato CSV utilizando separador
      ';' e padrão decimal ',' para facilitar uso em ambientes que
      seguem convenções regionais brasileiras.

    Examples
    --------
    >>> run_market_data_pipeline()

    Após a execução, os datasets estarão disponíveis no diretório
    configurado em `PASTA_DESTINO`.
    """
    print("🚀 PIPELINE MAXIMUM YFINANCE B3 - INICIADO\n")
    
    # 0. Cria as pastas e define as datas (Últimos 10 anos)
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)
    hoje = datetime.today()
    data_10_anos = (hoje - timedelta(days=365*10)).strftime('%Y-%m-%d')
    
    # 1. Carrega o Universo da B3
    tickers_b3 = obter_todos_tickers_b3()
    
    # 2. Executa os 4 Níveis de Extração
    
    # Nível 1: Preços OHLCV + Adj Close
    df_precos = extrair_precos_maciamente(tickers_b3, data_inicio=data_10_anos)
    if not df_precos.empty:
        df_precos.to_csv(PASTA_DESTINO / "01_yfinance_precos_raw.csv", sep=';', decimal=',', index=False, encoding='utf-8-sig')

    # Nível 2: Dividendos e Splits
    df_eventos = extrair_eventos_corporativos(tickers_b3)
    if not df_eventos.empty:
        df_eventos.to_csv(PASTA_DESTINO / "02_yfinance_eventos_raw.csv", sep=';', decimal=',', index=False, encoding='utf-8-sig')

    # Nível 3: Metadados (+100 colunas)
    df_info = extrair_info_avancada(tickers_b3)
    if not df_info.empty:
        df_info.to_csv(PASTA_DESTINO / "03_yfinance_info_raw.csv", sep=';', decimal=',', index=False, encoding='utf-8-sig')
        
    # Nível 4: DRE Padronizada
    df_balancos = extrair_balancos_anuais(tickers_b3)
    if not df_balancos.empty:
        df_balancos.to_csv(PASTA_DESTINO / "04_yfinance_balancos_raw.csv", sep=';', decimal=',', index=False, encoding='utf-8-sig')

    print(f"\n🏁 PIPELINE FINALIZADO COM SUCESSO DE NÍVEL DATA LAKE.")
    print(f"📁 Os 4 Mega-Datasets com todo o histórico e fundamentos da B3 estão em:\n{PASTA_DESTINO}")

if __name__ == "__main__":
    run_market_data_pipeline()
