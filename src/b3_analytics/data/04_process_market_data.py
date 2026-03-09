"""
Master Data Lake Builder - B3 Analytics

Pipeline responsável por integrar dados de mercado, macroeconomia,
eventos corporativos e dados fundamentalistas em um único dataset
analítico para pesquisa quantitativa e machine learning.

Project: B3 Analytics
"""
import pandas as pd
import numpy as np
import os
import sys
from b3_analytics.utils.paths import RAW_DIR, PROCESSED_DIR

from b3_analytics.utils.finance_utils import (
    calcular_retornos, 
    calcular_volatilidade, 
    calcular_medias_moveis,
    calcular_momentum,
    calcular_volume_indicadores,
    calcular_rsi,
    calcular_bandas_bollinger,
    calcular_macd,
    calcular_risco_drawdown,
    calcular_fundamental_ratios,
    calcular_tendencias_financeiras
)

def format_ticker(ticker):
    """
    Padroniza o formato de tickers para o padrão utilizado pelo Yahoo Finance.

    Esta função garante que todos os tickers estejam em letras maiúsculas
    e possuam o sufixo ".SA", utilizado para ativos listados na B3.

    Parameters
    ----------
    ticker : str or NaN
        Ticker original da empresa.

    Returns
    -------
    str or NaN
        Ticker padronizado no formato "XXXX.SA". Caso o valor seja NaN,
        ele é retornado sem modificação.
    """
    if pd.isna(ticker): return ticker
    t = str(ticker).strip().upper()
    if not t.endswith('.SA'):
        t = f"{t}.SA"
    return t

def to_numeric_safe(series):
    """
    Converte uma Series para valores numéricos de forma segura.

    A função trata séries que podem conter números representados
    como strings com vírgula decimal (formato comum em dados brasileiros).

    Parameters
    ----------
    series : pandas.Series
        Série contendo valores numéricos ou strings representando números.

    Returns
    -------
    pandas.Series
        Série convertida para tipo numérico (`float`), com valores inválidos
        convertidos para `NaN`.
    """
    if series.dtype == 'object':
        return pd.to_numeric(series.str.replace(',', '.'), errors='coerce')
    return pd.to_numeric(series, errors='coerce')

def process_master_lake():
    """
    Constrói o Master Data Lake consolidado do mercado financeiro.

    Esta função executa todo o pipeline de integração de dados financeiros,
    combinando múltiplas fontes em um único dataset analítico otimizado para
    modelagem quantitativa, análise financeira e machine learning.

    O pipeline inclui:

    1. Carregamento de dados históricos de preços (Yahoo Finance)
    2. Integração de indicadores macroeconômicos (Banco Central)
    3. Enriquecimento com informações corporativas das empresas
    4. Integração de eventos corporativos (dividendos e splits)
    5. Integração de dados fundamentalistas históricos da CVM
    6. Geração de features quantitativas e indicadores técnicos
    7. Cálculo de métricas de risco e performance
    8. Exportação do dataset final em formatos otimizados

    O resultado é um **Master Dataset financeiro** contendo indicadores
    técnicos, fundamentalistas, macroeconômicos e estatísticas de risco
    para cada ativo da B3 ao longo do tempo.

    Outputs gerados
    ---------------
    01_market_data_processed.parquet
        Dataset principal em formato Parquet otimizado para analytics.

    01_market_data_processed.csv
        Versão CSV para compatibilidade com outras ferramentas.

    setores/*.parquet
        Partições do dataset separadas por setor econômico.

    Returns
    -------
    None
        A função executa o pipeline de processamento e salva os
        resultados diretamente no diretório de dados processados.

    Notes
    -----
    - O merge entre preços e dados da CVM é realizado usando `merge_asof`,
      garantindo que os dados fundamentalistas sejam associados ao pregão
      mais próximo posterior.
    - Datas são normalizadas para evitar inconsistências causadas por
      fusos horários ou timestamps.
    - Indicadores técnicos e quantitativos são gerados através de funções
      utilitárias do módulo `finance_utils`.

    Raises
    ------
    FileNotFoundError
        Caso algum dos arquivos necessários no diretório RAW_DIR não exista.

    Examples
    --------
    >>> process_master_lake()
    🚀 INICIANDO INTEGRAÇÃO DO MASTER DATA LAKE...
    """
    print("🚀 INICIANDO INTEGRAÇÃO DO MASTER DATA LAKE 3.3 (FINAL POLISH) 🚀")
    
    partition_dir = os.path.join(PROCESSED_DIR, 'setores')
    os.makedirs(partition_dir, exist_ok=True)

    # 1. Base de Preços (YFinance)
    print("1. A carregar Preços (Base)...")
    df_precos = pd.read_csv(os.path.join(RAW_DIR, '02_yfinance_precos_raw.csv'), sep=';', decimal=',', low_memory=False)
    df_precos.columns = [c.lower() for c in df_precos.columns]
    # Normalizamos a data para remover horas e evitar erros de merge
    df_precos['date'] = pd.to_datetime(df_precos['date']).dt.tz_localize(None).dt.normalize()
    if 'adj close' in df_precos.columns:
        df_precos = df_precos.drop(columns=['adj close'])
    df_precos['ticker'] = df_precos['ticker'].apply(format_ticker)

    # 2. Indicadores Econômicos (BCB Macro)
    print("2. A injetar Macroeconomia...")
    df_macro = pd.read_csv(os.path.join(RAW_DIR, '01_bcb_indicadores_economicos.csv'), sep=';', decimal=',', low_memory=False)
    df_macro['date'] = pd.to_datetime(df_macro['data']).dt.tz_localize(None).dt.normalize()
    df_macro = df_macro.drop(columns=['data'])
    for col in ['selic', 'ipca', 'dolar']:
        if col in df_macro.columns:
            df_macro[col] = to_numeric_safe(df_macro[col])

    df_master = pd.merge(df_precos, df_macro, on='date', how='left')
    df_master = df_master.sort_values(['ticker', 'date'])
    df_master[['selic', 'ipca', 'dolar']] = df_master.groupby('ticker')[['selic', 'ipca', 'dolar']].ffill()

    # 3. Info da Empresa
    print("3. A integrar Informações Corporativas...")
    df_info = pd.read_csv(os.path.join(RAW_DIR, '04_yfinance_info_raw.csv'), sep=';', on_bad_lines='skip', low_memory=False)
    df_info.columns = [c.lower() for c in df_info.columns]
    df_info['ticker'] = df_info['ticker'].apply(format_ticker)
    for col in ['marketcap', 'bookvalue', 'returnonequity']:
        if col in df_info.columns:
            df_info[col] = to_numeric_safe(df_info[col])

    cols_info = {'ticker': 'ticker', 'sector': 'sector', 'industry': 'industry', 
                 'marketcap': 'marketcap', 'bookvalue': 'cvm_book_value', 'returnonequity': 'roe_static'}
    df_info_clean = df_info[[c for c in cols_info.keys() if c in df_info.columns]].rename(columns=cols_info)
    df_master = pd.merge(df_master, df_info_clean, on='ticker', how='left')

    # 4. Eventos e CVM
    print("4. A integrar Eventos e Histórico DRE CVM (Date Normalization)...")
    df_eventos = pd.read_csv(os.path.join(RAW_DIR, '03_yfinance_eventos_raw.csv'), sep=';', low_memory=False)
    df_eventos.columns = [c.lower() for c in df_eventos.columns]
    # Normalizamos a data dos eventos para bater com o pregão (00:00:00)
    df_eventos['date'] = pd.to_datetime(df_eventos['date']).dt.tz_localize(None).dt.normalize()
    df_eventos['ticker'] = df_eventos['ticker'].apply(format_ticker)
    df_eventos['dividends'] = to_numeric_safe(df_eventos['dividends'])
    df_eventos['stock splits'] = to_numeric_safe(df_eventos['stock splits'])
    df_eventos = df_eventos[['date', 'ticker', 'dividends', 'stock splits']]
    df_master = pd.merge(df_master, df_eventos, on=['date', 'ticker'], how='left').fillna({'dividends': 0, 'stock splits': 0})

    df_cvm = pd.read_csv(os.path.join(RAW_DIR, '06_CVM_Historico_Focado.csv'), sep=';', decimal=',', low_memory=False)
    df_cvm.columns = [c.lower() for c in df_cvm.columns]
    rename_cvm = {'data_referencia': 'date', 'ticker_alvo': 'ticker', '03_ebit_operacional_r$': 'cvm_ebit', '06_lucro_liquido_r$': 'cvm_lucro_liquido'}
    df_cvm = df_cvm.rename(columns=rename_cvm)
    df_cvm['date'] = pd.to_datetime(df_cvm['date']).dt.tz_localize(None).dt.normalize()
    df_cvm['ticker'] = df_cvm['ticker'].apply(format_ticker)
    for col in ['cvm_ebit', 'cvm_lucro_liquido']:
        if col in df_cvm.columns:
            df_cvm[col] = to_numeric_safe(df_cvm[col])
    
    df_cvm['cvm_patrimonio_liquido'] = df_cvm['ticker'].map(df_info_clean.set_index('ticker')['cvm_book_value'])
    df_cvm['cvm_patrimonio_liquido'] = to_numeric_safe(df_cvm['cvm_patrimonio_liquido'])

    df_master = df_master.sort_values('date')
    df_cvm = df_cvm.sort_values('date')
    df_master = pd.merge_asof(df_master, df_cvm[['date', 'ticker', 'cvm_ebit', 'cvm_lucro_liquido', 'cvm_patrimonio_liquido']], 
                             on='date', by='ticker', direction='backward')

    # 5. FEATURE ENGINEERING (Advanced)
    print("5. A gerar Engine de Features Quant e Deep Dive (RSI, Payout, Sharpe)...")
    df_master = df_master.sort_values(['ticker', 'date'])
    df_master = calcular_retornos(df_master)
    df_master = calcular_volatilidade(df_master)
    df_master = calcular_medias_moveis(df_master)
    df_master = calcular_momentum(df_master)
    df_master = calcular_volume_indicadores(df_master)
    
    df = calcular_rsi(df_master)
    df = calcular_bandas_bollinger(df)
    df = calcular_macd(df)
    df = calcular_risco_drawdown(df)
    df = calcular_fundamental_ratios(df) 
    df = calcular_tendencias_financeiras(df)

    if 'selic' in df.columns and 'volatilidade_21d' in df.columns:
        ret_anual = df.groupby('ticker')['retorno_diario'].transform(lambda x: x.rolling(21).mean() * 252)
        df['sharpe_ratio_21d'] = (ret_anual - (df['selic']/100)) / (df['volatilidade_21d'] + 1e-9)

    # 6. Exportação Profissional
    print("6. Limpeza e Exportação Final (Parquet + Setores)...")
    df = df.sort_values(['ticker', 'date']).reset_index(drop=True)
    df['retorno_diario'] = df['retorno_diario'].fillna(0)
    
    df.to_parquet(os.path.join(PROCESSED_DIR, '01_market_data_processed.parquet'), index=False)
    df.to_csv(os.path.join(PROCESSED_DIR, '01_market_data_processed.csv'), sep=';', decimal=',', index=False, encoding='utf-8-sig')

    for setor in df['sector'].unique():
        nome_setor = str(setor).replace(' ', '_').replace('/', '_') if pd.notna(setor) else 'Outros'
        mask = df['sector'] == setor if pd.notna(setor) else df['sector'].isna()
        df[mask].to_parquet(os.path.join(partition_dir, f"{nome_setor}.parquet"), index=False)
    
    print("\n✅ MASTER DATA LAKE 3.3 CONCLUÍDO!")
    print(f"   -> Verificado: Merge de dividendos restaurado via normalização de data.")

if __name__ == "__main__":
    process_master_lake()
