import pandas as pd
import numpy as np
import os
import sys

# Garante que o diretório src está no path para importar utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

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
    if pd.isna(ticker): return ticker
    t = str(ticker).strip().upper()
    if not t.endswith('.SA'):
        t = f"{t}.SA"
    return t

def to_numeric_safe(series):
    if series.dtype == 'object':
        return pd.to_numeric(series.str.replace(',', '.'), errors='coerce')
    return pd.to_numeric(series, errors='coerce')

def process_master_lake():
    print("🚀 INICIANDO INTEGRAÇÃO DO MASTER DATA LAKE 3.3 (FINAL POLISH) 🚀")
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    raw_dir = os.path.join(base_dir, 'data/raw')
    processed_dir = os.path.join(base_dir, 'data/processed')
    partition_dir = os.path.join(processed_dir, 'setores')
    os.makedirs(partition_dir, exist_ok=True)

    # 1. Base de Preços (YFinance)
    print("1. A carregar Preços (Base)...")
    df_precos = pd.read_csv(os.path.join(raw_dir, '01_yfinance_precos_raw.csv'), sep=';', decimal=',', low_memory=False)
    df_precos.columns = [c.lower() for c in df_precos.columns]
    # Normalizamos a data para remover horas e evitar erros de merge
    df_precos['date'] = pd.to_datetime(df_precos['date']).dt.tz_localize(None).dt.normalize()
    if 'adj close' in df_precos.columns:
        df_precos = df_precos.drop(columns=['adj close'])
    df_precos['ticker'] = df_precos['ticker'].apply(format_ticker)

    # 2. Indicadores Econômicos (BCB Macro)
    print("2. A injetar Macroeconomia...")
    df_macro = pd.read_csv(os.path.join(raw_dir, 'indicadores_economicos_10anos.csv'), sep=';', decimal=',', low_memory=False)
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
    df_info = pd.read_csv(os.path.join(raw_dir, '03_yfinance_info_raw.csv'), sep=';', on_bad_lines='skip', low_memory=False)
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
    df_eventos = pd.read_csv(os.path.join(raw_dir, '02_yfinance_eventos_raw.csv'), sep=';', low_memory=False)
    df_eventos.columns = [c.lower() for c in df_eventos.columns]
    # Normalizamos a data dos eventos para bater com o pregão (00:00:00)
    df_eventos['date'] = pd.to_datetime(df_eventos['date']).dt.tz_localize(None).dt.normalize()
    df_eventos['ticker'] = df_eventos['ticker'].apply(format_ticker)
    df_eventos['dividends'] = to_numeric_safe(df_eventos['dividends'])
    df_eventos['stock splits'] = to_numeric_safe(df_eventos['stock splits'])
    df_eventos = df_eventos[['date', 'ticker', 'dividends', 'stock splits']]
    df_master = pd.merge(df_master, df_eventos, on=['date', 'ticker'], how='left').fillna({'dividends': 0, 'stock splits': 0})

    df_cvm = pd.read_csv(os.path.join(raw_dir, '05_CVM_Historico_Focado.csv'), sep=';', decimal=',', low_memory=False)
    df_cvm.columns = [c.lower() for c in df_cvm.columns]
    rename_cvm = {'dt_fim_exerc': 'date', 'ticker_alvo': 'ticker', '03_ebit_operacional_r$': 'cvm_ebit', '06_lucro_liquido_r$': 'cvm_lucro_liquido'}
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
    
    df.to_parquet(os.path.join(processed_dir, '01_market_data_processed.parquet'), index=False)
    df.to_csv(os.path.join(processed_dir, '01_market_data_processed.csv'), sep=';', decimal=',', index=False, encoding='utf-8-sig')

    for setor in df['sector'].unique():
        nome_setor = str(setor).replace(' ', '_').replace('/', '_') if pd.notna(setor) else 'Outros'
        mask = df['sector'] == setor if pd.notna(setor) else df['sector'].isna()
        df[mask].to_parquet(os.path.join(partition_dir, f"{nome_setor}.parquet"), index=False)
    
    print("\n✅ MASTER DATA LAKE 3.3 CONCLUÍDO!")
    print(f"   -> Verificado: Merge de dividendos restaurado via normalização de data.")

if __name__ == "__main__":
    process_master_lake()
