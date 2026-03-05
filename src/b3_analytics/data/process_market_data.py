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
    calcular_volume_indicadores
)

def format_ticker(ticker):
    if pd.isna(ticker): return ticker
    t = str(ticker).strip().upper()
    if not t.endswith('.SA'):
        t = f"{t}.SA"
    return t

def process_master_lake():
    print("🚀 INICIANDO INTEGRAÇÃO DO MASTER DATA LAKE (6 FONTES) 🚀")
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    raw_dir = os.path.join(base_dir, 'data/raw')
    processed_dir = os.path.join(base_dir, 'data/processed')
    os.makedirs(processed_dir, exist_ok=True)

    # 1. Base de Preços (YFinance)
    print("📦 1/6 A carregar Preços (Base)...")
    df_precos = pd.read_csv(os.path.join(raw_dir, '01_yfinance_precos_raw.csv'), sep=';', decimal=',', low_memory=False)
    df_precos.columns = [c.lower() for c in df_precos.columns]
    df_precos['date'] = pd.to_datetime(df_precos['date']).dt.tz_localize(None)
    if 'adj close' in df_precos.columns:
        df_precos = df_precos.drop(columns=['adj close'])
    df_precos['ticker'] = df_precos['ticker'].apply(format_ticker)

    # 2. Indicadores Econômicos (Macro)
    print("📊 2/6 A integrar Indicadores Macro (Selic, IPCA, Dolar)...")
    df_macro = pd.read_csv(os.path.join(raw_dir, 'indicadores_economicos_10anos.csv'), sep=';', decimal=',', low_memory=False)
    df_macro['date'] = pd.to_datetime(df_macro['data']).dt.tz_localize(None)
    df_macro = df_macro.drop(columns=['data'])
    # Merge com precos
    df_master = pd.merge(df_precos, df_macro, on='date', how='left')
    # Ffill por ativo para não misturar dados macro em dias sem pregão de um ativo específico
    df_master = df_master.sort_values(['ticker', 'date'])
    df_master[['selic', 'ipca', 'dolar']] = df_master.groupby('ticker')[['selic', 'ipca', 'dolar']].ffill()

    # 3. Eventos Corporativos (Dividendos e Splits)
    print("💎 3/6 A integrar Eventos (Dividendos/Splits)...")
    df_eventos = pd.read_csv(os.path.join(raw_dir, '02_yfinance_eventos_raw.csv'), sep=';', decimal=',', low_memory=False)
    df_eventos.columns = [c.lower() for c in df_eventos.columns]
    df_eventos['date'] = pd.to_datetime(df_eventos['date']).dt.tz_localize(None)
    df_eventos['ticker'] = df_eventos['ticker'].apply(format_ticker)
    df_eventos = df_eventos[['date', 'ticker', 'dividends', 'stock splits']]
    df_master = pd.merge(df_master, df_eventos, on=['date', 'ticker'], how='left').fillna({'dividends': 0, 'stock splits': 0})

    # 4. Info da Empresa (Setor e Indústria)
    print("🏢 4/6 A integrar Informações Corporativas (Setores)...")
    df_info = pd.read_csv(os.path.join(raw_dir, '03_yfinance_info_raw.csv'), sep=';', on_bad_lines='skip', low_memory=False)
    df_info.columns = [c.lower() for c in df_info.columns]
    df_info['ticker'] = df_info['ticker'].apply(format_ticker)
    cols_info = ['ticker', 'sector', 'industry', 'marketcap', 'fulltimeemployees']
    df_info = df_info[[c for c in cols_info if c in df_info.columns]]
    df_master = pd.merge(df_master, df_info, on='ticker', how='left')

    # 5. Balanços e CVM
    print("📋 5/6 A integrar Balanços e Dados CVM Históricos...")
    df_cvm = pd.read_csv(os.path.join(raw_dir, '05_CVM_Historico_Focado.csv'), sep=';', decimal=',', low_memory=False)
    df_cvm.columns = [c.lower() for c in df_cvm.columns]
    
    rename_cvm = {
        'dt_fim_exerc': 'date',
        'ticker_alvo': 'ticker',
        '01_receita_liquida_r$': 'cvm_receita_liquida',
        '03_ebit_operacional_r$': 'cvm_ebit',
        '06_lucro_liquido_r$': 'cvm_lucro_liquido'
    }
    df_cvm = df_cvm.rename(columns=rename_cvm)
    df_cvm['date'] = pd.to_datetime(df_cvm['date']).dt.tz_localize(None)
    df_cvm['ticker'] = df_cvm['ticker'].apply(format_ticker)
    
    cols_to_keep = ['ticker', 'date', 'cvm_receita_liquida', 'cvm_ebit', 'cvm_lucro_liquido']
    df_cvm = df_cvm[[c for c in cols_to_keep if c in df_cvm.columns]]

    # IMPORTANTE: merge_asof exige que a coluna 'on' (date) esteja ordenada
    df_master = df_master.sort_values('date')
    df_cvm = df_cvm.sort_values('date')
    
    df_master = pd.merge_asof(
        df_master, df_cvm, 
        on='date', by='ticker', 
        direction='backward'
    )

    # 6. Calcular Features Quantitativas
    print("🧪 6/6 A gerar Engine de Features Quant (Regras de Ouro)...")
    # Voltamos a ordenar por ticker/date para as janelas móveis das utils
    df_master = df_master.sort_values(['ticker', 'date'])
    df_master = calcular_retornos(df_master)
    df_master = calcular_volatilidade(df_master)
    df_master = calcular_medias_moveis(df_master)
    df_master = calcular_momentum(df_master)
    df_master = calcular_volume_indicadores(df_master)

    # Limpeza Final e Exportação
    print("🧹 Limpeza e Exportação Final...")
    df_master = df_master.sort_values(['ticker', 'date']).reset_index(drop=True)
    
    output_path = os.path.join(processed_dir, '01_market_data_processed.csv')
    df_master.to_csv(output_path, sep=';', decimal=',', index=False, encoding='utf-8-sig')
    
    print("\n✅ MASTER DATA LAKE CONSTRUÍDO COM SUCESSO!")
    print(f"   -> Caminho: {output_path}")
    print(f"   -> Colunas Totais: {len(df_master.columns)}")
    print(f"   -> Registros: {len(df_master)}")

if __name__ == "__main__":
    process_master_lake()
