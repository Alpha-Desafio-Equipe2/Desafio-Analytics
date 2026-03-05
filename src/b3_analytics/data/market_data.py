import yfinance as yf
import pandas as pd
from pathlib import Path
import os
import time
from datetime import datetime, timedelta

# ==========================================
# CONFIGURAÇÕES DO ESTUDO DA B3
# ==========================================
TICKERS_B3 = [
    "PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA",
    "WEGE3.SA", "ABEV3.SA", "RENT3.SA", "ELET3.SA", "B3SA3.SA"
]
PASTA_DESTINO = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"

# ==========================================
# FUNÇÕES DE EXTRAÇÃO CORAÇÃO DO MARKET DATA
# ==========================================
def extrair_precos_maciamente(tickers: list, data_inicio: str = '2016-01-01') -> pd.DataFrame:
    """Extrai os preços históricos de múltiplos Tickers via Yahoo Finance API usando processamento paralelo."""
    print(f"📉 A iniciar o download massivo de {len(tickers)} ativos da B3 (A partir de {data_inicio})...")
    print("⏳ Aviso: Isto pode demorar alguns minutos. O download é paralelo (Multithreading).")

    try:
        # Download massivo com multithreading
        dados_b3_full = yf.download(tickers, start=data_inicio, threads=True, ignore_tz=True)
        
        # Transformar a matriz tridimensional num DataFrame plano tradicional
        df_b3_bruto = dados_b3_full.stack(level=1).reset_index()

        # Renomear colunas para o padrão do projeto
        df_b3_bruto.rename(columns={
            'level_1': 'Ticker', 
            'Date': 'Data_Merge',
            'Open': 'Preco_Abertura',
            'High': 'Preco_Maximo',
            'Low': 'Preco_Minimo',
            'Close': 'Preco_Fechamento',
            'Volume': 'Volume_Negociado'
        }, inplace=True)

        # Formatação de data extra
        df_b3_bruto['Data_Formatada'] = df_b3_bruto['Data_Merge'].dt.strftime('%d/%m/%Y')
        
        # Mantendo TODAS as colunas originadas do YFinance (Dataset sujo/completo) para limpeza posterior
        print(f"✅ MEGA DATASET DE PREÇOS CRIADO! Foram guardadas {len(df_b3_bruto)} linhas históricas.")
        return df_b3_bruto
        
    except Exception as e:
        print(f"❌ Erro crítico no download massivo: {e}")
        return pd.DataFrame()


def extrair_dividendos(tickers: list) -> pd.DataFrame:
    """Extrai o histórico de pagamento de dividendos de múltiplos Tickers via YFinance Object."""
    print(f"\n🔄 Iniciando extração de DIVIDENDOS para {len(tickers)} ativos...")
    
    df_dividendos = pd.DataFrame()
    
    for ticker in tickers:
        print(f"  -> Coletando dividendos de: {ticker}")
        try:
            acao = yf.Ticker(ticker)
            divs = acao.dividends
            
            if not divs.empty:
                # Transforma a série em DataFrame
                df_temp = divs.reset_index()
                df_temp.columns = ['Date', 'Dividends']
                df_temp['Ticker'] = ticker.replace('.SA', '')
                
                df_dividendos = pd.concat([df_dividendos, df_temp], ignore_index=True)
        except Exception as e:
            print(f"  ❌ Erro ao coletar dividendos de {ticker}: {e}")
            
    return df_dividendos


# ==========================================
# PIPELINE DE SALVAMENTO
# ==========================================
def run_market_data_pipeline():
    """Executa a pipeline completa: Extrair -> Padronizar -> Salvar"""
    print("🚀 PIPELINE DE DADOS DE MERCADO B3 - INICIADO\n")
    
    # 1. Garantir que a pasta raw existe
    PASTA_DESTINO.mkdir(parents=True, exist_ok=True)
    
    # 2. Coletar Preços (Exatos 10 anos atrás)
    hoje = datetime.today()
    data_10_anos = (hoje - timedelta(days=365*10)).strftime('%Y-%m-%d')
    df_precos = extrair_precos_maciamente(TICKERS_B3, data_inicio=data_10_anos)
    
    if not df_precos.empty:
        caminho_precos = PASTA_DESTINO / "precos_b3_mestre.csv"
        # Mantendo o formato decimal vírgula imposto pelo snippet se for padrão no projeto
        df_precos.to_csv(caminho_precos, sep=';', decimal=',', index=False, encoding='utf-8-sig')
        print(f"📁 Arquivo de PREÇOS salvo em: {caminho_precos}")

    # 3. Coletar Dividendos
    df_divs = extrair_dividendos(TICKERS_B3)
    
    if not df_divs.empty:
         # Opcional: Remover Timezone (UTC) das datas
        if 'Date' in df_divs.columns and pd.api.types.is_datetime64_any_dtype(df_divs['Date']):
            df_divs['Date'] = df_divs['Date'].dt.tz_localize(None)
            
        caminho_divs = PASTA_DESTINO / "dividendos_b3_mestre.csv"
        df_divs.to_csv(caminho_divs, index=False)
        print(f"✅ Arquivo de DIVIDENDOS salvo em: {caminho_divs} ({len(df_divs)} linhas)")
        
    print("\n🏁 PIPELINE FINALIZADO COM SUCESSO.")

if __name__ == "__main__":
    run_market_data_pipeline()
