import yfinance as yf
import pandas as pd
import fundamentus
from pathlib import Path
from datetime import datetime, timedelta
import warnings
import logging

# =====================================================================
# CONFIGURAÇÕES E BLINDAGEM DE ERROS
# =====================================================================
warnings.filterwarnings('ignore')
# Silencia erros do YFinance para não inundar o terminal quando uma ação não existir
logging.getLogger('yfinance').setLevel(logging.CRITICAL)

# Pasta onde os Mega-Datasets serão guardados
PASTA_DESTINO = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw"

# =====================================================================
# 0. DESCOBERTA DE TODOS OS TICKERS DA B3
# =====================================================================
def obter_todos_tickers_b3() -> list:
    """Usa o Fundamentus para descobrir todas as ações ativas na Bolsa hoje."""
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
        # Retorna uma lista de salvaguarda caso a internet falhe
        return ["PETR4.SA", "VALE3.SA", "ITUB4.SA", "BBDC4.SA", "BBAS3.SA", "WEGE3.SA"]

# =====================================================================
# 1. EXTRAÇÃO DE PREÇOS (O MÁXIMO DE COLUNAS)
# =====================================================================
def extrair_precos_maciamente(tickers: list, data_inicio: str) -> pd.DataFrame:
    """Extrai preços históricos via Yahoo Finance API usando processamento paralelo."""
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
    """Extrai Dividendos E Desdobramentos (Splits) que afetam o preço da ação."""
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
        except:
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
    """Extrai +100 colunas de informações cadastrais, de risco (Beta) e indicadores."""
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
        except:
            pass
            
    df_info = pd.DataFrame(lista_infos)
    num_colunas = len(df_info.columns) if not df_info.empty else 0
    print(f"   ✅ Metadados extraídos! Matriz com {num_colunas} colunas brutas capturada para {len(df_info)} ações.")
    return df_info

# =====================================================================
# 4. EXTRAÇÃO DE BALANÇOS (DRE / INCOME STATEMENT)
# =====================================================================
def extrair_balancos_anuais(tickers: list) -> pd.DataFrame:
    """Extrai a DRE (Demonstração do Resultado) anual padronizada pelo Yahoo Finance."""
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
        except:
            pass
            
    print(f"   ✅ Balanços extraídos! {len(df_balancos)} registos financeiros anuais globais capturados.")
    return df_balancos

# =====================================================================
# MOTOR DE EXECUÇÃO PRINCIPAL
# =====================================================================
def run_market_data_pipeline():
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
