import pandas as pd
import os
import sys

# Garante que o diretório src está no path para importar utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from b3_analytics.utils.finance_utils import calcular_retornos, calcular_volatilidade, calcular_medias_moveis

def run_processing_pipeline():
    """
    Lê os dados brutos (Raw), aplica os cálculos matemáticos financeiros
    e salva os DataFrames analíticos prontos para a IA e Dashboard.
    """
    print("⚙️ INICIANDO PIPELINE DE PROCESSAMENTO (ISSUE #5) ⚙️")
    
    caminho_entrada = 'data/raw/01_yfinance_precos_raw.csv'
    pasta_saida = 'data/processed'
    caminho_saida = os.path.join(pasta_saida, '01_market_data_processed.csv')
    
    os.makedirs(pasta_saida, exist_ok=True)
    
    if not os.path.exists(caminho_entrada):
        print(f"❌ Erro: Arquivo de entrada não encontrado em {caminho_entrada}.")
        return

    print("1. Lendo os dados brutos (suportando separador ';' e decimal ',')...")
    # Colunas no CSV: Date;Ticker;Adj Close;Close;High;Low;Open;Volume
    df = pd.read_csv(caminho_entrada, sep=';', decimal=',')
    
    # Padronização de nomes de colunas (lowercase)
    df.columns = [c.lower() for c in df.columns]
    
    # Garantir que a data é datetime para ordenação cronológica
    df['date'] = pd.to_datetime(df['date'])
    
    # Ordenação obrigatória: A matemática não funciona se os dias estiverem embaralhados
    df = df.sort_values(by=['ticker', 'date']).reset_index(drop=True)
    
    print("2. Calculando Retorno Diário e Acumulado...")
    df = calcular_retornos(df, coluna_preco='close')
    
    print("3. Calculando Volatilidade Anualizada (21 dias)...")
    df = calcular_volatilidade(df, coluna_retorno='retorno_diario', janela=21)
    
    print("4. Calculando Médias Móveis...")
    df = calcular_medias_moveis(df, coluna_preco='close')
    
    # Preenchimento de nulos após o pct_change
    df['retorno_diario'] = df['retorno_diario'].fillna(0)
    
    print("5. Salvando DataFrame Analítico...")
    df.to_csv(caminho_saida, sep=';', decimal=',', index=False, encoding='utf-8-sig')
    
    print("\n✅ PROCESSAMENTO CONCLUÍDO COM SUCESSO!")
    print(f"   -> Arquivo salvo em: {caminho_saida}")
    print(f"   -> Linhas processadas: {len(df)}")
    print(f"   -> Colunas disponíveis: {list(df.columns)}")

if __name__ == "__main__":
    run_processing_pipeline()
