import pandas as pd
import os
import sys

# Garante que o diretório src está no path para importar utils (Se rodar via terminal local)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from b3_analytics.utils.finance_utils import (
    calcular_retornos, 
    calcular_volatilidade, 
    calcular_medias_moveis,
    calcular_momentum,
    calcular_volume_indicadores
)

def run_processing_pipeline():
    """
    Aplica engenharia de features institucionais baseada nas 10 Regras de Ouro Quants.
    """
    print("⚙️ INICIANDO ENGENHARIA DE DADOS QUANTITATIVA (ISSUE #5) ⚙️")
    
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    caminho_entrada = os.path.join(base_dir, 'data/raw/01_yfinance_precos_raw.csv')
    pasta_saida = os.path.join(base_dir, 'data/processed')
    caminho_saida = os.path.join(pasta_saida, '01_market_data_processed.csv')
    
    os.makedirs(pasta_saida, exist_ok=True)
    
    if not os.path.exists(caminho_entrada):
        print(f"❌ Erro: Arquivo de entrada não encontrado em {caminho_entrada}.")
        return

    print("1. A ler e higienizar dados brutos...")
    df = pd.read_csv(caminho_entrada, sep=';', decimal=',')
    df.columns = [str(c).lower().strip() for c in df.columns]
    
    # Padronização e Limpeza: Tipagem e Formato de Datas
    df['date'] = pd.to_datetime(df['date'])
    
    # Eliminar Duplicatas de reentrada da B3
    df = df.drop_duplicates(subset=['ticker', 'date'], keep='last')
    
    df = df.sort_values(by=['ticker', 'date']).reset_index(drop=True)
    
    print("2. A tratar Missing Values (Forward Fill)...")
    # Forward Fill (O último preço negociado é a verdade atual)
    colunas_preco_vol = ['close', 'high', 'low', 'open', 'volume']
    colunas_existentes = [c for c in colunas_preco_vol if c in df.columns]
    df[colunas_existentes] = df.groupby('ticker')[colunas_existentes].ffill()
    
    print("3. A gerar Features Preditivas (Retornos e Volatilidade)...")
    df = calcular_retornos(df, coluna_preco='close')
    df = calcular_volatilidade(df, coluna_retorno='log_return')
    
    print("4. A calcular Indicadores de Tendência e Momentum...")
    df = calcular_medias_moveis(df, coluna_preco='close')
    df = calcular_momentum(df, coluna_preco='close')
    
    if 'volume' in df.columns:
        print("5. A mapear Fluxo Institucional (OBV)...")
        df = calcular_volume_indicadores(df, coluna_preco='close', coluna_vol='volume')
    
    # Preenchimento de arranques iniciais nulos
    df['retorno_diario'] = df['retorno_diario'].fillna(0)
    df['log_return'] = df['log_return'].fillna(0)
    
    print("6. A salvar Super DataFrame Master...")
    df.to_csv(caminho_saida, sep=';', decimal=',', index=False, encoding='utf-8-sig')
    
    print("\n✅ PROCESSAMENTO QUANTITATIVO CONCLUÍDO!")
    print(f"   -> Arquivo pronto para Machine Learning: {caminho_saida}")
    print(f"   -> Variáveis criadas: Log Return, Volatilidade (1M, 3M, 1A), OBV, Spreads, Momentum.")
    
if __name__ == "__main__":
    run_processing_pipeline()
