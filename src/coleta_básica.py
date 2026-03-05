import yfinance as yf
import pandas as pd
from pathlib import Path

# ===============================
# Configurações
# ===============================
ATIVO = "PETR4.SA"  # .SA para ações da B3
PERIODO = "5y"
INTERVALO = "1d"

# ===============================
# Coleta de dados
# ===============================
def coletar_dados(ativo, periodo, intervalo):
    print(f"Baixando dados de {ativo}...")
    
    dados = yf.download(
        ativo,
        period=periodo,
        interval=intervalo,
        auto_adjust=False
    )
    
    if dados.empty:
        raise ValueError("Nenhum dado foi retornado.")
    
    return dados


# ===============================
# Salvar CSV
# ===============================
def salvar_csv(dados, ativo):
    pasta_raw = Path("../data/raw")
    pasta_raw.mkdir(parents=True, exist_ok=True)
    
    caminho_arquivo = pasta_raw / f"{ativo.replace('.SA', '').lower()}.csv"
    dados.to_csv(caminho_arquivo)
    
    print(f"Arquivo salvo em: {caminho_arquivo}")


# ===============================
# Execução
# ===============================
if __name__ == "__main__":
    df = coletar_dados(ATIVO, PERIODO, INTERVALO)
    
    print("\nPrimeiras linhas:")
    print(df.head())
    
    print("\nInformações do dataset:")
    print(df.info())
    
    salvar_csv(df, ATIVO)