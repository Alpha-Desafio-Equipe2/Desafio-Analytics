"""
coleta_macro.py

Pipeline de coleta de dados macroeconômicos do Banco Central do Brasil
utilizando a API do python-bcb.

Atualmente coleta:
- SELIC
- IPCA

Os dados são retornados em formato DataFrame e podem ser utilizados
nas etapas de análise e modelagem do projeto.
"""

from datetime import date
from bcb import sgs
import pandas as pd
from b3_analytics.utils.paths import RAW_DIR

def obter_periodo_analise(anos: int = 10) -> tuple[date, date]:
    """
    Define o período de análise.

    Parameters
    ----------
    anos : int
        Quantidade de anos no passado a serem coletados.

    Returns
    -------
    tuple[date, date]
        Data inicial e final do período.
    """

    data_fim = date.today()
    data_inicio = date(data_fim.year - anos, data_fim.month, data_fim.day)

    return data_inicio, data_fim


def coletar_dados_macro(data_inicio: date, data_fim: date) -> pd.DataFrame:
    """
    Coleta indicadores macroeconômicos do Banco Central.

    Parameters
    ----------
    data_inicio : date
        Data inicial da coleta.
    data_fim : date
        Data final da coleta.

    Returns
    -------
    pd.DataFrame
        DataFrame contendo os indicadores macroeconômicos.
    """

    indicadores = {
        "selic": 432,  # taxa selic
        "ipca": 433,   # inflação IPCA
        "dolar": 1, # taxa do dólar
    }

    df = sgs.get(indicadores, start=data_inicio, end=data_fim)

    df.index.name = 'data'

    return df


def salvar_dados_macro(df: pd.DataFrame) -> None:
    """
    Salva os dados macroeconômicos em um arquivo CSV.

    Parameter
    ----------
    df : pd.DataFrame
        DataFrame contendo os indicadores macroeconômicos.
    """

    # caminho do arquivo: salvo na pasta paths.py(RAW_DIR)
    arquivo = RAW_DIR / "01_bcb_indicadores_economicos.csv"

    df.to_csv(arquivo, sep=';', decimal=',', encoding='utf-8-sig')
    df.describe()

def main():
    """Executa o pipeline de coleta de dados macroeconômicos."""

try:


    data_inicio, data_fim = obter_periodo_analise()

    print(f"Coletando dados macroeconômicos: {data_inicio} → {data_fim}")

    dados_macro = coletar_dados_macro(data_inicio, data_fim)

    print("\nPrévia dos dados coletados:")
    print(dados_macro.head())

    print("\nInformações do dataset:")
    print(dados_macro.info())
    print(dados_macro.describe())
    
    salvar_dados_macro(dados_macro)
    print(f"✅ Arquivo salvo: {RAW_DIR / '01_bcb_indicadores_economicos.csv'}")

except Exception as e:
    print(f"❌ Erro ao coletar dados macroeconômicos")
    print(f"Detalhes do erro: {e}")
    raise


if __name__ == "__main__":
    main()