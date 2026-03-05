import pandas as pd
import numpy as np

def calcular_retornos(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Calcula o retorno diário simples: (Preço_Atual / Preço_Anterior) - 1
    O agrupamento por ticker impede que o último preço da VALE3 seja 
    comparado com o primeiro preço da PETR4.
    """
    # Retorno Diário
    df['retorno_diario'] = df.groupby(agrupador)[coluna_preco].pct_change()
    
    # Retorno Acumulado (Produto Cumulativo: (1 + r1) * (1 + r2) ... - 1)
    # Primeiro preenchemos os NaN iniciais com 0 para o cumprod não quebrar
    df['retorno_acumulado'] = df.groupby(agrupador)['retorno_diario'].transform(
        lambda x: (1 + x.fillna(0)).cumprod() - 1
    )
    
    return df

def calcular_volatilidade(df: pd.DataFrame, coluna_retorno: str = 'retorno_diario', janela: int = 21, agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Calcula a volatilidade histórica em janela móvel anualizada.
    Fórmula: Desvio Padrão (janela) * Raiz(252 dias úteis)
    """
    df[f'volatilidade_{janela}d'] = df.groupby(agrupador)[coluna_retorno].transform(
        lambda x: x.rolling(window=janela).std() * np.sqrt(252)
    )
    
    return df

def calcular_medias_moveis(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Adiciona as médias de tendência (21 dias curtos e 200 dias longos).
    """
    df['mm21'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=21).mean())
    df['mm200'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=200).mean())
    return df
