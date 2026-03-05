import pandas as pd
import numpy as np

def calcular_retornos(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Calcula Retorno Simples (Visual) e Log Return (Para a IA).
    """
    # Retorno Simples (Para Dashboard)
    df['retorno_diario'] = df.groupby(agrupador)[coluna_preco].pct_change()
    
    # Log Return (Para Machine Learning - Aditivo e Normalizado)
    df['log_return'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: np.log(x / x.shift(1)))
    
    # Retorno Acumulado Simples
    df['retorno_acumulado'] = df.groupby(agrupador)['retorno_diario'].transform(
        lambda x: (1 + x.fillna(0)).cumprod() - 1
    )
    
    return df

def calcular_volatilidade(df: pd.DataFrame, coluna_retorno: str = 'log_return', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Calcula Volatilidade Rolling em janelas críticas (1 Mês, 1 Tri, 1 Ano).
    """
    janelas = [21, 63, 252]
    for janela in janelas:
        df[f'volatilidade_{janela}d'] = df.groupby(agrupador)[coluna_retorno].transform(
            lambda x: x.rolling(window=janela).std() * np.sqrt(252)
        )
    return df

def calcular_medias_moveis(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Médias Móveis, Spreads normalizados e Crossover (Golden/Death Cross).
    """
    df['sma_21'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=21).mean())
    df['sma_50'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=50).mean())
    df['sma_200'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=200).mean())
    
    # A feature que a IA realmente ama: Quão longe o preço está da média? (Spread)
    df['spread_sma50'] = (df[coluna_preco] - df['sma_50']) / df['sma_50']
    
    # Variável Categórica Binária: Golden Cross (1) ou Death Cross (0)
    df['tendencia_alta_50_200'] = (df['sma_50'] > df['sma_200']).astype(int)
    
    return df

def calcular_momentum(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    Retorno Acumulado em Janelas (Momentum) e Anomalia Quantitativa.
    """
    janelas_dias = {'1m': 21, '3m': 63, '6m': 126, '12m': 252}
    
    for label, dias in janelas_dias.items():
        df[f'momentum_{label}'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: (x / x.shift(dias)) - 1)
        
    # Anomalia Documentada: Retorno de 12 meses excluindo o último mês (Reversão à média de curto prazo)
    df['momentum_anomalia_12m_1m'] = df['momentum_12m'] - df['momentum_1m']
    
    return df

def calcular_volume_indicadores(df: pd.DataFrame, coluna_preco: str = 'close', coluna_vol: str = 'volume', agrupador: str = 'ticker') -> pd.DataFrame:
    """
    OBV (On-Balance Volume) e Volume Relativo.
    """
    # Direção do dia (1 para alta, -1 para baixa, 0 para estável)
    direcao = np.sign(df.groupby(agrupador)[coluna_preco].diff().fillna(0))
    
    # OBV: Acumula o volume com o sinal da direção
    df['obv'] = (df[coluna_vol] * direcao).groupby(df[agrupador]).cumsum()
    
    # Volume Relativo (Intensidade institucional)
    df['vol_medio_20d'] = df.groupby(agrupador)[coluna_vol].transform(lambda x: x.rolling(20).mean())
    df['volume_relativo'] = df[coluna_vol] / df['vol_medio_20d']
    
    return df
