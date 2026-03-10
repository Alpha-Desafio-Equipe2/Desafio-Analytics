import pandas as pd
import numpy as np

def calcular_retornos(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """Calcula Retorno Simples, Log Return e Retorno Acumulado."""
    df['retorno_diario'] = df.groupby(agrupador)[coluna_preco].pct_change()
    df['log_return'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: np.log(x / x.shift(1)))
    df['retorno_acumulado'] = df.groupby(agrupador)['retorno_diario'].transform(
        lambda x: (1 + x.fillna(0)).cumprod() - 1
    )
    return df

def calcular_volatilidade(df: pd.DataFrame, coluna_retorno: str = 'log_return', agrupador: str = 'ticker') -> pd.DataFrame:
    """Calcula Volatilidade Rolling (21, 63, 252 dias)."""
    janelas = [21, 63, 252]
    for janela in janelas:
        df[f'volatilidade_{janela}d'] = df.groupby(agrupador)[coluna_retorno].transform(
            lambda x: x.rolling(window=janela).std() * np.sqrt(252)
        )
    return df

def calcular_medias_moveis(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """Médias Móveis e Sinais de Tendência."""
    df['sma_21'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=21).mean())
    df['sma_50'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=50).mean())
    df['sma_200'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=200).mean())
    df['spread_sma50'] = (df[coluna_preco] - df['sma_50']) / df['sma_50']
    df['tendencia_alta_50_200'] = (df['sma_50'] > df['sma_200']).astype(int)
    return df

def calcular_momentum(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """Retorno Acumulado em Janelas de Tempo."""
    janelas_dias = {'1m': 21, '3m': 63, '6m': 126, '12m': 252}
    for label, dias in janelas_dias.items():
        df[f'momentum_{label}'] = df.groupby(agrupador)[coluna_preco].transform(lambda x: (x / x.shift(dias)) - 1)
    df['momentum_anomalia_12m_1m'] = df['momentum_12m'] - df['momentum_1m']
    return df

def calcular_volume_indicadores(df: pd.DataFrame, coluna_preco: str = 'close', coluna_vol: str = 'volume', agrupador: str = 'ticker') -> pd.DataFrame:
    """OBV e Volume Relativo."""
    direcao = np.sign(df.groupby(agrupador)[coluna_preco].diff().fillna(0))
    df['obv'] = (df[coluna_vol] * direcao).groupby(df[agrupador]).cumsum()
    df['vol_medio_20d'] = df.groupby(agrupador)[coluna_vol].transform(lambda x: x.rolling(20).mean())
    df['volume_relativo'] = df[coluna_vol] / df['vol_medio_20d']
    return df

def calcular_rsi(df: pd.DataFrame, coluna_preco: str = 'close', janela: int = 14, agrupador: str = 'ticker') -> pd.DataFrame:
    """Índice de Força Relativa (RSI)."""
    def rsi_calc(x):
        delta = x.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=janela).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=janela).mean()
        rs = gain / (loss + 1e-9)
        return 100 - (100 / (1 + rs))
    
    df[f'rsi_{janela}'] = df.groupby(agrupador)[coluna_preco].transform(rsi_calc)
    return df

def calcular_bandas_bollinger(df: pd.DataFrame, coluna_preco: str = 'close', janela: int = 20, num_std: int = 2, agrupador: str = 'ticker') -> pd.DataFrame:
    """Bandas de Bollinger."""
    sma = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=janela).mean())
    std = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.rolling(window=janela).std())
    df['bb_upper'] = sma + (std * num_std)
    df['bb_lower'] = sma - (std * num_std)
    return df

def calcular_macd(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """MACD e Sinal."""
    ema_rapida = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema_lenta = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.ewm(span=26, adjust=False).mean())
    df['macd_line'] = ema_rapida - ema_lenta
    df['macd_signal'] = df.groupby(agrupador)['macd_line'].transform(lambda x: x.ewm(span=9, adjust=False).mean())
    df['macd_hist'] = df['macd_line'] - df['macd_signal']
    return df

def calcular_risco_drawdown(df: pd.DataFrame, coluna_preco: str = 'close', agrupador: str = 'ticker') -> pd.DataFrame:
    """Drawdown Histórico."""
    pico = df.groupby(agrupador)[coluna_preco].transform(lambda x: x.cummax())
    df['drawdown'] = (df[coluna_preco] / (pico + 1e-9)) - 1
    return df

def calcular_fundamental_ratios(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula ROE, Dividend Yield e Indicadores de Valuation."""
    # ROE = Lucro Líquido / Patrimônio Líquido
    df['roe'] = df['cvm_lucro_liquido'] / (df['cvm_patrimonio_liquido'] + 1e-9)
    
    # Dividend Yield LTM
    df['dy_diario'] = df['dividends'] / (df['close'] + 1e-9)
    df['dy_ltm'] = df.groupby('ticker')['dy_diario'].transform(lambda x: x.rolling(252, min_periods=1).sum())
    
    # P/L e P/VP
    df['p_l'] = df['marketcap'] / (df['cvm_lucro_liquido'] + 1e-9)
    df['p_vp'] = df['marketcap'] / (df['cvm_patrimonio_liquido'] + 1e-9)
    
    # Payout Ratio (LTM Dividends / Lucro Líquido)
    # Dividends LTM em valor financeiro = dy_ltm * close (aproximação) ou soma dos Proventos
    df['payout_ratio'] = (df['dy_ltm'] * df['close'] * df['marketcap'] / df['close']) / (df['cvm_lucro_liquido'] + 1e-9)
    # Forma simplificada: dy_ltm * marketcap / cvm_lucro_liquido
    df['payout_ratio'] = (df['dy_ltm'] * df['marketcap']) / (df['cvm_lucro_liquido'] + 1e-9)

    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return df

def calcular_tendencias_financeiras(df: pd.DataFrame, agrupador: str = 'ticker') -> pd.DataFrame:
    """Calcula tendências de longo prazo (10 anos) para Yield e Lucro."""
    # Tendência de Yield (DY): Crescimento, Lateralização ou Queda (baseado inclinacao de 3 anos para durar menos tempo de calculo)
    def get_trend(x):
        if len(x) < 252*3: return "Incompleto"
        # Compara média dos últimos 252 dias vs média de 3 anos atrás
        current = x.iloc[-252:].mean()
        older = x.iloc[-252*3:-252*2].mean()
        diff = (current - older) / (older + 1e-9)
        if diff > 0.05: return "Crescimento"
        if diff < -0.05: return "Queda"
        return "Lateralização"

    # Aplicamos apenas no último registro por ativo por performance ou transformamos
    df['tendencia_dy_3y'] = df.groupby(agrupador)['dy_ltm'].transform(get_trend)
    
    return df
