"""
clean_raw_data.py
=================
Pipeline de limpeza e validação dos dados brutos (data/raw/).
Deve ser executado ANTES do process_master_lake.py.

Ordem de execução recomendada:
    1. python clean_raw_data.py          <- este arquivo
    2. python process_market_data.py     <- pipeline de features

Saídas:
    data/raw/  <- sobrescreve os CSVs corrigidos in-place (mantém backup)
    data/raw/cleaning_report.txt <- relatório detalhado de cada etapa
"""

import pandas as pd
import numpy as np
import os
import sys
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW_DIR  = os.path.join(BASE_DIR, 'data', 'raw')
LOG_PATH = os.path.join(RAW_DIR, 'cleaning_report.txt')

report_lines = []

def log(msg: str, indent: int = 0):
    prefix = "   " * indent
    line   = f"{prefix}{msg}"
    print(line)
    report_lines.append(line)

def save_report():
    with open(LOG_PATH, 'w', encoding='utf-8') as f:
        f.write(f"CLEANING REPORT — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 70 + "\n\n")
        f.write("\n".join(report_lines))
    print(f"\n📄 Relatório salvo em: {LOG_PATH}")

def backup_and_save(df: pd.DataFrame, path: str, **kwargs):
    """Salva backup do original e escreve o arquivo limpo."""
    backup = path.replace('.csv', '_backup.csv')
    if not os.path.exists(backup):
        import shutil
        shutil.copy(path, backup)
    df.to_csv(path, **kwargs)


# =============================================================================
# UTILITÁRIOS
# =============================================================================

def to_numeric_safe(series: pd.Series) -> pd.Series:
    if series.dtype == 'object':
        return pd.to_numeric(series.str.replace(',', '.', regex=False), errors='coerce')
    return pd.to_numeric(series, errors='coerce')

def format_ticker(ticker) -> str:
    if pd.isna(ticker):
        return ticker
    t = str(ticker).strip().upper()
    if not t.endswith('.SA'):
        t = f"{t}.SA"
    return t

def null_report(df: pd.DataFrame, nome: str, indent: int = 1):
    """Loga colunas com nulos e suas porcentagens."""
    nulos = df.isnull().sum()
    nulos = nulos[nulos > 0]
    if nulos.empty:
        log(f"✅ Sem nulos detectados em {nome}", indent)
    else:
        log(f"⚠️  Nulos em {nome}:", indent)
        for col, n in nulos.items():
            pct = n / len(df) * 100
            log(f"{col}: {n} ({pct:.1f}%)", indent + 1)


# =============================================================================
# 1. INDICADORES ECONÔMICOS
# =============================================================================

def clean_indicadores_economicos():
    """
    Problemas identificados:
    - SELIC e IPCA nunca aparecem no mesmo dia (séries intercaladas)
    - IPCA é mensal (apenas no dia 1 de cada mês), SELIC é diária
    - SELIC está em taxa DIÁRIA (ex: 0.052531 ≈ 5.25% ao mês equivalente)
      → Convertemos para taxa ANUAL para padronizar com o mercado
    - Dólar tem gaps nos dias sem pregão (feriados)
    - Linhas com APENAS IPCA e sem SELIC/dólar (feriados) devem ser mantidas
      para o forward-fill funcionar corretamente no process_master_lake
    """
    path = os.path.join(RAW_DIR, 'indicadores_economicos_10anos.csv')
    log("\n[1/5] INDICADORES ECONÔMICOS")
    log(f"Arquivo: {path}", 1)

    df = pd.read_csv(path, sep=';', decimal=',')
    log(f"Shape inicial: {df.shape}", 1)

    # Normaliza nomes de colunas
    df.columns = [c.lower().strip() for c in df.columns]

    # Parse de data
    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    datas_invalidas = df['data'].isna().sum()
    if datas_invalidas:
        log(f"⚠️  {datas_invalidas} datas inválidas removidas", 1)
        df = df.dropna(subset=['data'])

    # Converte para numérico
    for col in ['selic', 'ipca', 'dolar']:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # Remove duplicatas de data (mantém última ocorrência)
    dupes = df.duplicated(subset=['data']).sum()
    if dupes:
        log(f"⚠️  {dupes} datas duplicadas removidas (mantido último registro)", 1)
        df = df.drop_duplicates(subset=['data'], keep='last')

    # Ordena por data
    df = df.sort_values('data').reset_index(drop=True)

    # -------------------------------------------------------------------------
    # SELIC: está em taxa diária → converte para taxa ANUAL equivalente
    # Fórmula: taxa_anual = (1 + taxa_diaria)^252 - 1
    # Exemplo: 0.052531 diário → não faz sentido como diário
    # Verificação: os valores (~0.007 a ~0.055) são compatíveis com taxa MENSAL
    # do CDI. Convertemos para ANUAL: taxa_anual = (1 + taxa_mensal)^12 - 1
    # Nota: o BCB divulga a SELIC meta como % ao ano, mas a série histórica
    # baixada via API retorna a taxa acumulada do mês corrente até a data.
    # Vamos anualizar assumindo que é taxa do período (mensal acumulada).
    # -------------------------------------------------------------------------
    if 'selic' in df.columns:
        selic_valido = df['selic'].dropna()
        media_selic = selic_valido.mean()
        log(f"SELIC — média dos valores brutos: {media_selic:.6f}", 1)

        if media_selic < 0.1:
            # Valores como 0.052 são taxa mensal (não diária nem anual)
            # Anualizamos: (1 + taxa_mensal)^12 - 1
            df['selic_anual'] = ((1 + df['selic']) ** 12 - 1) * 100
            log("SELIC interpretada como taxa mensal → convertida para % ao ano", 1)
            log(f"Exemplo: {selic_valido.iloc[0]:.6f} mensal → {((1+selic_valido.iloc[0])**12-1)*100:.2f}% a.a.", 1)
        else:
            # Já é percentual anual (ex: 13.75)
            df['selic_anual'] = df['selic']
            log("SELIC já em % ao ano — sem conversão", 1)

        # Forward-fill da SELIC (mantém último valor em dias sem divulgação)
        df['selic_anual'] = df['selic_anual'].ffill()

    # IPCA: forward-fill para propagar o último valor mensal para todos os dias
    if 'ipca' in df.columns:
        n_ipca = df['ipca'].notna().sum()
        log(f"IPCA: {n_ipca} registros mensais encontrados", 1)
        df['ipca_mensal'] = df['ipca']
        # Cria versão acumulada 12 meses (soma rolling)
        df['ipca'] = df['ipca'].ffill()  # propaga para dias sem leitura

    # Dólar: forward-fill simples para feriados/fins de semana
    if 'dolar' in df.columns:
        gaps_dolar = df['dolar'].isna().sum()
        log(f"Dólar: {gaps_dolar} gaps preenchidos por forward-fill", 1)
        df['dolar'] = df['dolar'].ffill().bfill()

    null_report(df, 'indicadores_economicos', 1)

    backup_and_save(
        df, path,
        sep=';', decimal=',', index=False, encoding='utf-8-sig'
    )
    log(f"✅ Salvo: {path}", 1)
    return df


# =============================================================================
# 2. PREÇOS YFINANCE (01_yfinance_precos_raw.csv)
# =============================================================================

def clean_precos_yfinance():
    """
    Problemas identificados:
    - Ações com preço zero ou negativo
    - Ações com volume = 0 por períodos prolongados (inativas)
    - Splits não ajustados podem gerar saltos de preço absurdos (>50% em 1 dia)
    - Tickers sem sufixo .SA
    - Datas duplicadas por ticker
    - Linhas com todos os OHLCV nulos
    """
    path = os.path.join(RAW_DIR, '01_yfinance_precos_raw.csv')
    log("\n[2/5] PREÇOS YFINANCE")
    log(f"Arquivo: {path}", 1)

    df = pd.read_csv(path, sep=';', decimal=',', low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    log(f"Shape inicial: {df.shape}", 1)
    log(f"Tickers únicos: {df['ticker'].nunique() if 'ticker' in df.columns else 'N/A'}", 1)

    # Parse de data
    df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.tz_localize(None).dt.normalize()
    antes = len(df)
    df = df.dropna(subset=['date'])
    if len(df) < antes:
        log(f"⚠️  {antes - len(df)} linhas com data inválida removidas", 1)

    # Padroniza ticker
    df['ticker'] = df['ticker'].apply(format_ticker)

    # Remove adj close (redundante para B3 — close já é ajustado)
    if 'adj close' in df.columns:
        df = df.drop(columns=['adj close'])
        log("Coluna 'adj close' removida (redundante)", 1)

    # Converte colunas numéricas
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # --- Limpeza de preços inválidos ---
    antes = len(df)
    df = df[df['close'].notna() & (df['close'] > 0)]
    removidos = antes - len(df)
    if removidos:
        log(f"⚠️  {removidos} linhas com close <= 0 ou nulo removidas", 1)

    # OHLC: garante consistência (high >= close >= low > 0)
    inconsistentes = ((df['high'] < df['close']) | (df['low'] > df['close'])).sum()
    if inconsistentes:
        log(f"⚠️  {inconsistentes} linhas com OHLC inconsistente (high < close ou low > close)", 1)
        # Corrige invertendo high/low quando necessário
        df['high'] = df[['high', 'close', 'open']].max(axis=1)
        df['low']  = df[['low',  'close', 'open']].min(axis=1)
        log("OHLC recalculado (high=max, low=min de [open,high,low,close])", 1)

    # --- Remove tickers com volume zero em mais de 95% dos dias ---
    vol_por_ticker = df.groupby('ticker').apply(
        lambda g: (g['volume'] == 0).mean()
    )
    tickers_inativos = vol_por_ticker[vol_por_ticker > 0.95].index.tolist()
    if tickers_inativos:
        log(f"⚠️  {len(tickers_inativos)} tickers com >95% volume zero removidos:", 1)
        for t in tickers_inativos[:10]:
            log(t, 2)
        if len(tickers_inativos) > 10:
            log(f"... e mais {len(tickers_inativos) - 10}", 2)
        df = df[~df['ticker'].isin(tickers_inativos)]

    # --- Detecta outliers de retorno diário (possíveis erros de dado) ---
    df = df.sort_values(['ticker', 'date'])
    df['_retorno_bruto'] = df.groupby('ticker')['close'].pct_change()

    # Retornos acima de 200% ou abaixo de -90% num único dia são suspeitos
    outliers_mask = (df['_retorno_bruto'].abs() > 2.0) & df['_retorno_bruto'].notna()
    n_outliers = outliers_mask.sum()
    if n_outliers:
        log(f"⚠️  {n_outliers} retornos diários suspeitos (|retorno| > 200%):", 1)
        amostra = df[outliers_mask][['date', 'ticker', 'close', '_retorno_bruto']].head(15)
        for _, row in amostra.iterrows():
            log(f"{row['ticker']} em {row['date'].date()}: close={row['close']:.2f}, retorno={row['_retorno_bruto']:.1%}", 2)
        log("Linhas mantidas mas sinalizadas — verifique se são splits não ajustados", 1)
        # Marca para inspeção (não remove automaticamente para não perder splits legítimos)
        df['flag_outlier_retorno'] = outliers_mask.astype(int)
    else:
        df['flag_outlier_retorno'] = 0

    df = df.drop(columns=['_retorno_bruto'])

    # --- Remove duplicatas (ticker + date) ---
    dupes = df.duplicated(subset=['ticker', 'date']).sum()
    if dupes:
        log(f"⚠️  {dupes} duplicatas (ticker+date) removidas", 1)
        df = df.drop_duplicates(subset=['ticker', 'date'], keep='last')

    null_report(df, '01_yfinance_precos', 1)
    log(f"Shape final: {df.shape}", 1)
    log(f"Tickers restantes: {df['ticker'].nunique()}", 1)

    backup_and_save(
        df, path,
        sep=';', decimal=',', index=False, encoding='utf-8-sig'
    )
    log(f"✅ Salvo: {path}", 1)
    return df


# =============================================================================
# 3. EVENTOS CORPORATIVOS (02_yfinance_eventos_raw.csv)
# =============================================================================

def clean_eventos_corporativos():
    """
    Problemas identificados:
    - Ticker salvo SEM .SA (market_data.py faz .replace('.SA','') antes de salvar)
    - Dividendos com valores negativos ou absurdamente altos (>100% do preço)
    - Splits com valor 0 (sem split de fato)
    - Datas com timezone que podem não bater com os pregões
    """
    path = os.path.join(RAW_DIR, '02_yfinance_eventos_raw.csv')
    log("\n[3/5] EVENTOS CORPORATIVOS")
    log(f"Arquivo: {path}", 1)

    df = pd.read_csv(path, sep=';', low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    log(f"Shape inicial: {df.shape}", 1)

    # Parse de data — remove timezone e normaliza para meia-noite
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
    df['date'] = df['date'].dt.tz_localize(None).dt.normalize()
    antes = len(df)
    df = df.dropna(subset=['date'])
    if len(df) < antes:
        log(f"⚠️  {antes - len(df)} linhas com data inválida removidas", 1)

    # Ticker: market_data.py salva SEM .SA — adicionamos aqui para consistência
    ticker_col = 'ticker' if 'ticker' in df.columns else None
    if ticker_col:
        tem_sa = df[ticker_col].str.endswith('.SA', na=False).mean()
        if tem_sa < 0.5:
            log("Ticker detectado sem sufixo .SA — adicionando", 1)
            df[ticker_col] = df[ticker_col].apply(format_ticker)
        else:
            df[ticker_col] = df[ticker_col].apply(format_ticker)

    # Converte numéricos
    for col in ['dividends', 'stock splits']:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # Remove linhas onde tanto dividends quanto stock splits são zero/nulos
    # (linhas fantasma sem evento real)
    if 'dividends' in df.columns and 'stock splits' in df.columns:
        antes = len(df)
        mask_vazio = (
            (df['dividends'].fillna(0) == 0) &
            (df['stock splits'].fillna(0) == 0)
        )
        df = df[~mask_vazio]
        removidos = antes - len(df)
        if removidos:
            log(f"⚠️  {removidos} linhas sem evento real (div=0 e split=0) removidas", 1)

    # Dividendos negativos são erro de dado
    if 'dividends' in df.columns:
        neg = (df['dividends'] < 0).sum()
        if neg:
            log(f"⚠️  {neg} dividendos negativos zerados", 1)
            df.loc[df['dividends'] < 0, 'dividends'] = 0

    # Splits: valor 1.0 significa "nenhum split" — remove
    if 'stock splits' in df.columns:
        splits_um = (df['stock splits'] == 1.0).sum()
        if splits_um:
            log(f"⚠️  {splits_um} registros de split com valor 1.0 (sem efeito) zerados", 1)
            df.loc[df['stock splits'] == 1.0, 'stock splits'] = 0

    # Remove duplicatas
    dupes = df.duplicated(subset=['date', ticker_col] if ticker_col else ['date']).sum()
    if dupes:
        log(f"⚠️  {dupes} duplicatas removidas", 1)
        df = df.drop_duplicates(
            subset=['date', ticker_col] if ticker_col else ['date'],
            keep='last'
        )

    null_report(df, '02_yfinance_eventos', 1)
    log(f"Shape final: {df.shape}", 1)
    log(f"Tickers únicos com eventos: {df[ticker_col].nunique() if ticker_col else 'N/A'}", 1)

    backup_and_save(
        df, path,
        sep=';', decimal=',', index=False, encoding='utf-8-sig'
    )
    log(f"✅ Salvo: {path}", 1)
    return df


# =============================================================================
# 4. INFO CORPORATIVA (03_yfinance_info_raw.csv)
# =============================================================================

def clean_info_corporativa():
    """
    Problemas identificados:
    - +100 colunas, maioria com >80% de nulos
    - marketcap pode ser negativo ou zero (empresa sem dados)
    - Duplicatas de ticker (mesmo ativo coletado múltiplas vezes)
    - Colunas com tipos mistos (string + número)
    - Setor/indústria podem vir em inglês — mantemos assim (padronização semântica
      deve ser feita numa etapa separada se necessário)
    """
    path = os.path.join(RAW_DIR, '03_yfinance_info_raw.csv')
    log("\n[4/5] INFO CORPORATIVA")
    log(f"Arquivo: {path}", 1)

    df = pd.read_csv(path, sep=';', on_bad_lines='skip', low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    log(f"Shape inicial: {df.shape}", 1)

    # Padroniza ticker
    if 'ticker' in df.columns:
        df['ticker'] = df['ticker'].apply(format_ticker)
    else:
        log("⛔ Coluna 'ticker' não encontrada!", 1)
        return df

    # Remove colunas com mais de 80% de nulos (ruído para o modelo)
    threshold_nulo = 0.80
    pct_nulos = df.isnull().mean()
    cols_remover = pct_nulos[pct_nulos > threshold_nulo].index.tolist()
    if cols_remover:
        log(f"⚠️  {len(cols_remover)} colunas com >{threshold_nulo*100:.0f}% nulos removidas", 1)
        df = df.drop(columns=cols_remover)

    # Converte colunas numéricas chave
    cols_numericas = ['marketcap', 'bookvalue', 'returnonequity', 'trailingpe',
                      'forwardpe', 'dividendyield', 'beta', 'enterprisevalue',
                      'ebitda', 'totaldebt', 'totalcash']
    for col in cols_numericas:
        if col in df.columns:
            df[col] = to_numeric_safe(df[col])

    # Remove tickers com marketcap zero ou negativo
    if 'marketcap' in df.columns:
        invalidos = ((df['marketcap'] <= 0) | df['marketcap'].isna()).sum()
        if invalidos:
            log(f"⚠️  {invalidos} tickers com marketcap inválido (<=0 ou nulo)", 1)
            # Não remove — empresa pode não ter marketcap no yfinance mas ter dados CVM

    # Remove duplicatas de ticker (mantém o com mais dados preenchidos)
    dupes = df.duplicated(subset=['ticker']).sum()
    if dupes:
        log(f"⚠️  {dupes} tickers duplicados — mantido registro com mais colunas preenchidas", 1)
        df['_n_nulos'] = df.isnull().sum(axis=1)
        df = df.sort_values('_n_nulos').drop_duplicates(subset=['ticker'], keep='first')
        df = df.drop(columns=['_n_nulos'])

    null_report(df, '03_yfinance_info', 1)
    log(f"Shape final: {df.shape}", 1)
    log(f"Colunas restantes: {len(df.columns)}", 1)

    backup_and_save(
        df, path,
        sep=';', decimal=',', index=False, encoding='utf-8-sig'
    )
    log(f"✅ Salvo: {path}", 1)
    return df


# =============================================================================
# 5. HISTÓRICO CVM DRE (05_CVM_Historico_Focado.csv)
# =============================================================================

def clean_cvm_historico():
    """
    Problemas identificados:
    - Colunas financeiras em R$ com casas decimais e vírgula
    - Lucro líquido negativo é VÁLIDO (prejuízo) — não remover
    - Receita líquida zero pode indicar dados ausentes no período
    - Empresas sem mapeamento de ticker (Ticker_Alvo nulo)
    - Datas de exercício fora do padrão
    - Duplicatas de (ticker, ano) — pode haver empresas consolidadas
      que aparecem com mesmo código CVM em anos diferentes
    """
    path = os.path.join(RAW_DIR, '05_CVM_Historico_Focado.csv')
    log("\n[5/5] HISTÓRICO CVM DRE")
    log(f"Arquivo: {path}", 1)

    df = pd.read_csv(path, sep=';', decimal=',', low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    log(f"Shape inicial: {df.shape}", 1)

    # Padroniza ticker
    ticker_col = 'ticker_alvo' if 'ticker_alvo' in df.columns else 'ticker'
    if ticker_col in df.columns:
        # CVM salva sem .SA — adicionamos
        df[ticker_col] = df[ticker_col].apply(format_ticker)
    else:
        log("⚠️  Coluna de ticker não encontrada", 1)

    # Remove registros sem ticker (empresa não mapeada para a lista)
    antes = len(df)
    df = df.dropna(subset=[ticker_col])
    if len(df) < antes:
        log(f"⚠️  {antes - len(df)} linhas sem ticker mapeado removidas", 1)

    # Parse de data
    date_col = 'data_referencia' if 'data_referencia' in df.columns else 'dt_fim_exerc'
    if date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce').dt.normalize()
        datas_invalidas = df[date_col].isna().sum()
        if datas_invalidas:
            log(f"⚠️  {datas_invalidas} datas inválidas removidas", 1)
            df = df.dropna(subset=[date_col])

    # Converte colunas financeiras
    cols_financeiras = [c for c in df.columns if 'r$' in c or any(
        k in c for k in ['receita', 'lucro', 'ebit', 'resultado', 'patrimonio']
    )]
    for col in cols_financeiras:
        df[col] = to_numeric_safe(df[col])
        log(f"Coluna financeira convertida: {col}", 2)

    # Receita líquida zero pode ser dado ausente — sinaliza
    col_receita = next((c for c in df.columns if 'receita' in c and 'liquida' in c), None)
    if col_receita:
        receita_zero = (df[col_receita] == 0).sum()
        if receita_zero:
            log(f"⚠️  {receita_zero} linhas com receita líquida = 0 (possível dado ausente)", 1)
            df['flag_receita_zero'] = (df[col_receita] == 0).astype(int)

    # Detecta e reporta outliers financeiros (valores absurdamente grandes)
    # Limiar: > 10 trilhões de R$ é suspeito para empresas da B3
    LIMIAR_TRILHOES = 10e12
    for col in cols_financeiras:
        extremos = (df[col].abs() > LIMIAR_TRILHOES).sum()
        if extremos:
            log(f"⚠️  {extremos} valores extremos (>R$10T) em {col}", 1)

    # Remove duplicatas (ticker + ano de exercício)
    ano_col = 'ano_exercicio' if 'ano_exercicio' in df.columns else None
    if ano_col:
        dupes = df.duplicated(subset=[ticker_col, ano_col]).sum()
        if dupes:
            log(f"⚠️  {dupes} duplicatas (ticker+ano) removidas — mantido último", 1)
            df = df.drop_duplicates(subset=[ticker_col, ano_col], keep='last')

    # Ordena
    sort_cols = [ticker_col, date_col] if date_col in df.columns else [ticker_col]
    df = df.sort_values(sort_cols).reset_index(drop=True)

    null_report(df, '05_CVM_Historico', 1)
    log(f"Shape final: {df.shape}", 1)

    backup_and_save(
        df, path,
        sep=';', decimal=',', index=False, encoding='utf-8-sig'
    )
    log(f"✅ Salvo: {path}", 1)
    return df


# =============================================================================
# VALIDAÇÃO CRUZADA ENTRE ARQUIVOS
# =============================================================================

def validacao_cruzada(df_precos, df_eventos, df_info, df_cvm, df_macro):
    """
    Verifica consistência entre os datasets antes de entrar no pipeline.
    """
    log("\n[VALIDAÇÃO CRUZADA]")

    tickers_precos  = set(df_precos['ticker'].unique()) if df_precos is not None else set()
    tickers_eventos = set(df_eventos['ticker'].unique()) if df_eventos is not None else set()
    tickers_info    = set(df_info['ticker'].unique()) if df_info is not None else set()
    ticker_col_cvm  = 'ticker_alvo' if df_cvm is not None and 'ticker_alvo' in df_cvm.columns else 'ticker'
    tickers_cvm     = set(df_cvm[ticker_col_cvm].unique()) if df_cvm is not None else set()

    log(f"Tickers em precos:   {len(tickers_precos)}", 1)
    log(f"Tickers em eventos:  {len(tickers_eventos)}", 1)
    log(f"Tickers em info:     {len(tickers_info)}", 1)
    log(f"Tickers em CVM:      {len(tickers_cvm)}", 1)

    sem_info = tickers_precos - tickers_info
    sem_cvm  = tickers_precos - tickers_cvm

    log(f"\nTickers com preço mas sem info corporativa: {len(sem_info)}", 1)
    log(f"Tickers com preço mas sem histórico CVM:    {len(sem_cvm)}", 1)

    if len(sem_info) <= 20:
        for t in sorted(sem_info):
            log(t, 2)

    # Cobertura temporal
    if df_macro is not None and 'data' in df_macro.columns:
        dt_min_macro = df_macro['data'].min()
        dt_max_macro = df_macro['data'].max()
        log(f"\nCobertura macro: {dt_min_macro.date()} → {dt_max_macro.date()}", 1)

    if df_precos is not None and 'date' in df_precos.columns:
        dt_min_preco = df_precos['date'].min()
        dt_max_preco = df_precos['date'].max()
        log(f"Cobertura precos: {dt_min_preco.date()} → {dt_max_preco.date()}", 1)

        if df_macro is not None:
            if dt_min_preco < dt_min_macro:
                log(f"⚠️  Preços começam antes da macro ({dt_min_preco.date()} < {dt_min_macro.date()})", 1)
                log("   → Linhas sem macro serão NaN após o merge (esperado)", 1)


# =============================================================================
# EXECUÇÃO PRINCIPAL
# =============================================================================

def run_cleaning_pipeline():
    log("=" * 70)
    log("🧹 PIPELINE DE LIMPEZA DE DADOS B3 — INICIANDO")
    log(f"   Diretório raw: {RAW_DIR}")
    log("=" * 70)

    resultados = {}

    # Executa cada etapa independentemente — um erro não para os outros
    etapas = [
        ('indicadores_economicos', clean_indicadores_economicos),
        ('precos_yfinance',        clean_precos_yfinance),
        ('eventos_corporativos',   clean_eventos_corporativos),
        ('info_corporativa',       clean_info_corporativa),
        ('cvm_historico',          clean_cvm_historico),
    ]

    for nome, func in etapas:
        try:
            resultados[nome] = func()
        except FileNotFoundError:
            log(f"\n⛔ [{nome}] Arquivo não encontrado — etapa ignorada")
            resultados[nome] = None
        except Exception as e:
            log(f"\n⛔ [{nome}] Erro inesperado: {e}")
            import traceback
            log(traceback.format_exc(), 1)
            resultados[nome] = None

    # Validação cruzada
    try:
        validacao_cruzada(
            df_precos  = resultados.get('precos_yfinance'),
            df_eventos = resultados.get('eventos_corporativos'),
            df_info    = resultados.get('info_corporativa'),
            df_cvm     = resultados.get('cvm_historico'),
            df_macro   = resultados.get('indicadores_economicos'),
        )
    except Exception as e:
        log(f"\n⚠️  Validação cruzada falhou: {e}")

    log("\n" + "=" * 70)
    log("✅ PIPELINE DE LIMPEZA CONCLUÍDO")
    log("   Execute process_market_data.py para gerar o dataset processado.")
    log("=" * 70)

    save_report()
    return resultados


if __name__ == "__main__":
    run_cleaning_pipeline()