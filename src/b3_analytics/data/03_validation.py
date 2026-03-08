"""
03_validation.py
================
Validação dos dados extraídos pelo pipeline yfinance (02_yfinance_data_extraction.py).

Datasets validados:
    - 02_yfinance_precos_raw.csv      → Preços OHLCV
    - 03_yfinance_eventos_raw.csv     → Eventos corporativos (dividendos/splits)
    - 04_yfinance_info_raw.csv        → Metadados / fundamentals
    - 05_yfinance_balancos_raw.csv    → Balanços anuais (DRE)

Categorias de validação:
    1. Completude        – nulos, tickers faltando
    2. Consistência      – preços negativos, splits inválidos, OHLCV lógico
    3. Integridade temporal – datas fora do range, gaps, duplicatas
    4. Volume de dados   – mínimo de linhas esperado por dataset/ticker
"""

from __future__ import annotations

import sys
import warnings
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths – respeita a convenção do projeto (b3_analytics.utils.paths)
# ---------------------------------------------------------------------------
try:
    from b3_analytics.utils.paths import RAW_DIR  # type: ignore
except ImportError:
    # Fallback para execução avulsa
    RAW_DIR = Path(__file__).resolve().parents[3] / "data" / "raw"

# ---------------------------------------------------------------------------
# Configurações de limites
# ---------------------------------------------------------------------------
DATA_INICIO_PADRAO: date = date(2015, 1, 1)
DATA_FIM_PADRAO: date = date.today()

# Nº mínimo de linhas por ticker/dataset (ajuste conforme seu universo)
MIN_LINHAS_PRECOS: int = 200        # ~1 ano de pregões
MIN_LINHAS_EVENTOS: int = 1         # ao menos 1 evento no histórico
MIN_LINHAS_BALANCO: int = 3         # ao menos 3 anos de DRE

# Gap máximo permitido entre pregões (em dias corridos, excluindo fins de semana)
MAX_GAP_PREGAO_DIAS: int = 10

# Colunas críticas por dataset
COLUNAS_PRECOS = ["ticker", "Date", "Open", "High", "Low", "Close", "Volume"]
COLUNAS_EVENTOS = ["ticker", "date", "type", "value"]
COLUNAS_INFO    = ["ticker"]
COLUNAS_BALANCO = ["ticker", "year"]

# CSV read kwargs
CSV_KWARGS: dict[str, Any] = dict(sep=";", decimal=",", encoding="utf-8-sig")


# ---------------------------------------------------------------------------
# Estrutura de resultado
# ---------------------------------------------------------------------------
@dataclass
class ValidationResult:
    dataset: str
    categoria: str
    severidade: str          # "ERRO" | "AVISO" | "OK"
    mensagem: str
    detalhes: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        icone = {"ERRO": "❌", "AVISO": "⚠️ ", "OK": "✅"}.get(self.severidade, "ℹ️ ")
        base = f"{icone} [{self.dataset}] {self.categoria}: {self.mensagem}"
        if self.detalhes:
            base += "\n     → " + "\n     → ".join(self.detalhes[:10])
            if len(self.detalhes) > 10:
                base += f"\n     → ... (+{len(self.detalhes) - 10} outros)"
        return base


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ler_csv(nome_arquivo: str) -> pd.DataFrame | None:
    caminho = RAW_DIR / nome_arquivo
    if not caminho.exists():
        print(f"⚠️  Arquivo não encontrado: {caminho}")
        return None
    return pd.read_csv(caminho, **CSV_KWARGS)


def _ok(dataset: str, categoria: str, mensagem: str) -> ValidationResult:
    return ValidationResult(dataset, categoria, "OK", mensagem)


def _aviso(dataset: str, categoria: str, mensagem: str, detalhes: list[str] | None = None) -> ValidationResult:
    return ValidationResult(dataset, categoria, "AVISO", mensagem, detalhes or [])


def _erro(dataset: str, categoria: str, mensagem: str, detalhes: list[str] | None = None) -> ValidationResult:
    return ValidationResult(dataset, categoria, "ERRO", mensagem, detalhes or [])


# ===========================================================================
# 1. COMPLETUDE
# ===========================================================================

def validar_completude_precos(df: pd.DataFrame, nome: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []

    # Colunas obrigatórias
    faltando = [c for c in COLUNAS_PRECOS if c not in df.columns]
    if faltando:
        resultados.append(_erro(nome, "Completude", f"Colunas ausentes: {faltando}"))
    else:
        resultados.append(_ok(nome, "Completude", "Todas as colunas obrigatórias presentes"))

    # Nulos por coluna
    for col in [c for c in COLUNAS_PRECOS if c in df.columns and c != "ticker"]:
        pct_nulo = df[col].isna().mean() * 100
        if pct_nulo > 5:
            resultados.append(_erro(nome, "Completude", f"'{col}' com {pct_nulo:.1f}% de nulos"))
        elif pct_nulo > 0:
            resultados.append(_aviso(nome, "Completude", f"'{col}' com {pct_nulo:.1f}% de nulos"))

    # Tickers com Close inteiramente nulo
    if "ticker" in df.columns and "Close" in df.columns:
        sem_close = df.groupby("ticker")["Close"].apply(lambda s: s.isna().all())
        tickers_sem_close = sem_close[sem_close].index.tolist()
        if tickers_sem_close:
            resultados.append(_erro(nome, "Completude", f"{len(tickers_sem_close)} ticker(s) sem nenhum Close", tickers_sem_close))

    return resultados


def validar_completude_generica(df: pd.DataFrame, nome: str, colunas_criticas: list[str]) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []
    faltando = [c for c in colunas_criticas if c not in df.columns]
    if faltando:
        resultados.append(_erro(nome, "Completude", f"Colunas ausentes: {faltando}"))
    else:
        resultados.append(_ok(nome, "Completude", "Colunas obrigatórias presentes"))

    for col in [c for c in colunas_criticas if c in df.columns]:
        pct_nulo = df[col].isna().mean() * 100
        if pct_nulo > 0:
            nivel = "ERRO" if pct_nulo > 10 else "AVISO"
            resultados.append(ValidationResult(nome, "Completude", nivel, f"'{col}' com {pct_nulo:.1f}% de nulos"))

    return resultados


# ===========================================================================
# 2. CONSISTÊNCIA
# ===========================================================================

def validar_consistencia_precos(df: pd.DataFrame, nome: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []

    for col in ["Open", "High", "Low", "Close"]:
        if col not in df.columns:
            continue
        neg = df[col] < 0
        if neg.any():
            tickers = df.loc[neg, "ticker"].unique().tolist() if "ticker" in df.columns else []
            resultados.append(_erro(nome, "Consistência", f"'{col}' com valores negativos ({neg.sum()} linhas)", [str(t) for t in tickers]))

    # High >= Low
    if {"High", "Low"}.issubset(df.columns):
        inv = df["High"] < df["Low"]
        if inv.any():
            resultados.append(_erro(nome, "Consistência", f"High < Low em {inv.sum()} linhas"))
        else:
            resultados.append(_ok(nome, "Consistência", "High ≥ Low em todas as linhas"))

    # High >= Open e High >= Close
    for ref in ["Open", "Close"]:
        if {"High", ref}.issubset(df.columns):
            inv = df["High"] < df[ref]
            if inv.any():
                resultados.append(_aviso(nome, "Consistência", f"High < {ref} em {inv.sum()} linhas (possível ajuste corporativo)"))

    # Low <= Open e Low <= Close
    for ref in ["Open", "Close"]:
        if {"Low", ref}.issubset(df.columns):
            inv = df["Low"] > df[ref]
            if inv.any():
                resultados.append(_aviso(nome, "Consistência", f"Low > {ref} em {inv.sum()} linhas (possível ajuste corporativo)"))

    # Volume negativo
    if "Volume" in df.columns:
        neg_vol = df["Volume"] < 0
        if neg_vol.any():
            resultados.append(_erro(nome, "Consistência", f"Volume negativo em {neg_vol.sum()} linhas"))
        zero_vol = (df["Volume"] == 0).mean() * 100
        if zero_vol > 20:
            resultados.append(_aviso(nome, "Consistência", f"Volume = 0 em {zero_vol:.1f}% das linhas"))

    return resultados


def validar_consistencia_eventos(df: pd.DataFrame, nome: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []

    # Splits: ratio deve ser > 0
    if "type" in df.columns and "value" in df.columns:
        splits = df[df["type"].str.lower().str.contains("split", na=False)]
        if not splits.empty:
            inv_splits = splits[splits["value"] <= 0]
            if not inv_splits.empty:
                resultados.append(_erro(nome, "Consistência", f"{len(inv_splits)} split(s) com valor ≤ 0"))
            else:
                resultados.append(_ok(nome, "Consistência", f"{len(splits)} splits validados (ratio > 0)"))

        # Dividendos: valor deve ser > 0
        divs = df[df["type"].str.lower().str.contains("dividend", na=False)]
        if not divs.empty:
            inv_divs = divs[divs["value"] <= 0]
            if not inv_divs.empty:
                resultados.append(_erro(nome, "Consistência", f"{len(inv_divs)} dividendo(s) com valor ≤ 0"))
            else:
                resultados.append(_ok(nome, "Consistência", f"{len(divs)} dividendos validados (valor > 0)"))

    return resultados


# ===========================================================================
# 3. INTEGRIDADE TEMPORAL
# ===========================================================================

def validar_temporal_precos(
    df: pd.DataFrame,
    nome: str,
    data_inicio: date = DATA_INICIO_PADRAO,
    data_fim: date = DATA_FIM_PADRAO,
) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []

    if "Date" not in df.columns:
        resultados.append(_erro(nome, "Temporal", "Coluna 'Date' ausente — não é possível validar integridade temporal"))
        return resultados

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

    # Datas inválidas (NaT após conversão)
    nat = df["Date"].isna()
    if nat.any():
        resultados.append(_erro(nome, "Temporal", f"{nat.sum()} data(s) inválidas (NaT)"))

    df_valid = df[~nat]

    # Fora do range esperado
    fora_range = df_valid[(df_valid["Date"].dt.date < data_inicio) | (df_valid["Date"].dt.date > data_fim)]
    if not fora_range.empty:
        resultados.append(_aviso(nome, "Temporal", f"{len(fora_range)} linha(s) fora do range {data_inicio} – {data_fim}"))
    else:
        resultados.append(_ok(nome, "Temporal", f"Todas as datas dentro do range {data_inicio} – {data_fim}"))

    # Duplicatas de (ticker, Date)
    if "ticker" in df_valid.columns:
        dupl = df_valid.duplicated(subset=["ticker", "Date"])
        if dupl.any():
            resultados.append(_erro(nome, "Temporal", f"{dupl.sum()} linha(s) duplicadas em (ticker, Date)"))
        else:
            resultados.append(_ok(nome, "Temporal", "Sem duplicatas em (ticker, Date)"))

        # Gaps por ticker
        tickers_com_gap: list[str] = []
        for ticker, grupo in df_valid.groupby("ticker"):
            datas = grupo["Date"].sort_values().dt.date.tolist()
            for i in range(1, len(datas)):
                delta = (datas[i] - datas[i - 1]).days
                # Desconsidera fins de semana (aprox: 5/7 dias úteis)
                if delta > MAX_GAP_PREGAO_DIAS:
                    tickers_com_gap.append(f"{ticker}: gap de {delta}d entre {datas[i-1]} e {datas[i]}")
                    break  # reporta apenas o 1º gap por ticker

        if tickers_com_gap:
            resultados.append(_aviso(nome, "Temporal", f"{len(tickers_com_gap)} ticker(s) com gap > {MAX_GAP_PREGAO_DIAS} dias", tickers_com_gap))
        else:
            resultados.append(_ok(nome, "Temporal", f"Nenhum gap > {MAX_GAP_PREGAO_DIAS} dias detectado"))

    return resultados


def validar_temporal_generica(df: pd.DataFrame, nome: str, col_data: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []
    if col_data not in df.columns:
        resultados.append(_aviso(nome, "Temporal", f"Coluna '{col_data}' não encontrada"))
        return resultados

    datas = pd.to_datetime(df[col_data], errors="coerce")
    nat = datas.isna()
    if nat.any():
        resultados.append(_erro(nome, "Temporal", f"{nat.sum()} data(s) inválidas em '{col_data}'"))
    else:
        resultados.append(_ok(nome, "Temporal", f"Todas as datas em '{col_data}' são válidas"))
    return resultados


# ===========================================================================
# 4. VOLUME DE DADOS
# ===========================================================================

def validar_volume_precos(df: pd.DataFrame, nome: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []
    if "ticker" not in df.columns:
        resultados.append(_aviso(nome, "Volume", "Coluna 'ticker' ausente — pulando validação de volume por ticker"))
        return resultados

    contagem = df.groupby("ticker").size()
    abaixo_min = contagem[contagem < MIN_LINHAS_PRECOS]
    if not abaixo_min.empty:
        detalhes = [f"{t}: {n} linhas (mín. {MIN_LINHAS_PRECOS})" for t, n in abaixo_min.items()]
        resultados.append(_aviso(nome, "Volume", f"{len(abaixo_min)} ticker(s) com menos de {MIN_LINHAS_PRECOS} linhas", detalhes))
    else:
        resultados.append(_ok(nome, "Volume", f"Todos os tickers com ≥ {MIN_LINHAS_PRECOS} linhas de preços"))

    total = len(df)
    n_tickers = df["ticker"].nunique()
    resultados.append(_ok(nome, "Volume", f"Total: {total:,} linhas | {n_tickers} tickers únicos"))
    return resultados


def validar_volume_balanco(df: pd.DataFrame, nome: str) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []
    if "ticker" not in df.columns:
        return resultados

    contagem = df.groupby("ticker").size()
    abaixo_min = contagem[contagem < MIN_LINHAS_BALANCO]
    if not abaixo_min.empty:
        detalhes = [f"{t}: {n} ano(s)" for t, n in abaixo_min.items()]
        resultados.append(_aviso(nome, "Volume", f"{len(abaixo_min)} ticker(s) com menos de {MIN_LINHAS_BALANCO} anos de balanço", detalhes))
    else:
        resultados.append(_ok(nome, "Volume", f"Todos os tickers com ≥ {MIN_LINHAS_BALANCO} anos de balanço"))
    return resultados


def validar_volume_generico(df: pd.DataFrame, nome: str, min_linhas: int = 1) -> list[ValidationResult]:
    resultados: list[ValidationResult] = []
    total = len(df)
    if total < min_linhas:
        resultados.append(_erro(nome, "Volume", f"Dataset com apenas {total} linha(s) — mínimo esperado: {min_linhas}"))
    else:
        resultados.append(_ok(nome, "Volume", f"Dataset com {total:,} linha(s)"))
    return resultados


# ===========================================================================
# ORQUESTRADOR PRINCIPAL
# ===========================================================================

def executar_validacoes(
    data_inicio: date = DATA_INICIO_PADRAO,
    data_fim: date = DATA_FIM_PADRAO,
) -> list[ValidationResult]:
    todos: list[ValidationResult] = []

    # ------------------------------------------------------------------
    # Dataset 1 – Preços OHLCV
    # ------------------------------------------------------------------
    nome_precos = "02_yfinance_precos_raw.csv"
    df_precos = _ler_csv(nome_precos)
    if df_precos is not None:
        todos += validar_completude_precos(df_precos, nome_precos)
        todos += validar_consistencia_precos(df_precos, nome_precos)
        todos += validar_temporal_precos(df_precos, nome_precos, data_inicio, data_fim)
        todos += validar_volume_precos(df_precos, nome_precos)

    # ------------------------------------------------------------------
    # Dataset 2 – Eventos corporativos
    # ------------------------------------------------------------------
    nome_eventos = "03_yfinance_eventos_raw.csv"
    df_eventos = _ler_csv(nome_eventos)
    if df_eventos is not None:
        todos += validar_completude_generica(df_eventos, nome_eventos, COLUNAS_EVENTOS)
        todos += validar_consistencia_eventos(df_eventos, nome_eventos)
        col_data_ev = next((c for c in ["date", "Date", "data"] if c in df_eventos.columns), None)
        if col_data_ev:
            todos += validar_temporal_generica(df_eventos, nome_eventos, col_data_ev)
        todos += validar_volume_generico(df_eventos, nome_eventos, min_linhas=1)

    # ------------------------------------------------------------------
    # Dataset 3 – Metadados / Info
    # ------------------------------------------------------------------
    nome_info = "04_yfinance_info_raw.csv"
    df_info = _ler_csv(nome_info)
    if df_info is not None:
        todos += validar_completude_generica(df_info, nome_info, COLUNAS_INFO)
        todos += validar_volume_generico(df_info, nome_info, min_linhas=1)

    # ------------------------------------------------------------------
    # Dataset 4 – Balanços anuais
    # ------------------------------------------------------------------
    nome_balanco = "05_yfinance_balancos_raw.csv"
    df_balanco = _ler_csv(nome_balanco)
    if df_balanco is not None:
        todos += validar_completude_generica(df_balanco, nome_balanco, COLUNAS_BALANCO)
        col_data_bal = next((c for c in ["year", "date", "Date"] if c in df_balanco.columns), None)
        if col_data_bal:
            todos += validar_temporal_generica(df_balanco, nome_balanco, col_data_bal)
        todos += validar_volume_balanco(df_balanco, nome_balanco)

    return todos


# ===========================================================================
# RELATÓRIO
# ===========================================================================

def imprimir_relatorio(resultados: list[ValidationResult]) -> None:
    print("\n" + "=" * 70)
    print("  RELATÓRIO DE VALIDAÇÃO — PIPELINE YFINANCE / B3")
    print("=" * 70)

    # Agrupa por dataset
    datasets: dict[str, list[ValidationResult]] = {}
    for r in resultados:
        datasets.setdefault(r.dataset, []).append(r)

    erros_total = sum(1 for r in resultados if r.severidade == "ERRO")
    avisos_total = sum(1 for r in resultados if r.severidade == "AVISO")

    for ds, itens in datasets.items():
        print(f"\n📄 {ds}")
        print("-" * 60)
        for item in itens:
            print(item)

    print("\n" + "=" * 70)
    print(f"  RESUMO: {erros_total} erro(s)  |  {avisos_total} aviso(s)  |  {len(resultados)} verificações")
    print("=" * 70 + "\n")

    if erros_total > 0:
        sys.exit(1)   # retorna código de erro para CI/CD ou chamadas externas


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Validação dos dados extraídos pelo pipeline yfinance.")
    parser.add_argument("--inicio", default=str(DATA_INICIO_PADRAO), help="Data de início esperada (YYYY-MM-DD)")
    parser.add_argument("--fim",    default=str(DATA_FIM_PADRAO),    help="Data de fim esperada (YYYY-MM-DD)")
    args = parser.parse_args()

    data_inicio = date.fromisoformat(args.inicio)
    data_fim    = date.fromisoformat(args.fim)

    resultados = executar_validacoes(data_inicio, data_fim)
    imprimir_relatorio(resultados)