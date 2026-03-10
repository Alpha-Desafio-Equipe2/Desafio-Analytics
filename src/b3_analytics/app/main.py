import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import time
from scipy import stats
import yfinance as yf

# --- 1. CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Terminal | B3 Analytics",
    page_icon="▪",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- 2. CSS CUSTOMIZADO (INSTITUCIONAL FLAT) ---
st.markdown("""
<style>
    @import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap");

    html, body, [class*="css"] {
        font-family: "Inter", sans-serif !important;
    }

    .stApp {
        background-color: #131722;
    }

    [data-testid="stSidebar"] {
        background-color: #1e222d !important;
        border-right: 1px solid #2a2e39;
    }

    [data-testid="stMetric"] {
        background-color: #1e222d;
        padding: 16px 20px;
        border-radius: 6px;
        border: 1px solid #2a2e39;
        box-shadow: none;
        transition: border-color 0.2s ease;
    }

    [data-testid="stMetric"]:hover {
        border-color: #2962ff;
    }

    [data-testid="stMetricLabel"] {
        color: #787b86 !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        font-size: 0.75rem !important;
        letter-spacing: 0.5px;
        margin-bottom: 4px;
    }

    [data-testid="stMetricValue"] {
        color: #d1d4dc !important;
        font-weight: 600 !important;
        font-size: 1.8rem !important;
    }

    h1, h2, h3 {
        color: #e0e3eb !important;
        font-weight: 600 !important;
        text-shadow: none !important;
        letter-spacing: -0.5px;
    }

    .explanation {
        background-color: #1e222d;
        padding: 16px;
        border-radius: 4px;
        border-left: 3px solid #2962ff;
        margin-bottom: 24px;
        color: #a3a6af;
        font-size: 0.9rem;
        line-height: 1.5;
    }

    .explanation b {
        color: #d1d4dc;
        font-weight: 600;
    }

    .stTabs [data-baseweb="tab-list"] {
        background-color: transparent;
        border-bottom: 1px solid #2a2e39;
        gap: 32px;
    }

    .stTabs [data-baseweb="tab"] {
        color: #787b86 !important;
        border: none !important;
        background: transparent !important;
        padding-bottom: 12px;
        padding-top: 12px;
        font-weight: 500;
        font-size: 0.95rem;
    }

    .stTabs [aria-selected="true"] {
        color: #2962ff !important;
        border-bottom: 2px solid #2962ff !important;
    }

    #MainMenu { visibility: hidden; }
    footer { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# --- 3. LISTA DE ATIVOS ALVO ---
TICKERS_ALVO = [
    'BBSE3.SA', 'CXSE3.SA', 'PSSA3.SA', 'WIZC3.SA', 'ITUB4.SA', 'BPAC11.SA',
    'BBDC3.SA', 'ITSA4.SA', 'BBAS3.SA', 'SANB11.SA', 'B3SA3.SA', 'MULT3.SA',
    'BPAN4.SA', 'ALOS3.SA', 'BRAP4.SA', 'IGTI3.SA', 'BRSR6.SA', 'ABCB4.SA',
    'SIMH3.SA', 'IRBR3.SA', 'BEES3.SA', 'BMGB4.SA', 'PLPL3.SA', 'PINE4.SA',
    'LOGG3.SA', 'BGIP4.SA', 'SCAR3.SA', 'SYNE3.SA', 'MERC4.SA', 'RPAD3.SA',
    'ESPA3.SA', 'HBRE3.SA'
]

# Configurações de cores unificadas para gráficos
COLOR_UP = '#089981'     # Verde Institucional (TradingView)
COLOR_DOWN = '#F23645'   # Vermelho Institucional
COLOR_PRIMARY = '#2962FF' # Azul Principal
COLOR_BG = 'rgba(0,0,0,0)' # Fundo transparente para herdar o CSS

# --- 4. FUNÇÃO DE CONVERSÃO NUMÉRICA ---
def convert_br_number(val):
    if pd.isna(val) or val == '':
        return np.nan
    try:
        s = str(val).strip().replace('.', '').replace(',', '.')
        return float(s)
    except:
        return np.nan

# --- 5. CARGA E PRÉ‑PROCESSAMENTO DOS DADOS ---
@st.cache_data(ttl=3600)
def load_and_process_data():
    fixed_path = r"C:\Users\user\Desktop\Modelo de exemplo\Desafio-Analytics\data\processed\01_market_data_processed.csv"
    paths = [fixed_path, "data/processed/01_market_data_processed.csv", "./01_market_data_processed.csv"]

    df = None
    for p in paths:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8-sig') as f:
                    first_line = f.readline()
                    sep = ';' if ';' in first_line else ','
                df = pd.read_csv(p, sep=sep, engine='python', encoding='utf-8-sig', dtype=str)
                break
            except Exception as e:
                continue

    if df is None:
        return None

    df.columns = [c.strip().lower() for c in df.columns]
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    numeric_cols = [
        'open', 'high', 'low', 'close', 'volume', 'selic', 'ipca', 'dolar',
        'dividends', 'stock splits', 'retorno_diario', 'log_return', 'retorno_acumulado',
        'volatilidade_21d', 'volatilidade_63d', 'volatilidade_252d', 'sma_21', 'sma_50',
        'sma_200', 'spread_sma50', 'momentum_1m', 'momentum_3m', 'momentum_6m',
        'momentum_12m', 'momentum_anomalia_12m_1m', 'obv', 'vol_medio_20d',
        'volume_relativo', 'marketcap', 'fulltimeemployees', 'cvm_receita_liquida',
        'cvm_ebit', 'cvm_lucro_liquido', 'rsi', 'macd', 'signal_line',
        'bb_upper', 'bb_lower', 'drawdown'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].apply(convert_br_number)

    df_filtered = df[df["ticker"].isin(TICKERS_ALVO)].copy()
    df_filtered = df_filtered.dropna(subset=["date", "close", "ticker"])
    df_filtered = df_filtered.sort_values(["ticker", "date"])

    return df_filtered

with st.spinner("Sincronizando base de dados..."):
    df_full = load_and_process_data()

if df_full is None or df_full.empty:
    st.error("Falha de conexão com a base de dados. Verifique o diretório dos arquivos.")
    st.stop()

# Determina datas mínima e máxima disponíveis
min_date = df_full['date'].min().date()
max_date = df_full['date'].max().date()

# Inicializa o estado da sessão com o período padrão (1 ano)
if 'range_dates' not in st.session_state:
    default_start = max_date - pd.DateOffset(years=1)  # Timestamp
    default_start_date = default_start.date()
    if default_start_date < min_date:
        default_start_date = min_date
    st.session_state.range_dates = [default_start_date, max_date]

# --- 6. SIDEBAR – CONTROLES ---
with st.sidebar:
    st.markdown("<h2 style='text-align: left; font-size: 1.2rem; color: #D1D4DC;'>TERMINAL DE ANÁLISE</h2>", unsafe_allow_html=True)
    st.markdown("<div style='height: 1px; background-color: #2A2E39; margin-bottom: 20px;'></div>", unsafe_allow_html=True)

    main_ticker = st.selectbox("ATIVO PRINCIPAL", sorted(df_full['ticker'].unique()))

    selected_tickers = st.multiselect("BENCHMARK (COMPARAÇÃO)",
                                      sorted(df_full['ticker'].unique()),
                                      default=[main_ticker, 'ITUB4.SA', 'BBDC3.SA'][:3])
    if main_ticker not in selected_tickers:
        selected_tickers.append(main_ticker)

    st.markdown("<div style='margin-top: 20px; color: #787B86; font-size: 0.8rem; font-weight: 500;'>HORIZONTE TEMPORAL</div>", unsafe_allow_html=True)

    # Input de data (início e fim)
    date_range = st.date_input(
        "Selecione o período",
        value=st.session_state.range_dates,
        min_value=min_date,
        max_value=max_date,
        label_visibility="collapsed"
    )
    if len(date_range) == 2:
        st.session_state.range_dates = date_range

    # Botões de atalho
    st.markdown("<div style='margin: 10px 0 5px; color:#787B86; font-size:0.8rem;'>ATALHOS</div>", unsafe_allow_html=True)
    col_a, col_b, col_c, col_d, col_e = st.columns(5)
    with col_a:
        if st.button("10a"):
            start = (max_date - pd.DateOffset(years=10)).date()
            if start < min_date:
                start = min_date
            st.session_state.range_dates = [start, max_date]
            st.rerun()
    with col_b:
        if st.button("5a"):
            start = (max_date - pd.DateOffset(years=5)).date()
            if start < min_date:
                start = min_date
            st.session_state.range_dates = [start, max_date]
            st.rerun()
    with col_c:
        if st.button("1a"):
            start = (max_date - pd.DateOffset(years=1)).date()
            if start < min_date:
                start = min_date
            st.session_state.range_dates = [start, max_date]
            st.rerun()
    with col_d:
        if st.button("6m"):
            start = (max_date - pd.DateOffset(months=6)).date()
            if start < min_date:
                start = min_date
            st.session_state.range_dates = [start, max_date]
            st.rerun()
    with col_e:
        if st.button("Tudo"):
            st.session_state.range_dates = [min_date, max_date]
            st.rerun()

# Aplica o filtro de data
start_date, end_date = st.session_state.range_dates
df = df_full[(df_full['date'].dt.date >= start_date) & (df_full['date'].dt.date <= end_date)].copy()

df_main = df[df['ticker'] == main_ticker].copy()

# --- 7. FUNÇÕES AUXILIARES PARA CÁLCULOS ---
@st.cache_data
def calc_retornos_acumulados(df, tickers):
    df_ret = df[df['ticker'].isin(tickers)].copy()
    df_ret = df_ret.sort_values(['ticker', 'date'])
    df_ret['retorno_diario'] = df_ret.groupby('ticker')['close'].pct_change()
    df_ret['ret_acum'] = df_ret.groupby('ticker')['retorno_diario'].transform(lambda x: (1 + x).cumprod() - 1)
    return df_ret

@st.cache_data
def calc_risco_retorno(df, tickers, min_obs=60):
    resultados = []
    for ticker in tickers:
        df_t = df[df['ticker'] == ticker].sort_values('date').copy()
        if len(df_t) < min_obs:
            continue
        rets = df_t['retorno_diario'].dropna()
        if len(rets) < min_obs:
            continue
        ret_medio = rets.mean()
        ret_anual = (1 + ret_medio) ** 252 - 1
        vol_diaria = rets.std()
        vol_anual = vol_diaria * np.sqrt(252)
        if vol_anual < 0.001:
            continue
        sharpe = ret_anual / vol_anual
        resultados.append({
            'ticker': ticker,
            'retorno_anual': ret_anual * 100,
            'volatilidade_anual': vol_anual * 100,
            'sharpe_ratio': sharpe,
            'n_obs': len(rets)
        })
    return pd.DataFrame(resultados)

@st.cache_data
def calc_betas_ibov(df, tickers):
    """Calcula beta em relação ao Ibovespa usando yfinance."""
    # Baixa dados do Ibovespa
    ibov_raw = yf.download("^BVSP", start=df['date'].min(), end=df['date'].max(), progress=False)
    if ibov_raw.empty:
        return pd.DataFrame(columns=['ticker', 'beta'])
    ibov = ibov_raw['Close'].squeeze()
    ibov_ret = ibov.pct_change().dropna()
    ibov_ret.name = "ibov"

    # Pivot dos retornos dos ativos
    retornos = df.pivot(index='date', columns='ticker', values='close').pct_change().dropna(how='all')
    # Alinha datas
    common_dates = retornos.index.intersection(ibov_ret.index)
    retornos = retornos.loc[common_dates]
    ibov_ret = ibov_ret.loc[common_dates]

    betas = {}
    for ticker in tickers:
        if ticker not in retornos.columns:
            continue
        # Remove linhas com NaN em ambos
        df_combined = pd.concat([retornos[ticker], ibov_ret], axis=1).dropna()
        if len(df_combined) < 30:
            continue
        cov = np.cov(df_combined.iloc[:, 0], df_combined.iloc[:, 1])[0, 1]
        var = np.var(df_combined.iloc[:, 1])
        betas[ticker] = cov / var
    return pd.DataFrame(list(betas.items()), columns=['ticker', 'beta']).sort_values('beta')

# --- 8. ABAS PRINCIPAIS ---
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "ANÁLISE ESTRUTURAL",
    "DESEMPENHO RELATIVO",
    "MATRIZ DE RISCO",
    "FUNDAMENTOS",
    "SCREENER DE MERCADO"
])

# ==========================================================================
# ABA 1 – ANÁLISE ESTRUTURAL (ENRIQUECIDA)
# ==========================================================================
with tab1:
    st.title(f"{main_ticker} | Visão Técnica")

    st.markdown("""
    <div class='explanation'>
    <b>NOTA TÉCNICA:</b> Ação de preço estruturada com médias móveis (SMA 21, 50, 200).
    Volume financeiro com destaque nas barras (vermelho = volume > 2σ da média). 
    Índice de Força Relativa (RSI) e Drawdown acumulado disponíveis nos sub‑gráficos para análise de exaustão de tendência.
    </div>
    """, unsafe_allow_html=True)

    if df_main.empty:
        st.warning("Sem dados para o período selecionado.")
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    last_close = df_main['close'].iloc[-1]
    prev_close = df_main['close'].iloc[-2] if len(df_main) > 1 else last_close
    ret_daily = ((last_close / prev_close) - 1) * 100

    col1.metric("ÚLTIMO FECHAMENTO", f"R$ {last_close:,.2f}", f"{ret_daily:+.2f}%")

    if 'volatilidade_21d' in df_main.columns:
        vol_21d = df_main['volatilidade_21d'].dropna().iloc[-1] * 100 if not df_main['volatilidade_21d'].dropna().empty else 0
        col2.metric("VOLATILIDADE (21D)", f"{vol_21d:.1f}%")

    if 'volume_relativo' in df_main.columns:
        vol_rel = df_main['volume_relativo'].iloc[-1]
        col3.metric("VOLUME RELATIVO", f"{vol_rel:.2f}x")

    if 'tendencia_alta_50_200' in df_main.columns:
        tend = df_main['tendencia_alta_50_200'].iloc[-1]
        estado = "Alta" if tend == 1 else ("Baixa" if tend == 0 else "Neutra")
        col4.metric("TENDÊNCIA (50/200)", estado)

    # --- Gráfico principal com indicadores avançados ---
    # Pré‑cálculo de volume spikes
    df_main = df_main.sort_values('date')
    df_main['vol_ma20'] = df_main['volume'].rolling(20, min_periods=5).mean()
    df_main['vol_std20'] = df_main['volume'].rolling(20, min_periods=5).std()
    df_main['vol_spike'] = (df_main['volume'] > df_main['vol_ma20'] + 2 * df_main['vol_std20']).fillna(False)
    colors_volume = [COLOR_DOWN if spike else '#78909C' for spike in df_main['vol_spike']]

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.4, 0.2, 0.2, 0.2],
        subplot_titles=("Preço e Médias", "Volume (vermelho = spike ≥ 2σ)", "RSI (14)", "Drawdown %")
    )

    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df_main['date'], open=df_main['open'], high=df_main['high'],
        low=df_main['low'], close=df_main['close'], name='Price',
        increasing_line_color=COLOR_UP, decreasing_line_color=COLOR_DOWN,
        showlegend=False
    ), row=1, col=1)

    # Médias móveis
    if 'sma_21' in df_main.columns:
        fig.add_trace(go.Scatter(x=df_main['date'], y=df_main['sma_21'],
                                  line=dict(color='#FF9800', width=1.5), name='SMA 21'), row=1, col=1)
    if 'sma_50' in df_main.columns:
        fig.add_trace(go.Scatter(x=df_main['date'], y=df_main['sma_50'],
                                  line=dict(color=COLOR_PRIMARY, width=1.5), name='SMA 50'), row=1, col=1)
    if 'sma_200' in df_main.columns:
        fig.add_trace(go.Scatter(x=df_main['date'], y=df_main['sma_200'],
                                  line=dict(color='#9598A1', width=1.5, dash='dash'), name='SMA 200'), row=1, col=1)

    # Volume com spikes coloridos
    fig.add_trace(go.Bar(x=df_main['date'], y=df_main['volume'],
                         marker_color=colors_volume, opacity=0.7, name='Volume'), row=2, col=1)

    # RSI com zonas preenchidas
    if 'rsi' in df_main.columns:
        fig.add_trace(go.Scatter(x=df_main['date'], y=df_main['rsi'],
                                  line=dict(color='#FF9800', width=1.5), name='RSI'), row=3, col=1)
        # Zonas de sobrecompra/sobrevenda
        fig.add_hrect(y0=70, y1=100, line_width=0, fillcolor="rgba(255,0,0,0.07)", row=3, col=1)
        fig.add_hrect(y0=0,  y1=30,  line_width=0, fillcolor="rgba(0,200,0,0.07)", row=3, col=1)
        fig.add_hline(y=70, line_dash="dash", line_color="#9598A1", row=3, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="#9598A1", row=3, col=1)
        fig.update_yaxes(title_text="RSI", range=[0, 100], row=3, col=1)

    # Drawdown
    if 'drawdown' in df_main.columns:
        fig.add_trace(go.Scatter(
            x=df_main['date'],
            y=df_main['drawdown']*100,
            mode='lines',
            line=dict(color=COLOR_DOWN, width=1.5),
            fill='tozeroy',
            fillcolor='rgba(242, 54, 69, 0.1)',
            name='Drawdown %'
        ), row=4, col=1)
        fig.update_yaxes(title_text="DD %", tickformat=".1f", row=4, col=1)

    fig.update_layout(
        template='plotly_dark',
        paper_bgcolor=COLOR_BG,
        plot_bgcolor=COLOR_BG,
        height=950,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=0, r=0, t=40, b=0)
    )
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
    st.plotly_chart(fig, use_container_width=True)

    # --- MACD e Bollinger (já existentes) ---
    st.markdown("<br>", unsafe_allow_html=True)
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("<div style='color: #787B86; font-size: 0.85rem; font-weight: 600; margin-bottom: 10px;'>CONVERGÊNCIA/DIVERGÊNCIA DE MÉDIAS (MACD)</div>", unsafe_allow_html=True)
        if all(col in df_main.columns for col in ['macd', 'signal_line']):
            macd_data = df_main[['date', 'macd', 'signal_line']].dropna()
        else:
            close = df_main.set_index('date')['close']
            ema12 = close.ewm(span=12, adjust=False).mean()
            ema26 = close.ewm(span=26, adjust=False).mean()
            macd_line = ema12 - ema26
            signal_line = macd_line.ewm(span=9, adjust=False).mean()
            macd_data = pd.DataFrame({'date': macd_line.index, 'macd': macd_line.values, 'signal_line': signal_line.values}).dropna()

        if not macd_data.empty:
            fig_macd = go.Figure()
            fig_macd.add_trace(go.Scatter(x=macd_data['date'], y=macd_data['macd'], name='MACD', line=dict(color=COLOR_PRIMARY)))
            fig_macd.add_trace(go.Scatter(x=macd_data['date'], y=macd_data['signal_line'], name='Signal', line=dict(color='#FF9800')))
            fig_macd.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=300, margin=dict(l=0, r=0, t=10, b=0))
            fig_macd.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
            fig_macd.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
            st.plotly_chart(fig_macd, use_container_width=True)
        else:
            st.info("Dados insuficientes para MACD.")

    with col_b:
        st.markdown("<div style='color: #787B86; font-size: 0.85rem; font-weight: 600; margin-bottom: 10px;'>BANDAS DE BOLLINGER (VOLATILIDADE)</div>", unsafe_allow_html=True)
        if all(col in df_main.columns for col in ['bb_upper', 'bb_lower', 'sma_21']):
            fig_bb = go.Figure()
            fig_bb.add_trace(go.Scatter(x=df_main['date'], y=df_main['close'], name='Preço', line=dict(color='#D1D4DC')))
            fig_bb.add_trace(go.Scatter(x=df_main['date'], y=df_main['bb_upper'], name='Banda Sup.', line=dict(color='#434651', dash='dash')))
            fig_bb.add_trace(go.Scatter(x=df_main['date'], y=df_main['bb_lower'], name='Banda Inf.', line=dict(color='#434651', dash='dash')))
            fig_bb.add_trace(go.Scatter(x=df_main['date'], y=df_main['sma_21'], name='SMA 21', line=dict(color='#FF9800', width=1)))
            fig_bb.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=300, margin=dict(l=0, r=0, t=10, b=0))
            fig_bb.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
            fig_bb.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
            st.plotly_chart(fig_bb, use_container_width=True)

    # --- VWAP com bandas de desvio ---
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<div style='color: #787B86; font-size: 0.85rem; font-weight: 600; margin-bottom: 10px;'>VWAP COM BANDA DE ±1σ</div>", unsafe_allow_html=True)

    def calculate_vwap_bands(df, ticker):
        df_t = df[df['ticker'] == ticker].sort_values('date').copy()
        df_t['typical_price'] = (df_t['high'] + df_t['low'] + df_t['close']) / 3
        df_t['pv'] = df_t['typical_price'] * df_t['volume']
        df_t['cum_pv'] = df_t['pv'].cumsum()
        df_t['cum_vol'] = df_t['volume'].cumsum()
        df_t['vwap'] = df_t['cum_pv'] / df_t['cum_vol']
        df_t['vwap_std'] = (df_t['close'] - df_t['vwap']).expanding().std()
        return df_t

    df_vwap = calculate_vwap_bands(df, main_ticker)
    if not df_vwap.empty:
        fig_vwap = go.Figure()
        # Banda ±1σ
        fig_vwap.add_trace(go.Scatter(
            x=pd.concat([df_vwap['date'], df_vwap['date'].iloc[::-1]]),
            y=pd.concat([df_vwap['vwap'] + df_vwap['vwap_std'], (df_vwap['vwap'] - df_vwap['vwap_std']).iloc[::-1]]),
            fill='toself', fillcolor='rgba(255,165,0,0.12)',
            line=dict(color='rgba(255,165,0,0)'), name='±1σ VWAP', hoverinfo='skip'
        ))
        fig_vwap.add_trace(go.Scatter(x=df_vwap['date'], y=df_vwap['close'], mode='lines', name='Preço', line=dict(color='#D1D4DC')))
        fig_vwap.add_trace(go.Scatter(x=df_vwap['date'], y=df_vwap['vwap'], mode='lines', name='VWAP', line=dict(dash='dash', color=COLOR_PRIMARY)))
        fig_vwap.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=400, margin=dict(l=0, r=0, t=10, b=0))
        fig_vwap.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_vwap.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_vwap, use_container_width=True)
    else:
        st.info("Dados insuficientes para VWAP.")

# ==========================================================================
# ABA 2 – DESEMPENHO RELATIVO (mantido, apenas pequenos ajustes)
# ==========================================================================
with tab2:
    st.title("Desempenho Relativo & Benchmark")

    st.subheader("Retorno Acumulado Base 100")
    df_ret_acum = calc_retornos_acumulados(df, selected_tickers)
    if not df_ret_acum.empty:
        fig_ret = px.line(
            df_ret_acum, x='date', y='ret_acum', color='ticker',
            labels={'ret_acum': 'Performance', 'date': ''}
        )
        fig_ret.update_yaxes(tickformat=".1%")
        fig_ret.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=450)
        fig_ret.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_ret.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_ret, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Série Histórica de Preços")
    df_hist = df[df['ticker'].isin(selected_tickers)].copy()
    fig_hist = px.line(df_hist, x='date', y='close', color='ticker', labels={'close': 'Preço (R$)', 'date': ''})
    fig_hist.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=450)
    fig_hist.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
    fig_hist.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
    st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Volatilidade Comparativa")
    df_vol = df[df['ticker'].isin(selected_tickers)].copy()
    if all(col in df_vol.columns for col in ['volatilidade_21d', 'volatilidade_252d']):
        fig_vol = make_subplots(rows=2, cols=1, shared_xaxes=True, subplot_titles=('Vol 21d (curto prazo)', 'Vol 252d (longo prazo)'))
        for ticker in selected_tickers:
            df_t = df_vol[df_vol['ticker'] == ticker]
            fig_vol.add_trace(go.Scatter(x=df_t['date'], y=df_t['volatilidade_21d']*100, name=ticker), row=1, col=1)
            fig_vol.add_trace(go.Scatter(x=df_t['date'], y=df_t['volatilidade_252d']*100, name=ticker, showlegend=False), row=2, col=1)
        fig_vol.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=600)
        fig_vol.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_vol.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Relação com Indicadores Macroeconômicos")
    if not df_main.empty:
        def norm(series):
            mn, mx = series.min(), series.max()
            if pd.isna(mn) or pd.isna(mx) or mx == mn:
                return pd.Series(0.5, index=series.index)
            return (series - mn) / (mx - mn)

        df_norm = df_main.copy()
        series_to_plot = []
        if 'close' in df_norm.columns:
            df_norm['close_norm'] = norm(df_norm['close'])
            series_to_plot.append(('close_norm', main_ticker, COLOR_PRIMARY, 3, 'solid'))
        if 'selic' in df_norm.columns and df_norm['selic'].notna().any():
            df_norm['selic_norm'] = norm(df_norm['selic'])
            series_to_plot.append(('selic_norm', 'SELIC', '#D1D4DC', 1.5, 'dot'))
        if 'ipca' in df_norm.columns and df_norm['ipca'].notna().any():
            df_norm['ipca_norm'] = norm(df_norm['ipca'])
            series_to_plot.append(('ipca_norm', 'IPCA', COLOR_DOWN, 1.5, 'dash'))

        dolar_col = next((c for c in ['usd_brl', 'dolar', 'USD_BRL'] if c in df_norm.columns), None)
        if dolar_col and df_norm[dolar_col].notna().any():
            df_norm['dolar_norm'] = norm(df_norm[dolar_col])
            series_to_plot.append(('dolar_norm', 'Dólar', COLOR_UP, 1.5, 'dashdot'))

        fig_macro = go.Figure()
        for col, name, color, width, dash in series_to_plot:
            fig_macro.add_trace(go.Scatter(x=df_norm['date'], y=df_norm[col], name=name,
                                            line=dict(color=color, width=width, dash=dash)))
        fig_macro.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG,
                                height=450, yaxis_title="Escala Normalizada (0-1)")
        fig_macro.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_macro.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_macro, use_container_width=True)

# ==========================================================================
# ABA 3 – MATRIZ DE RISCO (ENRIQUECIDA)
# ==========================================================================
with tab3:
    st.title("Matriz de Risco e Distribuição")

    # Risco vs Retorno aprimorado com quadrantes e tendência
    df_risco = calc_risco_retorno(df, selected_tickers)
    if not df_risco.empty:
        # Classificação em quadrantes
        med_ret = df_risco['retorno_anual'].median()
        med_vol = df_risco['volatilidade_anual'].median()

        def classificar(row):
            if row['retorno_anual'] >= med_ret and row['volatilidade_anual'] <= med_vol:
                return '⭐ Ideal'
            elif row['retorno_anual'] >= med_ret:
                return '⚡ Agressivo'
            elif row['volatilidade_anual'] <= med_vol:
                return '🛡️ Defensivo'
            else:
                return '❌ Evitar'
        df_risco['classe'] = df_risco.apply(classificar, axis=1)

        fig_risco = px.scatter(
            df_risco, x='volatilidade_anual', y='retorno_anual', text='ticker',
            size=df_risco['sharpe_ratio'].abs() + 0.1, color='sharpe_ratio',
            color_continuous_scale='RdYlGn',
            hover_data={'classe': True, 'sharpe_ratio': ':.2f'},
            labels={'volatilidade_anual': 'Risco (Vol %)', 'retorno_anual': 'Retorno Anual %'}
        )
        fig_risco.update_traces(textposition='top center')

        # Linhas de mediana
        fig_risco.add_hline(y=med_ret, line_dash='dot', line_color='gray',
                            annotation_text=f'Mediana ret: {med_ret:.1f}%', annotation_position='right')
        fig_risco.add_vline(x=med_vol, line_dash='dot', line_color='gray',
                            annotation_text=f'Mediana vol: {med_vol:.1f}%', annotation_position='top')

        # Linha de tendência linear
        x = df_risco['volatilidade_anual'].values
        y = df_risco['retorno_anual'].values
        slope, intercept, r_value, *_ = stats.linregress(x, y)
        x_trend = np.linspace(x.min(), x.max(), 100)
        y_trend = slope * x_trend + intercept
        fig_risco.add_trace(go.Scatter(
            x=x_trend, y=y_trend, mode='lines', name=f'Tendência (R²={r_value**2:.2f})',
            line=dict(dash='dash', color='white', width=1.5)
        ))

        fig_risco.update_layout(
            template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG,
            height=650, coloraxis_colorbar=dict(title='Sharpe')
        )
        fig_risco.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_risco.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_risco, use_container_width=True)

        st.write("Classificação dos ativos:")
        st.dataframe(df_risco[['ticker', 'retorno_anual', 'volatilidade_anual', 'sharpe_ratio', 'classe']]
                     .style.format({'retorno_anual': '{:.1f}%', 'volatilidade_anual': '{:.1f}%', 'sharpe_ratio': '{:.2f}'}),
                     use_container_width=True, hide_index=True)
    else:
        st.warning("Não há dados suficientes para calcular risco/retorno.")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Heatmap de Correlação")
    heatmap_tickers = st.multiselect("Ativos para correlação:", sorted(df['ticker'].unique()), default=selected_tickers[:5])
    if len(heatmap_tickers) >= 2:
        retornos_heat = df[df['ticker'].isin(heatmap_tickers)].pivot(index='date', columns='ticker', values='close').pct_change().dropna()
        corr = retornos_heat.corr()
        fig_corr = px.imshow(corr, text_auto='.2f', aspect='auto', color_continuous_scale='RdBu_r', zmin=-1, zmax=1)
        fig_corr.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=600)
        st.plotly_chart(fig_corr, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Beta vs. Ibovespa")
    with st.spinner("Calculando betas com dados do Ibovespa..."):
        df_beta = calc_betas_ibov(df, selected_tickers)
    if not df_beta.empty:
        fig_beta = px.bar(df_beta, x='ticker', y='beta', color='beta', color_continuous_scale='RdBu_r')
        fig_beta.add_hline(y=1, line_dash='dash', line_color='gray', annotation_text='β = 1')
        fig_beta.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=450)
        fig_beta.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_beta.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_beta, use_container_width=True)
    else:
        st.info("Não foi possível calcular betas (poucos dados em comum com Ibovespa).")

    st.markdown("<br>", unsafe_allow_html=True)
    st.subheader("Fronteira Eficiente (Simulação de Portfólio)")
    ef_tickers = st.multiselect("Ativos para fronteira:", sorted(df['ticker'].unique()), default=selected_tickers[:3], key='ef')
    if len(ef_tickers) >= 2:
        retornos_ef = df[df['ticker'].isin(ef_tickers)].pivot(index='date', columns='ticker', values='close').pct_change().dropna()
        ret_medio = retornos_ef.mean() * 252
        cov = retornos_ef.cov() * 252
        n_ativos = len(ef_tickers)
        num_port = 3000
        results = np.zeros((3, num_port))
        for i in range(num_port):
            pesos = np.random.random(n_ativos)
            pesos /= pesos.sum()
            ret_port = np.sum(ret_medio * pesos)
            risco_port = np.sqrt(np.dot(pesos.T, np.dot(cov, pesos)))
            results[0, i] = risco_port
            results[1, i] = ret_port
            results[2, i] = ret_port / risco_port
        df_ef = pd.DataFrame({'Risco': results[0], 'Retorno': results[1]*100, 'Sharpe': results[2]})
        fig_ef = px.scatter(df_ef, x='Risco', y='Retorno', color='Sharpe', title='')
        fig_ef.update_layout(template='plotly_dark', paper_bgcolor=COLOR_BG, plot_bgcolor=COLOR_BG, height=500)
        fig_ef.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        fig_ef.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#2A2E39')
        st.plotly_chart(fig_ef, use_container_width=True)

# ==========================================================================
# ABA 4 – FUNDAMENTOS (mantido)
# ==========================================================================
with tab4:
    st.title("Dados Estruturais (CVM)")

    df_fund = df[df['ticker'].isin(selected_tickers)].sort_values('date').groupby('ticker').last().reset_index()
    cols_fund = ['ticker', 'sector', 'industry', 'marketcap',
                 'cvm_receita_liquida', 'cvm_ebit', 'cvm_lucro_liquido', 'fulltimeemployees']
    exist_fund = [c for c in cols_fund if c in df_fund.columns]
    if len(exist_fund) > 1:
        df_display = df_fund[exist_fund].copy()
        for col in ['marketcap', 'cvm_receita_liquida', 'cvm_ebit', 'cvm_lucro_liquido']:
            if col in df_display.columns:
                df_display[col] = df_display[col].apply(
                    lambda x: f"R$ {x/1e9:.2f} Bi" if pd.notna(x) and x != 0 else "-"
                )
        if 'fulltimeemployees' in df_display.columns:
            df_display['fulltimeemployees'] = df_display['fulltimeemployees'].apply(
                lambda x: f"{int(x):,}".replace(',', '.') if pd.notna(x) else "-"
            )
        rename_map = {
            'ticker': 'Ativo', 'sector': 'Setor', 'industry': 'Indústria',
            'marketcap': 'Valor de Mercado', 'cvm_receita_liquida': 'Receita Líquida',
            'cvm_ebit': 'EBIT', 'cvm_lucro_liquido': 'Lucro Líquido',
            'fulltimeemployees': 'Headcount'
        }
        df_display.rename(columns=rename_map, inplace=True)
        st.dataframe(df_display, use_container_width=True, hide_index=True)

# ==========================================================================
# ABA 5 – SCREENER DE MERCADO (mantido)
# ==========================================================================
with tab5:
    st.title("Screener Quantitativo")

    df_last = df.groupby('ticker').last().reset_index()
    metricas = ['ticker', 'close', 'retorno_diario', 'volatilidade_21d',
                'momentum_1m', 'momentum_6m', 'rsi', 'volume_relativo']

    exist_metricas = [c for c in metricas if c in df_last.columns]
    if len(exist_metricas) > 1:
        df_screener = df_last[exist_metricas].copy()

        # Formatando
        if 'close' in df_screener.columns:
            df_screener['close'] = df_screener['close'].apply(lambda x: f"R$ {x:,.2f}")
        if 'retorno_diario' in df_screener.columns:
            df_screener['retorno_diario'] = df_screener['retorno_diario'].apply(lambda x: f"{x*100:+.2f}%")
        if 'volatilidade_21d' in df_screener.columns:
            df_screener['volatilidade_21d'] = df_screener['volatilidade_21d'].apply(lambda x: f"{x*100:.2f}%")
        for col in ['momentum_1m', 'momentum_6m']:
            if col in df_screener.columns:
                df_screener[col] = df_screener[col].apply(lambda x: f"{x*100:+.2f}%")
        if 'rsi' in df_screener.columns:
            df_screener['rsi'] = df_screener['rsi'].apply(lambda x: f"{x:.1f}")
        if 'volume_relativo' in df_screener.columns:
            df_screener['volume_relativo'] = df_screener['volume_relativo'].apply(lambda x: f"{x:.2f}x")

        rename_screener = {
            'ticker': 'ATIVO', 'close': 'PREÇO', 'retorno_diario': 'VARIAÇÃO (1D)',
            'volatilidade_21d': 'VOLATILIDADE', 'momentum_1m': 'MOMENTUM 1M',
            'momentum_6m': 'MOMENTUM 6M', 'rsi': 'RSI', 'volume_relativo': 'VOL. RELATIVO'
        }
        df_screener.rename(columns=rename_screener, inplace=True)
        st.dataframe(df_screener, use_container_width=True, hide_index=True, height=600)

# --- 9. RODAPÉ ---
st.markdown("---")
st.markdown(
    "<center><p style='color: #787B86; font-size: 12px;'>"
    "Terminal Financeiro B3 • Todos os gráficos do notebook integrados • "
    "Desenvolvido com Streamlit e Plotly"
    "</p></center>",
    unsafe_allow_html=True
)