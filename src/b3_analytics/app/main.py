import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from datetime import datetime
from pathlib import Path

try:
    from streamlit_option_menu import option_menu
except ImportError:
    st.error("❌ Módulo 'streamlit-option-menu' não encontrado. Por favor, execute no terminal: pip install streamlit-option-menu")
    st.stop()

# Tentativa de importar caminhos do projeto (com fallback seguro)
try:
    from b3_analytics.utils.paths import RAW_DIR, PROCESSED_DIR
except ImportError:
    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    RAW_DIR = BASE_DIR / 'data' / 'raw'
    PROCESSED_DIR = BASE_DIR / 'data' / 'processed'

# --- 1. CONFIGURAÇÃO DA PÁGINA E CSS PREMIUM ---
st.set_page_config(
    page_title="Alpha-IA | Terminal B3",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* O SEU CSS DE GLASSMORPHISM (Efeito Terminal Financeiro) */
    .stMetric {
        background: rgba(255, 255, 255, 0.03);
        padding: 20px;
        border-radius: 15px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
        backdrop-filter: blur(12px);
        transition: transform 0.3s ease, border-color 0.3s ease;
    }
    .stMetric:hover {
        transform: translateY(-5px);
        border-color: #00f2ff;
    }
    
    /* Custom Header Styling Neon */
    h1 {
        color: #00f2ff !important;
        font-size: 2.8rem !important;
        text-shadow: 0 0 20px rgba(0, 242, 255, 0.5);
        margin-bottom: 20px;
    }
    h2, h3 { color: #e0e0e0 !important; }
    
    /* Cards de Insight / Explicações (Storytelling) */
    .insight-card {
        background: linear-gradient(135deg, rgba(0, 242, 255, 0.15) 0%, rgba(0, 0, 0, 0.4) 100%);
        padding: 25px;
        border-radius: 15px;
        border-left: 6px solid #00f2ff;
        margin-bottom: 25px;
        font-size: 1.1rem;
        line-height: 1.6;
        color: #e0e0e0;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
    }
    
    .warning-card {
        background: linear-gradient(135deg, rgba(255, 75, 75, 0.15) 0%, rgba(0, 0, 0, 0.4) 100%);
        border-left: 6px solid #ff4b4b;
    }
    
    /* Hide Streamlit clutter */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Glowing Deltas nas Métricas */
    [data-testid="stMetricDelta"] > div:nth-child(2) svg {
        filter: drop-shadow(0 0 8px currentColor);
    }
</style>
""", unsafe_allow_html=True)

# --- 2. MOTOR DE CARGA DE DADOS (CACHED) ---
@st.cache_data
def load_data():
    """Lê os dados processados e resolve problemas de formatação."""
    # Prioridade: Parquet Processado
    data_path = PROCESSED_DIR / '01_market_data_processed.parquet'
    if data_path.exists():
        df = pd.read_parquet(data_path)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'data' in df.columns:
            df['date'] = pd.to_datetime(df['data'])
        return df
    
    # Fallback: CSV Raw (Caso o Squad 2 ainda não tenha gerado o Parquet)
    raw_path = RAW_DIR / '01_yfinance_precos_raw.csv'
    if raw_path.exists():
        df = pd.read_csv(raw_path, sep=';', decimal=',')
        df.columns = [c.lower().strip() for c in df.columns]
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        elif 'data' in df.columns:
            df['date'] = pd.to_datetime(df['data'])
        return df
        
    return None

df_master = load_data()

# --- 3. BARRA LATERAL (CONTROLES E FILTROS) ---
with st.sidebar:
    st.markdown("<h2 style='text-align: center; color: #00f2ff; text-shadow: 0 0 10px #00f2ff;'>Comando Alpha</h2>", unsafe_allow_html=True)
    st.markdown("---")
    
    if df_master is not None:
        todos_tickers = sorted(df_master['ticker'].unique())
        
        
        selected_ticker = st.selectbox("🎯 Selecione o Ativo Principal", todos_tickers, index=0)
        
        st.markdown("### 📅 Horizonte Temporal")
        date_min, date_max = df_master['date'].min(), df_master['date'].max()
        # Default: Últimos 2 anos para os gráficos não ficarem poluídos logo de cara
        default_start = date_max - pd.DateOffset(years=2) 
        if default_start < date_min: default_start = date_min
        
        date_range = st.date_input("Intervalo de Análise", [default_start, date_max], min_value=date_min, max_value=date_max)
        
        # Recálculo Dinâmico (Magia para a apresentação!)
        st.markdown("---")
        st.markdown("### ⚙️ Calibragem Técnica Algorítmica")
        st.caption("Ajuste os parâmetros para ver a IA recalcular ao vivo.")
        sma_curta = st.slider("Média Móvel Rápida (Dias)", 5, 50, 21)
        sma_longa = st.slider("Média Móvel Lenta (Dias)", 50, 250, 200)
    else:
        st.error("⚠️ Base de dados não encontrada.")
        st.stop()

# Filtra o DataFrame Mestre pelo Ticker e Data selecionada
if len(date_range) == 2:
    start_date, end_date = pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])
    df_ticker = df_master[(df_master['ticker'] == selected_ticker) & (df_master['date'] >= start_date) & (df_master['date'] <= end_date)].copy()
else:
    df_ticker = df_master[df_master['ticker'] == selected_ticker].copy()

# --- 4. CÁLCULO DINÂMICO DE INDICADORES (On the fly) ---
df_ticker = df_ticker.sort_values('date')
df_ticker[f'SMA_{sma_curta}'] = df_ticker['close'].rolling(sma_curta).mean()
df_ticker[f'SMA_{sma_longa}'] = df_ticker['close'].rolling(sma_longa).mean()

# Bandas de Bollinger Dinâmicas (A volatilidade que o preço aguenta)
std = df_ticker['close'].rolling(20).std()
df_ticker['BB_Upper'] = df_ticker['close'].rolling(20).mean() + (std * 2)
df_ticker['BB_Lower'] = df_ticker['close'].rolling(20).mean() - (std * 2)

# --- 5. NAVEGAÇÃO SUPERIOR (Menu Bonito) ---
selected_tab = option_menu(
    menu_title=None,
    options=["Visão Geral", "Raio-X Técnico", "Fundamentos & Macro", "Radar Institucional"],
    icons=["tv", "cpu", "globe2", "radar"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "transparent", "margin-bottom": "20px"},
        "icon": {"color": "#00f2ff", "font-size": "22px"},
        "nav-link": {"font-size": "18px", "text-align": "center", "margin": "0px", "--hover-color": "rgba(0, 242, 255, 0.1)", "font-weight": "bold"},
        "nav-link-selected": {"background-color": "rgba(0, 242, 255, 0.2)", "color": "#00f2ff", "border-bottom": "3px solid #00f2ff"},
    }
)

# === VARIAVEIS DE STATUS RÁPIDO PARA OS CARDS ===
last_close = df_ticker['close'].iloc[-1] if not df_ticker.empty else 0
prev_close = df_ticker['close'].iloc[-2] if len(df_ticker) > 1 else last_close
delta_pct = ((last_close / prev_close) - 1) * 100 if prev_close != 0 else 0

rsi_val = df_ticker['rsi_14'].iloc[-1] if 'rsi_14' in df_ticker.columns else 50
vol_val = df_ticker['volatilidade_21d'].iloc[-1] * 100 if 'volatilidade_21d' in df_ticker.columns else 0

# =========================================================================================
# ABA 1: VISÃO GERAL (O Terminal do Investidor)
# =========================================================================================
if selected_tab == "Visão Geral":
    st.title(f"📊 Terminal de Operações: {selected_ticker}")
    
    # 1. Grid de Métricas Premium (Glassmorphism)
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Preço de Fecho (R$)", f"R$ {last_close:,.2f}", f"{delta_pct:+.2f}% Diário")
    col2.metric("Força Relativa (RSI)", f"{rsi_val:.1f}", "Alta Pressão" if rsi_val > 60 else ("Sobrevenda" if rsi_val < 40 else "Zona Neutra"))
    col3.metric("Risco (Volatilidade 21d)", f"{vol_val:.1f}%", delta_color="inverse")
    
    tendencia = "Alta Clara 📈" if last_close > df_ticker[f'SMA_{sma_longa}'].iloc[-1] else "Baixa Severa 📉"
    col4.metric(f"Tendência ({sma_longa} dias)", tendencia)

    # 2. Caixa de Storytelling
    st.markdown("""
    <div class='insight-card'>
        💡 <b>Visão do Analista (Como ler este gráfico):</b> <br>
        O gráfico de Velas (Candlestick) abaixo mostra a verdadeira batalha entre compradores e vendedores. 
        As <b>faixas laranjas (Bandas de Bollinger)</b> indicam as zonas limite de explosão do preço. Se o preço toca a faixa inferior e o RSI (acima) está baixo, o nosso algoritmo procura oportunidades de compra (Ação 'Barata' e sobrevendida).
    </div>
    """, unsafe_allow_html=True)
    
    # 3. O Grande Gráfico de Preço + Volume
    fig_main = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.75, 0.25])
    
    # Candlestick (Velas)
    fig_main.add_trace(go.Candlestick(
        x=df_ticker['date'], open=df_ticker['open'], high=df_ticker['high'], low=df_ticker['low'], close=df_ticker['close'], name='Cotação'
    ), row=1, col=1)
    
    # Bollinger Bands
    fig_main.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['BB_Upper'], line=dict(color='rgba(255, 165, 0, 0.4)', width=1), name='Limite Superior (Venda)', showlegend=False), row=1, col=1)
    fig_main.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['BB_Lower'], line=dict(color='rgba(255, 165, 0, 0.4)', width=1), fill='tonexty', fillcolor='rgba(255, 165, 0, 0.05)', name='Canal de Volatilidade'), row=1, col=1)

    # Volume Bar Chart colorido
    colors = ['#00d26a' if row['close'] >= row['open'] else '#ff4b4b' for idx, row in df_ticker.iterrows()]
    fig_main.add_trace(go.Bar(x=df_ticker['date'], y=df_ticker['volume'], marker_color=colors, name='Volume Negociado'), row=2, col=1)
    
    fig_main.update_layout(
        template='plotly_dark', height=750, xaxis_rangeslider_visible=False,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1, font=dict(size=14))
    )
    st.plotly_chart(fig_main, use_container_width=True)

# =========================================================================================
# ABA 2: RAIO-X TÉCNICO E IA
# =========================================================================================
elif selected_tab == "Raio-X Técnico":
    st.title("🔬 Sinais Algorítmicos e Momentum")
    
    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("### 🤖 O Parecer da Inteligência")
        
        # Lógica de Recomendação Automática
        if rsi_val < 30 and last_close > df_ticker['BB_Lower'].iloc[-1]: 
            sinal, cor, desc = "COMPRA FORTE", "#00d26a", "Ação extremamente barata (RSI baixo) e a respeitar o limite inferior de volatilidade."
        elif rsi_val > 70 and last_close < df_ticker['BB_Upper'].iloc[-1]: 
            sinal, cor, desc = "VENDA FORTE", "#ff4b4b", "Ação sobrecomprada (Cara). O risco de correção e queda abrupta é altíssimo."
        else: 
            sinal, cor, desc = "AGUARDAR (NEUTRO)", "#feca57", "O ativo está no meio do seu canal normal. Sem assimetria clara de risco/retorno."
            
        st.markdown(f"""
        <div style='text-align: center; padding: 30px; background: rgba(0,0,0,0.5); border-radius: 15px; border: 2px solid {cor}; box-shadow: 0 0 20px {cor}40;'>
            <h2 style='color: {cor} !important; margin:0;'>{sinal}</h2>
            <p style='color: #aaa; margin-top: 10px; font-size: 1.1rem;'>{desc}</p>
        </div>
        """, unsafe_allow_html=True)
        
        # Gráfico de Velocímetro (Gauge) para o RSI
        fig_gauge = go.Figure(go.Indicator(
            mode = "gauge+number",
            value = rsi_val,
            domain = {'x': [0, 1], 'y': [0, 1]},
            title = {'text': "Pressão de Compra (RSI)", 'font': {'size': 20, 'color': 'white'}},
            gauge = {
                'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "white"},
                'bar': {'color': "rgba(255,255,255,0.5)"},
                'bgcolor': "rgba(0,0,0,0)",
                'borderwidth': 2,
                'bordercolor': "gray",
                'steps': [
                    {'range': [0, 30], 'color': "rgba(0, 210, 106, 0.6)"}, # Verde (Sobrevenda)
                    {'range': [30, 70], 'color': "rgba(128, 128, 128, 0.2)"}, # Cinza (Neutro)
                    {'range': [70, 100], 'color': "rgba(255, 75, 75, 0.6)"}], # Vermelho (Sobrecompra)
            }
        ))
        fig_gauge.update_layout(height=300, template='plotly_dark', margin=dict(l=20, r=20, t=50, b=20), paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_gauge, use_container_width=True)
        
    with c2:
        st.subheader("MACD: O Radar de Reversão de Tendência")
        st.markdown("""
        <div class='insight-card' style='padding: 15px; margin-bottom: 10px;'>
            💡 O <b>Histograma (Barras)</b> mede a força do mercado. Quando as barras vermelhas começam a diminuir e cruzam para cima, temos uma forte indicação algorítmica de que a queda acabou.
        </div>
        """, unsafe_allow_html=True)
        
        col_macd = 'macd_line' if 'macd_line' in df_ticker.columns else None
        col_sig = 'macd_signal' if 'macd_signal' in df_ticker.columns else None
        col_hist = 'macd_hist' if 'macd_hist' in df_ticker.columns else None
        
        if col_macd and col_sig:
            fig_macd = go.Figure()
            fig_macd.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_macd], name='Linha MACD', line=dict(color='#00f2ff', width=2)))
            fig_macd.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_sig], name='Sinal', line=dict(color='#ff7f0e', width=2)))
            if col_hist:
                cores_hist = ['#00d26a' if val >= 0 else '#ff4b4b' for val in df_ticker[col_hist]]
                fig_macd.add_trace(go.Bar(x=df_ticker['date'], y=df_ticker[col_hist], name='Força (Histograma)', marker_color=cores_hist))
                
            fig_macd.update_layout(template='plotly_dark', height=500, margin=dict(l=10, r=10, t=30, b=10), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
            st.plotly_chart(fig_macd, use_container_width=True)
        else:
            st.warning("⚠️ Dados de MACD não disponíveis no Dataset Processado.")

# =========================================================================================
# ABA 3: FUNDAMENTOS & MACROECONOMIA
# =========================================================================================
elif selected_tab == "Fundamentos & Macro":
    st.title("🌍 O Cenário Global: Ação vs Economia Brasileira")
    
    st.markdown("""
    <div class='insight-card'>
        💡 <b>A Prova de Ouro do Analista (Custo de Oportunidade):</b><br> 
        O mercado financeiro não vive num vácuo; ele concorre com a Taxa de Juros do Governo (Selic). Nós transformamos a Ação e a Selic na <b>Base 100</b>. Isto significa que simulamos R$ 100 investidos no primeiro dia do gráfico. 
        <br><i>Conclusão: Se a linha azul (Ação) estiver abaixo da vermelha (Selic), o investidor sofreu stress na bolsa de valores para perder para a Renda Fixa!</i>
    </div>
    """, unsafe_allow_html=True)

    fig_macro = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Base 100 da Ação
    df_ticker['Base_100_Acao'] = (df_ticker['close'] / df_ticker['close'].iloc[0]) * 100
    fig_macro.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['Base_100_Acao'], name=f'Crescimento {selected_ticker}', line=dict(color='#00f2ff', width=4)), secondary_y=False)
    
    # Base 100 da Selic
    if 'selic' in df_ticker.columns:
        df_ticker['Retorno_Selic_Diario'] = (df_ticker['selic'] / 100) / 252
        df_ticker['Base_100_Selic'] = 100 * (1 + df_ticker['Retorno_Selic_Diario']).cumprod()
        fig_macro.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['Base_100_Selic'], name='Rendimento Selic (Seguro)', fill='tozeroy', line=dict(color='rgba(255, 75, 75, 0.4)')), secondary_y=False)

    # Adiciona o Dólar no eixo secundário se existir
    if 'dolar' in df_ticker.columns:
        fig_macro.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['dolar'], name='Dólar (R$)', line=dict(color='#feca57', dash='dot', width=2)), secondary_y=True)

    fig_macro.update_layout(template='plotly_dark', height=700, hovermode="x unified", title="Guerra de Rendimentos: Ação vs Selic vs Dólar", title_font_size=20)
    fig_macro.update_yaxes(title_text="Capital Acumulado (Base 100)", secondary_y=False)
    fig_macro.update_yaxes(title_text="Cotação Dólar (R$)", secondary_y=True)
    st.plotly_chart(fig_macro, use_container_width=True)

# =========================================================================================
# ABA 4: RADAR INSTITUCIONAL (MAPA DO DINHEIRO) - AMPLIADA
# =========================================================================================
elif selected_tab == "Radar Institucional":
    st.title("🎯 Radar Institucional e Mapas de Valor")
    
    # Prepara um dataframe com a "fotografia" do último dia de TODAS as ações do dataset
    df_last = df_master.sort_values('date').groupby('ticker').tail(1).copy()
    
    # Dividimos em 2 Grandes Mapas
    st.markdown("""
    <div class='insight-card'>
        💡 <b>A Visão 'Quant' (Quantitativa):</b> Aqui avaliamos a Bolsa Inteira de uma vez. Usamos o gráfico de Markowitz (Risco vs Retorno) para encontrar ativos seguros, e a Matriz de Value Investing (CVM) para encontrar empresas cujo Lucro não reflete o Preço.
    </div>
    """, unsafe_allow_html=True)
    
    tab_radar1, tab_radar2, = st.tabs(["⚖️ Fronteira de Risco (Markowitz)", "🔥 Heatmap de Correlação"])
    
    # --- SUB-ABA 1: MARKOWITZ ---
    with tab_radar1:
        retorno_col = 'momentum_12m' if 'momentum_12m' in df_last.columns else ('retorno_acumulado' if 'retorno_acumulado' in df_last.columns else None)
        risco_col = 'volatilidade_252d' if 'volatilidade_252d' in df_last.columns else ('volatilidade_21d' if 'volatilidade_21d' in df_last.columns else None)
        
        if risco_col and retorno_col:
            df_plot_risk = df_last.dropna(subset=[risco_col, retorno_col])
            df_plot_risk = df_plot_risk[(df_plot_risk[risco_col] > 0) & (df_plot_risk[risco_col] < 1.5)] # Max 150% de vol
            
            df_plot_risk['Risco %'] = df_plot_risk[risco_col] * 100
            df_plot_risk['Retorno %'] = df_plot_risk[retorno_col] * 100
            
            fig_scatter = px.scatter(
                df_plot_risk, x='Risco %', y='Retorno %', text='ticker', color='Retorno %',
                size='volume' if 'volume' in df_plot_risk.columns else None,
                color_continuous_scale='RdYlGn', title='Fronteira Eficiente: O Quadrante Superior Esquerdo é o Ouro'
            )
            
            fig_scatter.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.5)")
            fig_scatter.update_traces(textposition='top center', marker=dict(opacity=0.8, line=dict(width=1, color='DarkSlateGrey')))
            fig_scatter.update_layout(template='plotly_dark', height=700)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("⚠️ Colunas de Volatilidade ou Retorno em falta no Dataset para gerar a Fronteira de Risco.")

        if 'pl' in df_last.columns and 'roe' in df_last.columns:
            st.markdown("<p style='font-size:18px;'>Procuramos empresas no <b>Quadrante Esquerdo (Baratas / P/L Baixo)</b> e <b>Superior (Eficientes / ROE Alto)</b>.</p>", unsafe_allow_html=True)
            
            df_val = df_last.dropna(subset=['pl', 'roe'])
            # Filtro para ignorar bizarrices extremas que quebram o gráfico
            df_val = df_val[(df_val['pl'] > 0) & (df_val['pl'] < 40) & (df_val['roe'] > -50) & (df_val['roe'] < 100)]
            
            fig_val = px.scatter(
                df_val, x='pl', y='roe', text='ticker', color='roe',
                color_continuous_scale='Mint', title='Radar CVM: Valuation (Preço/Lucro) vs Eficiência (ROE %)'
            )
            
            fig_val.add_vline(x=15, line_dash="dash", line_color="orange", annotation_text="P/L Justo")
            fig_val.add_hline(y=15, line_dash="dash", line_color="orange", annotation_text="ROE Mínimo Aceitável")
            fig_val.update_traces(textposition='top center', marker=dict(size=12, opacity=0.9, line=dict(width=1, color='white')))
            fig_val.update_layout(template='plotly_dark', height=700, xaxis_title="Valuation (P/L) - Menor é Melhor", yaxis_title="Lucratividade (ROE %) - Maior é Melhor")
            st.plotly_chart(fig_val, use_container_width=True)
        else:
            st.warning("⚠️ Colunas de Fundamentos CVM (PL e ROE) não encontradas no dataset processado. Execute o mapeador da CVM do Squad 1.")

    # --- SUB-ABA 2: CORRELAÇÃO MACRO ---
    with tab_radar2:
        st.markdown("<p style='font-size:18px;'>Como o ativo selecionado se comporta perante as taxas da economia oficial?</p>", unsafe_allow_html=True)
        
        colunas_macro = ['close', 'volume', 'selic', 'dolar', 'ipca']
        colunas_existentes = [c for c in colunas_macro if c in df_ticker.columns]
        
        if len(colunas_existentes) > 2:
            # Calcular matriz de correlação baseada apenas nos dados visíveis
            corr_matrix = df_ticker[colunas_existentes].corr()
            
            # Formatar nomes bonitos
            nomes_bonitos = {'close': 'Preço Ação', 'volume': 'Volume', 'selic': 'Taxa Selic', 'dolar': 'Câmbio Dólar', 'ipca': 'Inflação'}
            corr_matrix.rename(index=nomes_bonitos, columns=nomes_bonitos, inplace=True)
            
            fig_corr = px.imshow(
                corr_matrix, text_auto=".2f", aspect="auto",
                color_continuous_scale='RdBu_r', zmin=-1, zmax=1,
                title=f"Heatmap de Correlação: {selected_ticker} vs Macroeconomia"
            )
            fig_corr.update_layout(template='plotly_dark', height=600)
            st.plotly_chart(fig_corr, use_container_width=True)
        else:
            st.warning("⚠️ Dados macroeconômicos insuficientes para cruzar a correlação.")

# Rodapé final limpo
st.markdown("---")
st.markdown("<p style='text-align: center; color: #555; font-size: 14px;'>Desenvolvido por Equipe 2 | Analytics Pro B3 - Inteligência Computacional Aplicada a Finanças</p>", unsafe_allow_html=True)