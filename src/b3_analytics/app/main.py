import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
try:
    from streamlit_option_menu import option_menu
except ImportError:
    st.error("❌ Módulo 'streamlit-option-menu' não encontrado. Por favor, execute: pip install streamlit-option-menu")
    st.stop()
import os
from datetime import datetime
from pathlib import Path

from b3_analytics.utils.paths import RAW_DIR, PROCESSED_DIR

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Alpha-IA Dashboard | B3 Analytics",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- CARREGAR ESTILOS ---
def local_css(file_name):
    if os.path.exists(file_name):
        with open(file_name) as f:
            st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Ajuste do caminho do estilo usando os novos padrões
style_path = os.path.join(os.path.dirname(__file__), 'assets/styles.css')
local_css(style_path)

# --- CARREGAR DADOS ---
@st.cache_data
def load_data():
    # Prioridade para o parquet processado
    data_path = PROCESSED_DIR / '01_market_data_processed.parquet'
    if data_path.exists():
        df = pd.read_parquet(data_path)
        df['date'] = pd.to_datetime(df['date'])
        return df
    
    # Fallback para o CSV raw do yfinance se o processado não existir
    raw_path = RAW_DIR / '01_yfinance_precos_raw.csv'
    if raw_path.exists():
        # O notebook usa sep=';' e decimal=','
        df = pd.read_csv(raw_path, sep=';', decimal=',')
        df['Date'] = pd.to_datetime(df['Date'])
        # Normalizar nomes de colunas para o que o dashboard espera
        df = df.rename(columns={
            'Date': 'date',
            'Ticker': 'ticker',
            'Close': 'close',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Volume': 'volume'
        })
        return df
    return None

df_master = load_data()

# --- SIDEBAR ---
with st.sidebar:
    st.image("https://raw.githubusercontent.com/Alpha-Desafio-Equipe2/Desafio-Analytics/main/docs/logo_placeholder.png", width=200) # Placeholder
    st.markdown("## 🛠️ Controles")
    
    tickers_financeiros = [
        'BBSE3.SA', 'CXSE3.SA', 'PSSA3.SA', 'WIZC3.SA', 'ITUB4.SA', 'BPAC11.SA', 'BBDC3.SA', 
        'ITSA4.SA', 'BBAS3.SA', 'SANB11.SA', 'B3SA3.SA', 'MULT3.SA', 'BPAN4.SA', 'ALOS3.SA', 
        'BRAP4.SA', 'IGTI3.SA', 'BRSR6.SA', 'ABCB4.SA', 'SIMH3.SA', 'IRBR3.SA', 'BEES3.SA', 
        'BMGB4.SA', 'PLPL3.SA', 'PINE4.SA', 'LOGG3.SA', 'BGIP4.SA', 'SCAR3.SA', 'SYNE3.SA', 
        'MERC4.SA', 'RPAD3.SA', 'ESPA3.SA', 'HBRE3.SA'
    ]
    
    if df_master is not None:
        # Filtrar tickers que realmente existem no dataset
        tickers_validos = sorted([t for t in tickers_financeiros if t in df_master['ticker'].unique()])
        if not tickers_validos:
            tickers_validos = sorted(df_master['ticker'].unique())
            
        selected_ticker = st.selectbox("Selecione o Ativo", tickers_validos, index=0)
        
        sectors = sorted(df_master['sector'].dropna().unique()) if 'sector' in df_master.columns else []
        selected_sector = st.selectbox("Filtrar por Setor", ["Todos"] + sectors)
        
        st.markdown("---")
        st.markdown("### 📅 Período")
        date_range = st.date_input("Intervalo de Datas", [df_master['date'].min(), df_master['date'].max()])
    else:
        st.error("Dados não encontrados! Por favor, execute o notebook b3_analytics.ipynb primeiro.")

# --- NAVEGAÇÃO ---
selected_tab = option_menu(
    menu_title=None,
    options=["Visão Geral", "Análise Técnica", "Fundamentos & Macro", "Risco & Beta"],
    icons=["house", "graph-up", "bank", "pie-chart"],
    menu_icon="cast",
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "#0e1117"},
        "icon": {"color": "#00f2ff", "font-size": "20px"},
        "nav-link": {"font-size": "16px", "text-align": "left", "margin": "0px", "--hover-color": "#262730"},
        "nav-link-selected": {"background-color": "#00f2ff", "color": "black"},
    }
)

# --- FILTRAGEM ---
if df_master is not None:
    df_ticker = df_master[df_master['ticker'] == selected_ticker].sort_values('date')
    
    # KPIs rápidos com tratamento de erro
    last_close = df_ticker['close'].iloc[-1] if 'close' in df_ticker.columns else 0
    prev_close = df_ticker['close'].iloc[-2] if len(df_ticker) > 1 and 'close' in df_ticker.columns else last_close
    delta_pct = ((last_close / prev_close) - 1) * 100 if prev_close != 0 else 0
    
    rsi_val = df_ticker['rsi_14'].iloc[-1] if 'rsi_14' in df_ticker.columns else 0
    # ROE estático ou calculado
    roe_val = df_ticker['roe'].iloc[-1] * 100 if 'roe' in df_ticker.columns else 0
    
    # --- RENDERIZAÇÃO ---
    if selected_tab == "Visão Geral":
        st.title(f"🚀 Visão Geral: {selected_ticker}")
        
        st.info("💡 **Dica:** Esta visão combina o preço histórico com o **Drawdown (Queda Máxima)**. O Drawdown mostra o quanto o ativo caiu desde seu último topo, ajudando a medir a resiliência.")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Preço Atual", f"R$ {last_close:,.2f}", f"{delta_pct:+.2f}%")
        col2.metric("RSI (14)", f"{rsi_val:.1f}", "Sobrecompra" if rsi_val > 70 else ("Sobrevenda" if rsi_val < 30 else "Neutro"))
        col3.metric("ROE Real", f"{roe_val:.2f}%")

        # Gráfico Principal
        st.subheader("📊 Histórico de Preços e Drawdown")
        fig_price = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Velas (Candlestick)
        fig_price.add_trace(go.Candlestick(x=df_ticker['date'], open=df_ticker['open'], high=df_ticker['high'], low=df_ticker['low'], close=df_ticker['close'], name='Preço'), secondary_y=False)
        
        # Médias Móveis
        if 'sma_21' in df_ticker.columns:
            fig_price.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['sma_21'], name='Média 21d', line=dict(color='orange', width=1)), secondary_y=False)
        if 'sma_50' in df_ticker.columns:
            fig_price.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['sma_50'], name='Média 50d', line=dict(color='cyan', width=1)), secondary_y=False)
            
        # Drawdown (Escala ajustada 100x para % conforme pedido)
        if 'drawdown' in df_ticker.columns:
            fig_price.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['drawdown'] * 100, name='Drawdown %', fill='tozeroy', line=dict(color='rgba(255, 0, 0, 0.3)', width=0.5)), secondary_y=True)
            
        fig_price.update_layout(template='plotly_dark', height=600, xaxis_rangeslider_visible=False, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
        fig_price.update_yaxes(title_text="Preço (R$)", secondary_y=False)
        fig_price.update_yaxes(title_text="Queda (%)", secondary_y=True, range=[-100, 5])
        
        st.plotly_chart(fig_price, width='stretch')

    elif selected_tab == "Análise Técnica":
        st.title("📈 Estudo Técnico Avançado")
        st.markdown("""
        Nesta seção, analisamos o **Momentum** e a **Volatilidade**. 
        *   **RSI:** Identifica se o papel está 'caro' (acima de 70) ou 'barato' (abaixo de 30).
        *   **Volatilidade:** Mede o 'nervosismo' do mercado. Valores altos sugerem maior risco e variação de preço.
        """)
        
        col_t1, col_t2 = st.columns(2)
        
        with col_t1:
            st.subheader("Momentum (RSI)")
            fig_rsi = px.line(df_ticker, x='date', y='rsi_14', title="Índice de Força Relativa")
            fig_rsi.add_hline(y=70, line_dash="dash", line_color="red")
            fig_rsi.add_hline(y=30, line_dash="dash", line_color="green")
            fig_rsi.update_layout(template='plotly_dark')
            st.plotly_chart(fig_rsi, width='stretch')
            
        with col_t2:
            st.subheader("Volatilidade (21d) %")
            # Ajustando escala para % (100x)
            if 'volatilidade_21d' in df_ticker.columns:
                df_ticker['vol_pct'] = df_ticker['volatilidade_21d'] * 100
                fig_vol = px.area(df_ticker, x='date', y='vol_pct', title="Volatilidade Anualizada (%)")
                fig_vol.update_layout(template='plotly_dark')
                st.plotly_chart(fig_vol, width='stretch')
            else:
                st.warning("Dados de volatilidade não encontrados.")

        st.subheader("MACD & Divergências")
        st.info("O MACD é a diferença entre médias rápidas e lentas. Cruzamentos da linha de Sinal (laranja) costumam indicar inversões de tendência.")
        fig_macd = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05)
        
        # Mapeamento de colunas do parquet (Garantindo que não haja KeyError)
        col_macd = 'macd_line' if 'macd_line' in df_ticker.columns else ('macd_ml' if 'macd_ml' in df_ticker.columns else None)
        col_signal = 'macd_signal' if 'macd_signal' in df_ticker.columns else ('macd_signal_ml' if 'macd_signal_ml' in df_ticker.columns else None)
        col_hist = 'macd_hist' if 'macd_hist' in df_ticker.columns else ('macd_diff_ml' if 'macd_diff_ml' in df_ticker.columns else None)
        
        if col_macd and col_signal:
            fig_macd.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_macd], name='MACD', line=dict(color='cyan')), row=1, col=1)
            fig_macd.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_signal], name='Sinal', line=dict(color='orange')), row=1, col=1)
        else:
            st.warning("Dados de MACD não encontrados para este ativo.")
        
        if col_hist:
            fig_macd.add_trace(go.Bar(x=df_ticker['date'], y=df_ticker[col_hist], name='Histograma'), row=2, col=1)
            
        fig_macd.update_layout(template='plotly_dark', height=500)
        st.plotly_chart(fig_macd, width='stretch')

    elif selected_tab == "Fundamentos & Macro":
        st.title("🏛️ Pilares Fundamentalistas e Cenário Macro")
        
        st.info("📈 **Correlação Macro:** O gráfico abaixo permite comparar como o Lucro da Empresa (verde) e o Dólar (laranja) influenciam a Cotação (pontilhado). Em empresas exportadoras, o Dólar alto costuma puxar o lucro.")

        # Mapeamento de colunas macro
        col_lucro = 'cvm_lucro_liquido' if 'cvm_lucro_liquido' in df_ticker.columns else None
        col_dolar = 'dolar' if 'dolar' in df_ticker.columns else ('cambio' if 'cambio' in df_ticker.columns else None)
        col_selic = 'selic_acumulada' if 'selic_acumulada' in df_ticker.columns else ('selic' if 'selic' in df_ticker.columns else None)
        col_ipca = 'ipca_acumulado' if 'ipca_acumulado' in df_ticker.columns else ('ipca' if 'ipca' in df_ticker.columns else None)
        
        st.subheader("Trindade do Valor: Preço vs Lucro vs Câmbio")
        fig_trindade = make_subplots(specs=[[{"secondary_y": True}]])
        
        if col_lucro:
            # Lucro costuma ser um valor muito maior, ffill para preencher os dias entre balanços
            df_ticker['lucro_plot'] = df_ticker[col_lucro].ffill()
            fig_trindade.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['lucro_plot'], name='Lucro (CVM)', line=dict(color='lime', width=2)), secondary_y=False)
        
        fig_trindade.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['close'], name='Cotação', line=dict(color='white', width=1, dash='dot')), secondary_y=False)
        
        if col_dolar:
            # Dólar em eixo secundário para escala melhor (10x em relação ao preço)
            fig_trindade.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_dolar], name='Dólar (R$)', line=dict(color='orange', width=2, dash='dash')), secondary_y=True)
            
        fig_trindade.update_layout(template='plotly_dark', height=600)
        st.plotly_chart(fig_trindade, width='stretch')
        
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.subheader("Desafio da Renda Fixa")
            st.markdown("Comparação entre reinvestir na ação vs deixar parado na SELIC.")
            fig_selic = go.Figure()
            if 'retorno_acumulado' in df_ticker.columns:
                fig_selic.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker['retorno_acumulado'] * 100, name='Retorno Ação', line=dict(color='cyan')))
            if col_selic:
                fig_selic.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_selic] * 100, name='Retorno Selic (CDI)', fill='tozeroy', line=dict(color='rgba(255,255,255,0.2)')))
            fig_selic.update_layout(template='plotly_dark', title="Retorno Acumulado % vs SELIC")
            st.plotly_chart(fig_selic, width='stretch')
            
        with col_m2:
            st.subheader("Pricing Power (vs Inflação)")
            st.markdown("Verifica se o crescimento do lucro supera a inflação (IPCA).")
            fig_ipca = go.Figure()
            if col_lucro:
                fig_ipca.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_lucro].pct_change().ffill().cumsum() * 100, name='Cresc. Lucro', line=dict(color='lime')))
            if col_ipca:
                fig_ipca.add_trace(go.Scatter(x=df_ticker['date'], y=df_ticker[col_ipca] * 100, name='IPCA Acumulado', line=dict(color='red')))
            fig_ipca.update_layout(template='plotly_dark', title="Crescimento Lucro vs Inflação")
            st.plotly_chart(fig_ipca, width='stretch')

        st.markdown("---")
        st.subheader("Mapeamento do Universo (Bunker Setorial)")
        # Comparação setorial no Bunker
        if 'sector' in df_master.columns:
            df_latest = df_master.groupby('ticker').last().reset_index()
            if selected_sector != "Todos":
                df_latest = df_latest[df_latest['sector'] == selected_sector]
                
            fig_bunker = px.scatter(df_latest, x='roe', y='drawdown', hover_name='ticker', 
                                     size='volatilidade_21d' if 'volatilidade_21d' in df_latest.columns else None, 
                                     color='retorno_acumulado' if 'retorno_acumulado' in df_latest.columns else None,
                                     title="Bunker Setorial: Resiliência vs Rentabilidade",
                                     labels={'roe': 'ROE (%)', 'drawdown': 'Drawdown Máximo (%)'},
                                     color_continuous_scale='RdYlGn')
            
            # Melhorar estética: remover labels fixos para evitar poluição, aumentar tamanho dos pontos
            fig_bunker.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')), selector=dict(mode='markers'))
            fig_bunker.update_layout(template='plotly_dark', height=600)
            st.plotly_chart(fig_bunker, width='stretch')
        else:
            st.info("Informações setoriais não disponíveis para comparação.")

    elif selected_tab == "Risco & Beta":
        st.title("🛡️ Gestão de Risco e Comparação de Ativos")
        
        st.info("Esta seção traz a inteligência de carteira do notebook. O **Beta** mede quanto o ativo se move em relação ao Ibovespa. Um Beta de 1.5 significa que se o Ibovespa subir 1%, o ativo tende a subir 1.5%.")
        
        # Simulação de Beta se não houver coluna pronta (no parquet vimos que não tem 'beta' direto pra todos)
        # Mas o notebook calculou. Se o parquet tiver, usamos, senão mostramos aviso.
        
        col_r1, col_r2 = st.columns(2)
        
        with col_r1:
            st.subheader("Risco vs Retorno (Fronteira)")
            # Usando últimos dados de todos os ativos para o scatter
            df_port = df_master.groupby('ticker').last().reset_index()
            fig_risk = px.scatter(df_port, x='volatilidade_21d', y='retorno_acumulado', text='ticker',
                                   title="Risco (Vol) vs Retorno Acumulado",
                                   labels={'volatilidade_21d': 'Risco Anualizado', 'retorno_acumulado': 'Retorno Total'},
                                   color='sharpe_ratio_21d' if 'sharpe_ratio_21d' in df_port.columns else None,
                                   color_continuous_scale='Viridis')
            fig_risk.update_layout(template='plotly_dark', height=500)
            st.plotly_chart(fig_risk, width='stretch')
            
        with col_r2:
            st.subheader("Beta (Sensibilidade ao Mercado)")
            # Se não houver beta, mostramos volatilidade como proxy de risco relativo
            if 'beta' in df_port.columns:
                fig_beta = px.bar(df_port.sort_values('beta'), x='ticker', y='beta', title="Beta dos Ativos")
                fig_beta.update_layout(template='plotly_dark', height=500)
                st.plotly_chart(fig_beta, width='stretch')
            else:
                st.warning("Indicador Beta não calculado no dataset atual. Exibindo Volatilidade Relativa.")
                fig_vol_rel = px.bar(df_port.sort_values('volatilidade_21d'), x='ticker', y='volatilidade_21d', title="Volatilidade Relativa")
                fig_vol_rel.update_layout(template='plotly_dark', height=500)
                st.plotly_chart(fig_vol_rel, width='stretch')

st.markdown("---")
st.caption("Desenvolvido por Equipe 2 | Alpha Analytics Dashboard v1.0")
