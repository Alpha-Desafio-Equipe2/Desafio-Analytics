import pandas as pd
import requests
import zipfile
import io
import os
import time
import traceback
from datetime import datetime
from b3_analytics.utils.paths import RAW_DIR


# =========================================================================
# UNIVERSO ALVO (A LISTA DE ELITE)
# excluidos IRBR3, bnbr3, BAZA3, BSLI3, PEAB3, BMEB4
# =========================================================================
TICKERS_ALVO = [
    "PETR4","ITUB4","VALE3","BPAC11","ABEV3","WEGE3","BBDC3","AXIA3","ITSA4","BBAS3","VIVT3","SANB11","SBSP3", "B3SA3","RDOR3","SUZB3","BBSE3","EMBJ3","CPLE3","TIMS3","RENT3","CPFE3","CXSE3","EQTL3","PRIO3","RADL3", "NEOE3","ENEV3","GGBR3","EGIE3","CMIG4","VBBR3","PSSA3","MOTV3","CMIN3","RAIL3","UGPA3","MBRF3","ENGI11","KLBN11","CSAN3","TOTS3","CSMG3","ISAE4","CGAS3","MULT3","BPAN4","REDE3","ALOS3","MRSA3B","LREN3","TAEE11","EQPA3","CEGR3","HYPE3","GGPS3","SAPR11","CYRE3","CEEB3","GMAT3","AURE3","AZUL54","SMFT3","NATU3","ALUP11","ASAI3","GOAU4","ENMT4","CURY3","CSNA3","FLRY3","ALPA4","BRAP4","USIM5","BRAV3","TTEN3","EKTR3","CASN3","SLCE3","POMO4","IGTI3","DIRR3","SRNA3","MDIA3","BRSR6","UNIP6","ODPV3","VIVA3","BRKM5","RAIZ4","MGLU3","ECOR3","ORVR3","COGN3","FRAS3","WHRL4","CBAV3","ABCB4","JHSF3","SMTO3","VULC3","HBSA3","EQMA3B","CLSC4","MRVE3","SHUL4","AZZA3","HAPV3","SIMH3","BEEF3","LEVE3","MOVI3","DXCO3","RIAA3","DASA3","CBEE3","VAMO3","GRND3","INTB3","PGMN3","EZTC3","CEAB3","RECV3","TEND3","MILS3","LAVV3","LOGN3","COCE5","YDUQ3","FESA4","GEPA4","EMAE4","MDNE3","BEES3","BMGB4","PINE4","PLPL3","SBFG3","BHIA3","AUAU3","TGMA3","TFCO4","ONCO3","LOGG3","BLAU3","CSED3","CAML3","PNVL3","RAPT4","RANI3","JSLG3","CEBR3","AGRO3","FIQE3","BMOB3","ARML3","MATD3","LWSA3","EUCA3","VTRU3","OPCT3","ANIM3","LIGT3","TRIS3","VLID3","TUPY3","SEQL3","EVEN3","DESK3","MYPK3","KEPL3","WIZC3","SEER3","ZAMP3","OFSA3","BRST3","TELB3","PCAR3","OBTC3","PRNR3","SOJA3","GPAR3","PFRM3","BGIP4","CVCB3","ALPK3","FRIO3","MOSI3","MLAS3","AMER3","DEXP3","JALL3","BIOM3","LAND3","WLMM3","SCAR3","CSUD3","TASA3","MELK3","PATI3","ROMI3","AALR3","SYNE3","ALLD3","CEED3","CGRA4","TKNO4","MERC4","VITT3","MTSA4","POSI3","QUAL3","CRPG5","TECN3","AMOB3","RNEW4","AFLT3","VVEO3","AMAR3","PTBL3","RPAD3","VSTE3","DOHL4","LJQQ3","ESPA3","MTRE3","PTNT4","HBOR3","CAMB3","CASH3","AMBP3","DMVF3","MEAL3","EALT4","HBRE3","MNP3"
]

# =========================================================================
# MOTOR DE RESOLUÇÃO FOCADO (MAPEAR APENAS A ELITE)
# =========================================================================
def gerar_mapa_cvm_focado(tickers_alvo):
    """
    Gera um mapeamento entre códigos de registro da CVM e tickers da B3
    a partir dos arquivos públicos de Formulário Cadastral (FCA).

    A função baixa automaticamente o dataset anual do FCA disponibilizado
    pela Comissão de Valores Mobiliários (CVM) e identifica quais empresas
    correspondem aos tickers fornecidos. Diferentemente de abordagens
    genéricas, o processamento é focado apenas nos tickers de interesse,
    descartando centenas de empresas irrelevantes logo na etapa inicial.

    O resultado é um dicionário que relaciona o código CVM da empresa
    ao seu respectivo ticker negociado na bolsa.

    Parameters
    ----------
    tickers_alvo : list
        Lista de tickers da B3 que devem ser mapeados para seus respectivos
        códigos de registro na CVM (ex: ["PETR4", "VALE3", "ITUB4"]).

    Returns
    -------
    dict
        Dicionário contendo o mapeamento entre código CVM e ticker.
        Estrutura do resultado:

        {
            codigo_cvm: ticker
        }

        Exemplo:
        {
            9512: "PETR4",
            4170: "VALE3",
            19348: "ITUB4"
        }

    Notes
    -----
    - Os dados são obtidos diretamente do portal público de dados abertos
      da CVM (https://dados.cvm.gov.br).
    - O ano de referência utilizado corresponde ao último ano completo
      disponível no momento da execução.
    - Caso o código CVM não esteja presente no arquivo de valores
      mobiliários, a função tenta recuperar a informação através do
      arquivo geral de companhias usando o CNPJ como chave de ligação.
    - Apenas empresas cujo ticker esteja presente na lista `tickers_alvo`
      são incluídas no resultado final.
    - Em caso de erro no download ou processamento do dataset,
      a função retorna um dicionário vazio.

    Examples
    --------
    >>> tickers = ["PETR4", "VALE3", "ITUB4"]
    >>> mapa = gerar_mapa_cvm_focado(tickers)
    >>> mapa[9512]
    'PETR4'
    """

    ano_referencia = datetime.now().year - 1
    print(f"🕵️‍♂️ Passo 1: A mapear Códigos CVM para a sua lista de Elite (Ano Base: {ano_referencia})...")
    
    url_fca = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/FCA/DADOS/fca_cia_aberta_{ano_referencia}.zip"
    
    try:
        resposta = requests.get(url_fca, timeout=60)
        if resposta.status_code != 200:
            print(f"❌ Erro ao baixar o ficheiro FCA ({resposta.status_code}).")
            return {}
            
        arquivo_zip = zipfile.ZipFile(io.BytesIO(resposta.content))
        nome_ficheiro_acoes = f"fca_cia_aberta_valor_mobiliario_{ano_referencia}.csv"
        nome_ficheiro_geral = f"fca_cia_aberta_geral_{ano_referencia}.csv"
        
        dicionario_cvm_ticker = {}
        
        with arquivo_zip.open(nome_ficheiro_acoes) as f:
            df_fca = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
            
            cols = {c.upper(): c for c in df_fca.columns}
            col_ticker = cols.get('CODIGO_NEGOCIACAO', cols.get('COD_NEGOCIACAO'))
            col_cvm = cols.get('CODIGO_CVM', cols.get('CD_CVM'))
            
            # Se não houver CD_CVM no ficheiro de ações, busca no geral via CNPJ
            if not col_cvm:
                with arquivo_zip.open(nome_ficheiro_geral) as f_g:
                    df_geral = pd.read_csv(f_g, sep=';', encoding='ISO-8859-1')
                    cols_g = {c.upper(): c for c in df_geral.columns}
                    col_cvm_g = cols_g.get('CODIGO_CVM', cols_g.get('CD_CVM'))
                    
                    df_cvm_map = df_geral[['CNPJ_Companhia', col_cvm_g]].drop_duplicates()
                    df_fca = pd.merge(df_fca, df_cvm_map, on='CNPJ_Companhia', how='left')
                    col_cvm = col_cvm_g

            df_fca = df_fca.dropna(subset=[col_ticker, col_cvm])
            grupos = df_fca.groupby(col_cvm)
            
            for cd_cvm, grupo in grupos:
                tickers = [str(t).strip() for t in grupo[col_ticker].unique()]
                
                # A MÁGICA ACONTECE AQUI: Em vez de adivinhar o melhor Ticker,
                # nós verificamos se algum Ticker desta empresa está na lista.
                for t in tickers:
                    if t in tickers_alvo:
                        dicionario_cvm_ticker[int(cd_cvm)] = t
                        break # Encontrou a correspondência exata!
                
        print(f"   ✅ SUCESSO! Das {len(tickers_alvo)} empresas pedidas, mapeámos {len(dicionario_cvm_ticker)} ativas na CVM.")
        return dicionario_cvm_ticker

    except Exception as e:
        print(f"   ❌ Falha no mapeamento: {e}")
        return {}


# =========================================================================
# EXTRATOR OTIMIZADO DE BIG DATA (DRE)
# =========================================================================
def extrair_e_refinar_cvm_historico(ano_inicio=2015, ano_fim=2026):
    """
    Extrai e processa demonstrações financeiras anuais (DRE) das empresas
    listadas na B3 a partir dos dados públicos da CVM.

    A função realiza o download dos arquivos anuais de Demonstração
    Financeira Padronizada (DFP) disponibilizados pela Comissão de
    Valores Mobiliários (CVM) e filtra apenas as empresas presentes
    na lista de tickers alvo do projeto.

    O processo inclui:

    1. Mapeamento entre códigos CVM e tickers da B3.
    2. Download dos arquivos anuais de DFP no portal de dados abertos da CVM.
    3. Filtragem apenas das empresas desejadas para reduzir uso de memória.
    4. Seleção de contas contábeis específicas da DRE.
    5. Consolidação e transformação dos dados em formato tabular.
    6. Conversão e limpeza das métricas financeiras.

    Os indicadores financeiros extraídos incluem:

    - Receita líquida
    - Lucro bruto
    - EBIT operacional
    - Resultado financeiro
    - Lucro antes de impostos (EBT)
    - Lucro líquido

    Parameters
    ----------
    ano_inicio : int, optional
        Ano inicial para extração das demonstrações financeiras.
        Default = 2015.

    ano_fim : int, optional
        Ano final para extração das demonstrações financeiras.
        Default = 2026.

    Returns
    -------
    pd.DataFrame or None
        DataFrame contendo o histórico financeiro anual das empresas
        selecionadas. Cada linha representa uma empresa em um determinado
        exercício fiscal.

        Estrutura típica:

        - DENOM_CIA : nome da empresa
        - CD_CVM : código de registro na CVM
        - Ticker_Alvo : ticker correspondente na B3
        - Data_Referencia : data de encerramento do exercício
        - Ano_Exercicio : ano fiscal
        - 01_Receita_Liquida_R$
        - 02_Lucro_Bruto_R$
        - 03_EBIT_Operacional_R$
        - 04_Resultado_Financeiro_R$
        - 05_Lucro_Antes_Impostos_EBT_R$
        - 06_Lucro_Liquido_R$

        Retorna `None` caso nenhum dado seja processado.

    Notes
    -----
    - Os dados são obtidos diretamente do portal de dados abertos da CVM
      (https://dados.cvm.gov.br).
    - Apenas empresas pertencentes à lista `TICKERS_ALVO` são processadas,
      reduzindo significativamente o consumo de memória e tempo de execução.
    - Os valores financeiros são convertidos para reais (R$), multiplicando
      os valores originais por 1000 conforme o padrão da CVM.
    - O processo inclui múltiplas tentativas de download para lidar com
      instabilidades de rede ou indisponibilidade temporária dos arquivos.
    - Caso o arquivo de um determinado ano não esteja disponível no portal
      da CVM, o ano é ignorado e o processamento continua.

    Examples
    --------
    >>> df_cvm = extrair_e_refinar_cvm_historico(2015, 2024)
    >>> df_cvm.head()

    """

    print(f"\n📥 A descarregar Balanços da Elite B3 ({ano_inicio} a {ano_fim})...")
    
    # 1. Obter o mapeamento exato
    mapa_cvm_ticker = gerar_mapa_cvm_focado(TICKERS_ALVO)
    if not mapa_cvm_ticker:
        return None
        
    # Esta é a nossa chave-mestra. Qualquer balanço que não seja destes códigos é descartado.
    codigos_cvm_alvo = list(mapa_cvm_ticker.keys())

    mapa_contas = {
        '3.01': '01_Receita_Liquida_R$',
        '3.03': '02_Lucro_Bruto_R$',
        '3.05': '03_EBIT_Operacional_R$',
        '3.06': '04_Resultado_Financeiro_R$',
        '3.09': '05_Lucro_Antes_Impostos_EBT_R$',
        '3.11': '06_Lucro_Liquido_R$'
    }
    contas_alvo = list(mapa_contas.keys())
    lista_balancos = []
    
    # 2. LOOP DE EXTRAÇÃO (Sem necessidade de cadastro extra da CVM)
    print("\n📦 Passo 2: A descarregar e processar as DREs anuais...")
    
    for ano in range(ano_inicio, ano_fim + 1):
        sucesso_ano = False
        for tentativa in range(1, 4):
            print(f"   -> [{tentativa}/3] A processar ano {ano}...", end="\r")
            url = f"https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/dfp_cia_aberta_{ano}.zip"
            
            try:
                tempo_espera = 20 * tentativa
                resposta = requests.get(url, timeout=tempo_espera, stream=True)
                
                if resposta.status_code == 200:
                    conteudo = io.BytesIO()
                    for chunk in resposta.iter_content(chunk_size=1024*1024): 
                        if chunk: conteudo.write(chunk)
                    
                    arquivo_zip = zipfile.ZipFile(conteudo)
                    ficheiro_dre = f"dfp_cia_aberta_DRE_con_{ano}.csv"
                    
                    if ficheiro_dre in arquivo_zip.namelist():
                        with arquivo_zip.open(ficheiro_dre) as f:
                            df_ano = pd.read_csv(f, sep=';', encoding='ISO-8859-1')
                            
                            # REFINAMENTO MASSIVO DE MEMÓRIA: Filtra apenas as empresas da sua lista
                            df_ano = df_ano[df_ano['CD_CVM'].isin(codigos_cvm_alvo)]
                            
                            df_ano = df_ano[df_ano['ORDEM_EXERC'] == 'ÚLTIMO']
                            df_filtro = df_ano[df_ano['CD_CONTA'].isin(contas_alvo)].copy()
                            df_filtro['Conta_Nome'] = df_filtro['CD_CONTA'].map(mapa_contas)
                            
                            lista_balancos.append(df_filtro)
                            print(f"   ✅ Ano {ano}: Extraído com sucesso! (Focado na lista)      ")
                            sucesso_ano = True
                            break
                    else:
                        print(f"   ⚠️ Ano {ano}: Ficheiro DRE ausente no ZIP.      ")
                        sucesso_ano = True
                        break
                elif resposta.status_code == 404:
                    print(f"   ⚠️ Ano {ano}: Ficheiro ainda não existe no Governo.      ")
                    sucesso_ano = True
                    break
                else:
                    time.sleep(2)
                    
            except Exception as e:
                if tentativa == 3:
                    print(f"   ❌ Ano {ano}: Falha de rede após 3 tentativas.      ")
                else:
                    time.sleep(3)
        
        if not sucesso_ano:
            print(f"   🛑 Ignorando o ano {ano}.")
            
    if not lista_balancos:
        print("❌ Nenhum dado foi processado.")
        return None

    # 3. TRANSFORMAÇÃO E LIMPEZA
    print("\n⚙️ Passo 3: A criar o Dataset Final...")
    df_historico_cvm = pd.concat(lista_balancos, ignore_index=True)
    
    df_refinado = df_historico_cvm.pivot_table(
        index=['DENOM_CIA', 'CD_CVM', 'DT_FIM_EXERC'],
        columns='Conta_Nome',
        values='VL_CONTA',
        aggfunc='sum'
    ).reset_index()
    
    colunas_dinheiro = list(mapa_contas.values())
    for col in colunas_dinheiro:
        if col in df_refinado.columns:
            df_refinado[col] = df_refinado[col] * 1000
    
    df_refinado = df_refinado.dropna(subset=['01_Receita_Liquida_R$', '06_Lucro_Liquido_R$'], how='all')
    
    df_refinado['Data_Referencia'] = pd.to_datetime(df_refinado['DT_FIM_EXERC']).dt.strftime('%Y-%m-%d')
    df_refinado['Ano_Exercicio'] = pd.to_datetime(df_refinado['DT_FIM_EXERC']).dt.year
    
    # Injetamos o Ticker exato que você pediu
    df_refinado['Ticker_Alvo'] = df_refinado['CD_CVM'].map(mapa_cvm_ticker)
    
    cols_base = ['DENOM_CIA', 'CD_CVM', 'Ticker_Alvo', 'Data_Referencia', 'Ano_Exercicio']
    cols_finais = [c for c in cols_base + colunas_dinheiro if c in df_refinado.columns]
    
    df_refinado = df_refinado[cols_finais]
    df_refinado = df_refinado.sort_values(by=['Ticker_Alvo', 'Ano_Exercicio'])
    
    print(f"✅ FINALIZADO! O ficheiro agora está perfeitamente alinhado com a sua lista de Tickers.")
    return df_refinado

# =======================================================
# EXECUÇÃO DO PIPELINE
# =======================================================
if __name__ == "__main__":
    df_focado = extrair_e_refinar_cvm_historico(2015, 2026)
    
    if df_focado is not None:
        PASTA_DATASETS = RAW_DIR
        os.makedirs(PASTA_DATASETS, exist_ok=True)
        
        caminho_cvm = f"{PASTA_DATASETS}/06_CVM_Historico_Focado.csv"
        
        df_focado.to_csv(caminho_cvm, sep=';', decimal=',', index=False, encoding='utf-8-sig')
        
        print(f"\n📁 Arquivo Master Salvo: {caminho_cvm}")
        print("\n🔎 Amostra dos Dados Limpos (Só as empresas da sua lista):")
        
        colunas_display = ['Ticker_Alvo', 'Ano_Exercicio', '01_Receita_Liquida_R$', '06_Lucro_Liquido_R$']
        colunas_display = [c for c in colunas_display if c in df_focado.columns]
        
        print(df_focado[colunas_display].head(10))