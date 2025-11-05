import requests
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()

token = os.environ.get("MEU_TOKEN_API")
headers = {'Authorization': 'Bearer {}'.format(token)}

# --- DADOS: Operação ---
params = {
'id_produto': ('173', '1365')
}

api = 'https://laboratoriodefinancas.com/api/v2/pesquisa/operacao'

# --- Chamada de API (Mantenha este código original) ---
try:
    response = requests.get(api, params=params, headers=headers)
    dados = response.json()
    df = pd.DataFrame(dados)
except Exception as e:
    # Se a API falhar, isso deve ser tratado no ambiente local
    print(f"ERRO: Falha na chamada da API. Verifique o token ou a URL. Detalhe: {e}")
    # Cria um DataFrame vazio para não quebrar o resto do script
    df = pd.DataFrame(columns=['nome_pais', 'id_pais', 'tipo_operacao', 'descricao', 'periodo', 'valor'])


#países a serem utilizados (PARA VCR, VCRS, NEI, CAGR)
countries = [
    "World", "Germany", "Netherlands", "Côte d'Ivoire", "Belgium", "Italy",
    "Poland", "France", "United States of America", "Canada", "Malaysia",
    "Ghana", "Ecuador", "Indonesia", "United Kingdom", "Switzerland"
]

#aumento da série histórica
years = list(range(2015, 2025))
periods = [str(years) for years in range(2015, 2025)]


# --- PREPARAÇÃO GERAL ---

# Converte a coluna de valor para numérico, tratando erros
def safe_to_numeric(series):
    return pd.to_numeric(series, errors='coerce')

df.loc[:, 'valor'] = safe_to_numeric(df['valor'])

# Cria lista de países IHH (TODOS os países, exceto 'World', baseado no ID=1)
hhi_countries_all = df[df["id_pais"] != 1]["nome_pais"].unique()


# --- DADOS DE MUNDO (Mundo - id_pais == 1) ---

df_world = df[df["id_pais"]==1].copy()

df_world_exported_cacau = df_world[
    (df_world["tipo_operacao"] == "Exported") &
    (df_world["descricao"] == "Cocoa and cocoa preparations") &
    (df_world["periodo"].isin(periods))
].copy()

df_world_imported_cacau = df_world[
    (df_world["tipo_operacao"] == "Imported") & 
    (df_world["descricao"] == "Cocoa and cocoa preparations") &
    (df_world["periodo"].isin(periods))
].copy()

export_cacau_mundial = df_world_exported_cacau.set_index('periodo')['valor'].rename('export_cacau_mundial')
import_cacau_mundial = df_world_imported_cacau.set_index('periodo')['valor'].rename('import_cacau_mundial')
export_total_mundial = df_world[
    (df_world["tipo_operacao"] == "Exported") & 
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
].set_index('periodo')['valor'].rename('export_total_mundial')


# --- DADOS DE PAÍSES (VCR, VCRS, NEI, CAGR) ---

df_countries_limited = df[df["nome_pais"].isin(countries)].copy()
df_countries_limited = df_countries_limited[df_countries_limited["id_pais"] != 1].copy() # Remove World

# Filtros
df_paises_exported_cacau = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Exported") & 
    (df_countries_limited["descricao"] == "Cocoa and cocoa preparations") & 
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].copy()

df_paises__exported_commodites = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Exported") & 
    (df_countries_limited["descricao"] == "TOTAL - All products") &  
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].copy()

df_paises_imported_cacau = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Imported") &  
    (df_countries_limited["descricao"] == "Cocoa and cocoa preparations") &  
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].copy()

# Componentes de cálculo (LIMITADO)
export_cacau_pelo_pais = df_paises_exported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('export_cacau_pelo_pais')
export_totais_pelo_pais = df_paises__exported_commodites.set_index(['nome_pais', 'periodo'])['valor'].rename('export_totais_pelo_pais')
import_cacau_pelo_pais = df_paises_imported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('import_cacau_pelo_pais')


# --- DADOS DE PAÍSES (IHH - TODOS OS PAÍSES) ---

df_countries_all = df[df["nome_pais"].isin(hhi_countries_all)].copy() # Usa a lista COMPLETA de países

# Filtros IHH (TODOS)
df_ihh_exported_cacau = df_countries_all[
    (df_countries_all["tipo_operacao"]=="Exported") & 
    (df_countries_all["descricao"] == "Cocoa and cocoa preparations") & 
    (df_countries_all["periodo"].astype(str).isin(periods)) 
].copy()

df_ihh_imported_cacau = df_countries_all[
    (df_countries_all["tipo_operacao"]=="Imported") &  
    (df_countries_all["descricao"] == "Cocoa and cocoa preparations") &  
    (df_countries_all["periodo"].astype(str).isin(periods)) 
].copy()

# Componentes de cálculo (TODOS PARA IHH)
export_cacau_pelo_pais_all = df_ihh_exported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('export_cacau_pelo_pais_all')
import_cacau_pelo_pais_all = df_ihh_imported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('import_cacau_pelo_pais_all')


# --- 3. COMBINAÇÃO DOS DADOS (PRINCIPAL - LIMITADO PARA VCR/NEI) ---

df_calc = pd.concat([export_cacau_pelo_pais, export_totais_pelo_pais, import_cacau_pelo_pais], axis=1)

# Adiciona os dados mundiais, fazendo o merge pelo 'periodo'
df_calc = (
    df_calc.reset_index()
    .merge(export_cacau_mundial, on='periodo')
    .merge(export_total_mundial, on='periodo')
    .merge(import_cacau_mundial, on='periodo')
)
df_calc = df_calc.set_index(['nome_pais', 'periodo'])
df_calc = df_calc.fillna(0)
df_calc.replace([np.inf, -np.inf], np.nan, inplace=True)


# --- 4. CÁLCULO DOS ÍNDICES (VCR, VCRS, NEI) ---

participacao_pais_no_cacau = df_calc['export_cacau_pelo_pais'] / df_calc['export_totais_pelo_pais'].replace(0, np.nan)
participacao_mundo_no_cacau = df_calc['export_cacau_mundial'] / df_calc['export_total_mundial'].replace(0, np.nan)
df_calc['VCR'] = participacao_pais_no_cacau.fillna(0) / participacao_mundo_no_cacau.fillna(0)
df_calc['VCR'].replace([np.inf, -np.inf], np.nan, inplace=True) 
df_calc['VCRS'] = (df_calc['VCR'] - 1) / (df_calc['VCR'] + 1)
df_calc['NEI'] = np.where(
    (df_calc['export_cacau_pelo_pais'] + df_calc['import_cacau_pelo_pais']) == 0, 
    0, 
    (df_calc['export_cacau_pelo_pais'] - df_calc['import_cacau_pelo_pais']) / (df_calc['export_cacau_pelo_pais'] + df_calc['import_cacau_pelo_pais'])
)


# --- 5. CÁLCULO DO CAGR (Taxa de Crescimento Anual Composta) ---

export_cacau_pelo_pais_sem_world = export_cacau_pelo_pais.copy()
df_pivot_cagr = export_cacau_pelo_pais_sem_world.unstack('periodo')
valor_inicial = df_pivot_cagr.get('2015', pd.Series(np.nan, index=df_pivot_cagr.index))
valor_final = df_pivot_cagr.get('2024', pd.Series(np.nan, index=df_pivot_cagr.index))
anos = 2024 - 2015

cagr = np.where(
    valor_inicial == 0,
    np.nan, 
    ((valor_final / valor_inicial) ** (1 / anos)) - 1
)
cagr = pd.Series(cagr, index=valor_inicial.index).rename('CAGR (2015-2024)')


# --- 6. CÁLCULO DO IHH (Índice Herfindahl-Hirschman) ---

# 6.1. COMBINAÇÃO DOS DADOS PARA IHH (TODOS OS PAÍSES)
df_ihh_calc = pd.concat([export_cacau_pelo_pais_all, import_cacau_pelo_pais_all], axis=1)

# Adiciona os dados mundiais, fazendo o merge pelo 'periodo'
df_ihh_calc = (
    df_ihh_calc.reset_index()
    .merge(export_cacau_mundial, on='periodo')
    .merge(import_cacau_mundial, on='periodo')
)
df_ihh_calc = df_ihh_calc.set_index(['nome_pais', 'periodo'])
df_ihh_calc = df_ihh_calc.fillna(0)


# --- IHH para EXPORTADORES (Anual) ---
export_cacau_mundial_safe = df_ihh_calc['export_cacau_mundial'].replace(0, np.nan)
df_ihh_calc['market_share_export'] = (df_ihh_calc['export_cacau_pelo_pais_all'] / export_cacau_mundial_safe).fillna(0)
df_ihh_calc['IHH_EXPORTADORES_CONTRIBUICAO'] = (df_ihh_calc['market_share_export'] * 100) ** 2
ihh_exportadores_por_ano = df_ihh_calc.groupby('periodo')['IHH_EXPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_EXPORTADORES')

# --- IHH para IMPORTADORES (Anual) ---
import_cacau_mundial_safe = df_ihh_calc['import_cacau_mundial'].replace(0, np.nan)
df_ihh_calc['market_share_import'] = (df_ihh_calc['import_cacau_pelo_pais_all'] / import_cacau_mundial_safe).fillna(0)
df_ihh_calc['IHH_IMPORTADORES_CONTRIBUICAO'] = (df_ihh_calc['market_share_import'] * 100) ** 2
ihh_importadores_por_ano = df_ihh_calc.groupby('periodo')['IHH_IMPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_IMPORTADORES')


# --- 7. CRIAÇÃO DOS DATAFRAMES FINAIS ---

# DataFrames para VCR, VCRS, NEI, CAGR (sem alterações)
df_vcr = df_calc['VCR'].unstack('periodo')
df_vcrs = df_calc['VCRS'].unstack('periodo')
df_nei = df_calc['NEI'].unstack('periodo')
df_cagr = cagr.to_frame()

# IHH - Contribuição (Por País/Ano)
df_ihh_exportadores_contribuicao = df_ihh_calc['IHH_EXPORTADORES_CONTRIBUICAO'].unstack('periodo')
df_ihh_importadores_contribuicao = df_ihh_calc['IHH_IMPORTADORES_CONTRIBUICAO'].unstack('periodo')

# IHH - Total Anual
df_ihh_exportadores_total = ihh_exportadores_por_ano.to_frame()
df_ihh_importadores_total = ihh_importadores_por_ano.to_frame()


# --- NOVO PASSO: COMBINAR CONTRIBUIÇÃO (PAÍS) E TOTAL (IHH) ---

def combine_hhi_sheets(df_contribution, df_total, total_index_name):
    """Combina o DF de Contribuição por País com o Total Anual IHH, adicionando o total na última linha."""
    
    # 1. Transpõe o DF total para que os anos se tornem colunas
    df_total_T = df_total.T
    
    # 2. Renomeia o índice (que será 'IHH_TOTAL_EXPORTADORES' ou similar) para o nome desejado ('IHH Total')
    df_total_T.index = [total_index_name]
    
    # 3. Garante que os DataFrames tenham os mesmos períodos (colunas)
    common_cols = df_contribution.columns.intersection(df_total_T.columns)
    df_contribution_filtered = df_contribution[common_cols]
    df_total_T_filtered = df_total_T[common_cols]
    
    # 4. Concatena o DF de Contribuição e o DF Total (Total fica na última linha)
    df_combined = pd.concat([df_contribution_filtered, df_total_T_filtered])
    
    return df_combined

# Combinação para Exportadores
df_ihh_export_combinado = combine_hhi_sheets(
    df_ihh_exportadores_contribuicao, 
    df_ihh_exportadores_total, 
    'IHH Total'
)

# Combinação para Importadores
df_ihh_import_combinado = combine_hhi_sheets(
    df_ihh_importadores_contribuicao, 
    df_ihh_importadores_total, 
    'IHH Total'
)


# --- 8. SALVAR OS RESULTADOS EM ARQUIVO EXCEL ---

nome_arquivo_excel = "analise_indece_cacau.xlsx"
formato_precisao_total = "%.5f"

try:
    with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:

        # Índices VCR, VCRS, NEI, CAGR (Sem alterações)
        df_vcr.to_excel(writer, sheet_name='VCR', float_format=formato_precisao_total) 
        df_vcrs.to_excel(writer, sheet_name='VCRS', float_format=formato_precisao_total) 
        df_nei.to_excel(writer, sheet_name='NEI', float_format=formato_precisao_total) 
        df_cagr.to_excel(writer, sheet_name='CAGR', float_format=formato_precisao_total)

        # IHH COMBINADO (NOVO)
        df_ihh_export_combinado.to_excel(writer, sheet_name='IHH_EXPORT_COMBINADO', float_format=formato_precisao_total)
        df_ihh_import_combinado.to_excel(writer, sheet_name='IHH_IMPORT_COMBINADO', float_format=formato_precisao_total)


    print("\n\n" + "="*50)
    print(f"SUCESSO! Todos os DataFrames foram salvos em:")
    print(f"'{nome_arquivo_excel}'")
    print("As abas de IHH agora estão combinadas nas abas 'IHH_EXPORT_COMBINADO' e 'IHH_IMPORT_COMBINADO'.")
    print("O IHH Total está na última linha de cada aba combinada.")
    print("="*50)
except Exception as e:
    print(f"\nERRO FATAL AO SALVAR O ARQUIVO: {e}")
    print("Verifique se os DataFrames não estão vazios após o processamento da API.")