import requests
import pandas as pd
import numpy as np
import os 
from dotenv import load_dotenv 

# Carrega variáveis de ambiente (token)
load_dotenv()


token = os.environ.get("MEU_TOKEN_API")
headers = {'Authorization': 'Bearer {}'.format(token)}

# --- API CALL (pesquisa/produto) ---
try:
    response = requests.get('https://laboratoriodefinancas.com/api/v2/pesquisa/produto', headers=headers)
    dados = response.json()
    df_produto = pd.DataFrame(dados) 
except Exception as e:
    print(f"Aviso: Falha na chamada API de produto: {e}")


# --- API CALL (pesquisa/operacao) ---
params = {
'id_produto': ('1365', '179') # 'Chocolate' e 'TOTAL'
}

api = 'https://laboratoriodefinancas.com/api/v2/pesquisa/operacao'
print("Buscando dados da API... Isso pode demorar um pouco.")
try:
    response = requests.get(api, params=params, headers=headers)
    dados = response.json()
    df = pd.DataFrame(dados)
    print("Dados recebidos.")
except Exception as e:
    print(f"ERRO: Falha na chamada da API de operação. Verifique o token. Detalhe: {e}")
    # Cria um DataFrame vazio para não quebrar o resto do script
    df = pd.DataFrame(columns=['nome_pais', 'id_pais', 'tipo_operacao', 'descricao', 'periodo', 'valor'])


#países a serem utilizados (VCR, VCRS, NEI, CAGR)
countries_limited = [
     "World", 
     "Germany",
     "Belgium",
     "Italy",
     "Poland",
     "Netherlands",
     "Canada",
     "United States of America",
     "France",
     "United Kingdom",
     "Switzerland",
     "Türkiye",
     "Russian Federation",
     "Mexico",
     "Spain",
     "Austria"
]


#série histórica
years = list(range(2015, 2025))
periods = [str(years) for years in range(2015, 2025)]

# --- PREPARAÇÃO GERAL ---

# Converte a coluna de valor para numérico, tratando erros
def safe_to_numeric(series):
    return pd.to_numeric(series, errors='coerce')

df.loc[:, 'valor'] = safe_to_numeric(df['valor'])

# Cria lista de países IHH (TODOS os países, exceto 'World', baseado no ID=1)
hhi_countries_all = df[df["id_pais"] != 1]["nome_pais"].unique()

# --- DADOS DE MUNDO (id_pais == 1) ---
df_world = df[df["id_pais"]==1].copy()

# Componentes Mundiais 
df_world_exported_chocolate = df_world[
     (df_world["tipo_operacao"] == "Exported") &       
     (df_world["descricao"] == "Chocolate and other food preparations containing cocoa") &
     (df_world["periodo"].isin(periods))
].copy() 

df_world_imported_chocolate = df_world[
     (df_world["tipo_operacao"] == "Imported") &       
     (df_world["descricao"] == "Chocolate and other food preparations containing cocoa") &
     (df_world["periodo"].isin(periods))
].copy() 

export_mundial_chocolate = df_world_exported_chocolate.set_index('periodo')['valor'].rename('export_mundial_chocolate')
import_mundial_chocolate = df_world_imported_chocolate.set_index('periodo')['valor'].rename('import_mundial_chocolate')
export_total_mundial = df_world[
    (df_world["tipo_operacao"] == "Exported") & 
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
].set_index('periodo')['valor'].rename('export_total_mundial')
export_total_mundial_df = df_world[
    (df_world["tipo_operacao"] == "Exported") & 
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
].set_index('periodo')['valor'].rename('export_total_mundial')


# --- DADOS DE PAÍSES LIMITADOS (VCR, VCRS, NEI, CAGR) ---
df_countries_limited = df[df["nome_pais"].isin(countries_limited)].copy()
df_countries_limited = df_countries_limited[df_countries_limited["id_pais"] != 1].copy() # Remove World

# Componentes de cálculo (LIMITADO)
export_chocolate_pelo_pais = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Exported") & 
    (df_countries_limited["descricao"] == "Chocolate and other food preparations containing cocoa") & 
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].set_index(['nome_pais', 'periodo'])['valor'].rename('export_chocolate_pelo_pais')

export_total_pelo_pais = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Exported") & 
    (df_countries_limited["descricao"] == "TOTAL - All products") & 
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].set_index(['nome_pais', 'periodo'])['valor'].rename('export_total_pelo_pais')

import_chocolate_pelo_pais = df_countries_limited[
    (df_countries_limited["tipo_operacao"]=="Imported") & 
    (df_countries_limited["descricao"] == "Chocolate and other food preparations containing cocoa") & 
    (df_countries_limited["periodo"].astype(str).isin(periods)) 
].set_index(['nome_pais', 'periodo'])['valor'].rename('import_chocolate_pelo_pais')


# --- DADOS DE PAÍSES TOTAIS (IHH) ---
df_countries_all = df[df["nome_pais"].isin(hhi_countries_all)].copy()

# Componentes de cálculo (TODOS PARA IHH)
export_chocolate_pelo_pais_all = df_countries_all[
    (df_countries_all["tipo_operacao"]=="Exported") & 
    (df_countries_all["descricao"] == "Chocolate and other food preparations containing cocoa") & 
    (df_countries_all["periodo"].astype(str).isin(periods)) 
].set_index(['nome_pais', 'periodo'])['valor'].rename('export_chocolate_pelo_pais_all')

import_chocolate_pelo_pais_all = df_countries_all[
    (df_countries_all["tipo_operacao"]=="Imported") & 
    (df_countries_all["descricao"] == "Chocolate and other food preparations containing cocoa") & 
    (df_countries_all["periodo"].astype(str).isin(periods)) 
].set_index(['nome_pais', 'periodo'])['valor'].rename('import_chocolate_pelo_pais_all')


# --- 3. COMBINAÇÃO DOS DADOS (PRINCIPAL - LIMITADO PARA VCR/NEI) ---

df_calc = pd.concat([export_chocolate_pelo_pais, export_total_pelo_pais, import_chocolate_pelo_pais], axis=1)

df_calc = (
     df_calc.reset_index()
     .merge(export_mundial_chocolate, on='periodo')
     .merge(export_total_mundial, on='periodo')
     .merge(import_mundial_chocolate, on='periodo')
)
df_calc = df_calc.set_index(['nome_pais', 'periodo'])
df_calc = df_calc.fillna(0)
df_calc.replace([np.inf, -np.inf], np.nan, inplace=True)


# --- 4. CÁLCULO DOS ÍNDICES (VCR, VCRS, NEI) ---

print("Calculando VCR, VCRS, NEI...")

participacao_pais_no_chocolate = df_calc['export_chocolate_pelo_pais'] / df_calc['export_total_pelo_pais']
participacao_mundo_no_chocolate = df_calc['export_mundial_chocolate'] / df_calc['export_total_mundial']
df_calc['VCR'] = participacao_pais_no_chocolate / participacao_mundo_no_chocolate
df_calc['VCRS'] = (df_calc['VCR'] - 1) / (df_calc['VCR'] + 1)
df_calc['NEI'] = np.where(
      (df_calc['export_chocolate_pelo_pais'] + df_calc['import_chocolate_pelo_pais']) == 0, 
      0, 
      (df_calc['export_chocolate_pelo_pais'] - df_calc['import_chocolate_pelo_pais']) / (df_calc['export_chocolate_pelo_pais'] + df_calc['import_chocolate_pelo_pais'])
)
df_calc.replace([np.inf, -np.inf], np.nan, inplace=True)


# --- 5. CÁLCULO DO CAGR (Taxa de Crescimento Anual Composta) ---

print("\n--- Calculando CAGR (2015-2024) para Exportações de Chocolate ---")

df_pivot_cagr = export_chocolate_pelo_pais.unstack('periodo')
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
df_ihh_calc = pd.concat([export_chocolate_pelo_pais_all, import_chocolate_pelo_pais_all], axis=1)

df_ihh_calc = (
    df_ihh_calc.reset_index()
    .merge(export_mundial_chocolate, on='periodo')
    .merge(import_mundial_chocolate, on='periodo')
)
df_ihh_calc = df_ihh_calc.set_index(['nome_pais', 'periodo'])
df_ihh_calc = df_ihh_calc.fillna(0)


# --- IHH para EXPORTADORES ---
export_mundial_chocolate_safe = df_ihh_calc['export_mundial_chocolate'].replace(0, np.nan)
df_ihh_calc['IHH_EXPORTADORES_CONTRIBUICAO'] = (
    (df_ihh_calc['export_chocolate_pelo_pais_all'] / export_mundial_chocolate_safe).fillna(0) * 100
) ** 2
ihh_exportadores_por_ano = df_ihh_calc.groupby('periodo')['IHH_EXPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_EXPORTADORES')

# --- IHH para IMPORTADORES ---
import_mundial_chocolate_safe = df_ihh_calc['import_mundial_chocolate'].replace(0, np.nan)
df_ihh_calc['IHH_IMPORTADORES_CONTRIBUICAO'] = (
    (df_ihh_calc['import_chocolate_pelo_pais_all'] / import_mundial_chocolate_safe).fillna(0) * 100
) ** 2
ihh_importadores_por_ano = df_ihh_calc.groupby('periodo')['IHH_IMPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_IMPORTADORES')


# --- 7. CRIAÇÃO E COMBINAÇÃO DOS DATAFRAMES FINAIS ---

# IHH - Contribuição (Por País/Ano)
df_ihh_exportadores_contribuicao = df_ihh_calc['IHH_EXPORTADORES_CONTRIBUICAO'].unstack('periodo')
df_ihh_importadores_contribuicao = df_ihh_calc['IHH_IMPORTADORES_CONTRIBUICAO'].unstack('periodo')

# IHH - Total Anual
df_ihh_exportadores_total = ihh_exportadores_por_ano.to_frame()
df_ihh_importadores_total = ihh_importadores_por_ano.to_frame()

# --- FUNÇÃO DE COMBINAÇÃO ---

def combine_hhi_sheets(df_contribution, df_total, total_index_name):
    """Combina o DF de Contribuição por País com o Total Anual IHH, adicionando o total na última linha."""
    
    df_total_T = df_total.T
    df_total_T.index = [total_index_name]
    
    common_cols = df_contribution.columns.intersection(df_total_T.columns)
    df_contribution_filtered = df_contribution[common_cols]
    df_total_T_filtered = df_total_T[common_cols]
    
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

df_vcr = df_calc['VCR'].unstack('periodo')
df_vcrs = df_calc['VCRS'].unstack('periodo')
df_nei = df_calc['NEI'].unstack('periodo')
df_cagr = cagr.to_frame()


nome_arquivo_excel = "analise_indices_chocolate.xlsx"
formato_precisao_total = "%.5f"

try:
    with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:

        # Índices VCR, VCRS, NEI, CAGR
        df_vcr.to_excel(writer, sheet_name='VCR', float_format=formato_precisao_total) 
        df_vcrs.to_excel(writer, sheet_name='VCRS', float_format=formato_precisao_total) 
        df_nei.to_excel(writer, sheet_name='NEI', float_format=formato_precisao_total) 
        df_cagr.to_excel(writer, sheet_name='CAGR', float_format=formato_precisao_total)

        # IHH COMBINADO (CONTRIBUIÇÃO DE TODOS OS PAÍSES + TOTAL)
        df_ihh_export_combinado.to_excel(writer, sheet_name='IHH_EXPORT_COMBINADO', float_format=formato_precisao_total)
        df_ihh_import_combinado.to_excel(writer, sheet_name='IHH_IMPORT_COMBINADO', float_format=formato_precisao_total)


    print("\n\n" + "="*50)
    print(f"SUCESSO! Todos os DataFrames foram salvos em:")
    print(f"'{nome_arquivo_excel}'")
    print("O IHH Total agora é calculado usando todos os países para maior precisão, e as abas VCR/NEI/CAGR usam apenas a lista limitada.")
    print("="*50)
except Exception as e:
    print(f"\nERRO FATAL AO SALVAR O ARQUIVO: {e}")
    print("Verifique se os DataFrames não estão vazios após o processamento da API.")