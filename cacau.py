import requests
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
load_dotenv()


token = os.environ.get("MEU_TOKEN_API")
headers = {'Authorization': 'Bearer {}'.format(token)}
response = requests.get('https://laboratoriodefinancas.com/api/v2/pesquisa/produto', headers=headers)
dados = response.json()
df = pd.DataFrame(dados)
# 00 para Mundo e 18 para cacau
df[df["cod2"].isin(["00", "18"])]


#PARA MUNDO E CACAU IMPORTED E EXPORTED
params = {
'id_produto': ('173', '1365')
}

api = 'https://laboratoriodefinancas.com/api/v2/pesquisa/operacao'
response = requests.get(api, params=params, headers=headers)
dados = response.json()
df = pd.DataFrame(dados)


#países a serem utilizados 
countries = [
    "World", "Germany", "Netherlands", "Côte d'Ivoire", "Belgium", "Italy",
    "Poland", "France", "United States of America", "Canada", "Malaysia",
    "Ghana", "Ecuador", "Indonesia", "United Kingdom", "Switzerland"
]

#aumento da série histórica
years = list(range(2015, 2025))
periods = [str(years) for years in range(2015, 2025)]
# periods = ['2020', '2021', '2022', '2023', '2024']

# --- MUNDO ---

#filtros
df = df[df["nome_pais"].isin(countries)].copy() # Adicionado .copy()
df_world = df[df["id_pais"]==1].copy() # Adicionado .copy()




'''
DADOS DE EXPORTAÇÃO

'''

df_world_exported_cacau = df_world[
    (df_world["tipo_operacao"] == "Exported") &
    (df_world["descricao"] == "Cocoa and cocoa preparations") &
    (df_world["periodo"].isin(periods))
].copy() # Adicionado .copy()

#filtro para exportação total do mundo (todos os commodites)
df_world_exported_total = df_world[
    (df_world["tipo_operacao"] == "Exported") & 
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
].copy()



'''
DADOS DE IMPORTAÇÃO

'''

df_world_imported_cacau = df_world[
    (df_world["tipo_operacao"] == "Imported") & 
    (df_world["descricao"] == "Cocoa and cocoa preparations") &
    (df_world["periodo"].isin(periods))
].copy() # Adicionado .copy()

#filtro para exportação total do mundo (todos os commodites)
df_world_imported_total = df_world[
    (df_world["tipo_operacao"] == "Imported") &
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
].copy()




# --- PAÍSES ---

'''
DADOS DE EXPORTAÇÃO

'''

#países exported para cacau
df_paises_exported_cacau = df[
    (df["nome_pais"].isin(countries)) & 
    (df["tipo_operacao"]=="Exported") & 
    (df["descricao"] == "Cocoa and cocoa preparations") & 
    (df["periodo"].astype(str).isin(periods)) 
].copy() # Adicionado .copy()

#países exported para todas os commodites
df_paises__exported_commodites = df[
    (df["nome_pais"].isin(countries)) & 
    (df["tipo_operacao"]=="Exported") & 
    (df["descricao"] == "TOTAL - All products") &   
    (df["periodo"].astype(str).isin(periods)) 
].copy() # Adicionado .copy()


'''
DADOS DE IMPORTAÇÃO

'''

#países imported para cacau
df_paises_imported_cacau = df[
    (df["nome_pais"].isin(countries)) & 
    (df["tipo_operacao"]=="Imported") &  
    (df["descricao"] == "Cocoa and cocoa preparations") &   
    (df["periodo"].astype(str).isin(periods)) 
].copy() # Adicionado .copy()

#países imported para todas os commodites
df_paises__imported_commodites = df[
    (df["nome_pais"].isin(countries)) & 
    (df["tipo_operacao"]=="Imported") &  
    (df["descricao"] == "TOTAL - All products") &   
    (df["periodo"].astype(str).isin(periods)) 
].copy() # Adicionado .copy()


# ---ÍNDICES ---


# --- 1. PREPARAÇÃO DOS DADOS ---

# Converte as colunas de valor para numérico, tratando erros
def safe_to_numeric(series):
    return pd.to_numeric(series, errors='coerce')

# Aplica a função safe_to_numeric usando .loc para evitar SettingWithCopyWarning
df_world_exported_cacau.loc[:, 'valor'] = safe_to_numeric(df_world_exported_cacau['valor'])
df_world_exported_total.loc[:, 'valor'] = safe_to_numeric(df_world_exported_total['valor'])

df_paises_exported_cacau.loc[:, 'valor'] = safe_to_numeric(df_paises_exported_cacau['valor'])
df_paises__exported_commodites.loc[:, 'valor'] = safe_to_numeric(df_paises__exported_commodites['valor'])

df_paises_imported_cacau.loc[:, 'valor'] = safe_to_numeric(df_paises_imported_cacau['valor'])

# --- 2. CRIAÇÃO DOS COMPONENTES DE CÁLCULO ---



# (export_cacau_pelo_pais) Exportações de Cacau por País (j) e Ano
export_cacau_pelo_pais = df_paises_exported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('export_cacau_pelo_pais')


# (export_totais_pelo_pais) Exportações Totais por País (j) e Ano
export_totais_pelo_pais = df_paises__exported_commodites.set_index(['nome_pais', 'periodo'])['valor'].rename('export_totais_pelo_pais')


# (import_cacau_pelo_pais) Importações de Cacau por País (j) e Ano
import_cacau_pelo_pais = df_paises_imported_cacau.set_index(['nome_pais', 'periodo'])['valor'].rename('import_cacau_pelo_pais')


# (export_cacau_mundial) Exportações Mundiais de Cacau por Ano
export_cacau_mundial = df_world_exported_cacau.set_index('periodo')['valor'].rename('export_cacau_mundial')


# (export_total_mundial) Exportações Totais Mundiais por Ano
export_total_mundial = df_world_exported_total.set_index('periodo')['valor'].rename('export_total_mundial')

# [NOVO COMPONENTE] (import_cacau_mundial) Importações Mundiais de Cacau por Ano
import_cacau_mundial = df_world_imported_cacau.set_index('periodo')['valor'].rename('import_cacau_mundial')


# --- 3. COMBINAÇÃO DOS DADOS ---



# Combina os dados dos países (Exportação e Importação por País)
df_calc = pd.concat([export_cacau_pelo_pais, export_totais_pelo_pais, import_cacau_pelo_pais], axis=1)

# Adiciona os dados mundiais, fazendo o merge pelo 'periodo'
df_calc = (
    df_calc.reset_index()
    .merge(export_cacau_mundial, on='periodo')
    .merge(export_total_mundial, on='periodo')
    .merge(import_cacau_mundial, on='periodo') # <--- CORRIGIDO: O merge que faltava
)
df_calc = df_calc.set_index(['nome_pais', 'periodo'])


# Preenche valores NaN (países que podem não importar cacau, por ex.) com 0 para evitar erros
df_calc = df_calc.fillna(0)


# Remove "World" da lista de países para não calcular VCR para ele mesmo
df_calc = df_calc.drop(index='World', level='nome_pais', errors='ignore')



# --- 4. CÁLCULO DOS ÍNDICES (VCR, VCRS, NEI) ---




# VCR (Vantagem Comparativa Revelada)
# VCR = (export_cacau_pelo_pais / export_totais_pelo_pais) / (export_cacau_mundial / export_total_mundial)
participacao_pais_no_cacau = df_calc['export_cacau_pelo_pais'] / df_calc['export_totais_pelo_pais']
participacao_mundo_no_cacau = df_calc['export_cacau_mundial'] / df_calc['export_total_mundial']
df_calc['VCR'] = participacao_pais_no_cacau / participacao_mundo_no_cacau

# VCRS (Vantagem Comparativa Revelada Simétrica)
# VCRS = (VCR - 1) / (VCR + 1)
df_calc['VCRS'] = (df_calc['VCR'] - 1) / (df_calc['VCR'] + 1)

# NEI (Índice de Exportação Líquida)
# NEI = (export_cacau_pelo_pais - import_cacau_pelo_pais) / (export_cacau_pelo_pais + import_cacau_pelo_pais)
# Usamos np.where para evitar divisão por zero se X+M == 0
df_calc['NEI'] = np.where(
    (df_calc['export_cacau_pelo_pais'] + df_calc['import_cacau_pelo_pais']) == 0, 
    0, 
    (df_calc['export_cacau_pelo_pais'] - df_calc['import_cacau_pelo_pais']) / (df_calc['export_cacau_pelo_pais'] + df_calc['import_cacau_pelo_pais'])
)

# Substitui valores infinitos (caso export_totais_pelo_pais ou export_total_mundial sejam 0) por NaN
df_calc.replace([np.inf, -np.inf], np.nan, inplace=True)




# --- 5. CÁLCULO DO CAGR (Taxa de Crescimento Anual Composta) ---

# Pivota os dados de exportação de cacau (export_cacau_pelo_pais)
df_pivot_cagr = export_cacau_pelo_pais.unstack('periodo')


# Seleciona os anos inicial e final
valor_inicial = df_pivot_cagr['2015']
valor_final = df_pivot_cagr['2024']
anos = 2024 - 2015

# Fórmula CAGR: ((Valor Final / Valor Inicial) ** (1 / N_Anos)) - 1
# Usamos np.where para definir CAGR como NaN se o valor inicial for 0.
cagr = np.where(
    valor_inicial == 0,
    np.nan, 
    ((valor_final / valor_inicial) ** (1 / anos)) - 1
)
cagr = pd.Series(cagr, index=valor_inicial.index) # Converte de volta para Series
cagr = cagr.rename('CAGR (2015-2024)').drop('World', errors='ignore')


# --- 6. CÁLCULO DO IHH (Índice Herfindahl-Hirschman) ---

# --- IHH para EXPORTADORES ---
# (s_j) Market Share de cada país (j) no mercado mundial de cacau (i)
df_calc['market_share_export'] = (df_calc['export_cacau_pelo_pais'] / df_calc['export_cacau_mundial'])

# (s_j)^2 * 10000 (IHH para Exportadores por País/Ano)
df_calc['IHH_EXPORTADORES_CONTRIBUICAO'] = (df_calc['market_share_export'] * 100) ** 2

# IHH Anual Total para Exportadores (Soma das Contribuições)
ihh_exportadores_por_ano = df_calc.groupby('periodo')['IHH_EXPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_EXPORTADORES')

# --- IHH para IMPORTADORES ---
# Participação de Mercado dos Importadores
df_calc['market_share_import'] = (df_calc['import_cacau_pelo_pais'] / df_calc['import_cacau_mundial'])

# (s_j)^2 * 10000 (IHH para Importadores por País/Ano)
df_calc['IHH_IMPORTADORES_CONTRIBUICAO'] = (df_calc['market_share_import'] * 100) ** 2

# IHH Anual Total para Importadores (Soma das Contribuições)
ihh_importadores_por_ano = df_calc.groupby('periodo')['IHH_IMPORTADORES_CONTRIBUICAO'].sum().rename('IHH_TOTAL_IMPORTADORES')


# --- IHH Total do Período (Soma das Contribuições por País) ---

# Soma a contribuição do IHH de cada país ao longo de todos os anos (2015-2024)
ihh_total_periodo_export = (
    df_calc['IHH_EXPORTADORES_CONTRIBUICAO']
    .groupby('nome_pais').sum()
    .rename('IHH_SOMA_PERIODO_EXPORT')
)

ihh_total_periodo_import = (
    df_calc['IHH_IMPORTADORES_CONTRIBUICAO']
    .groupby('nome_pais').sum()
    .rename('IHH_SOMA_PERIODO_IMPORT')
)


# --- 7. EXIBIÇÃO DOS RESULTADOS ---

print("\n\n" + "="*70)
print("RESULTADOS - IHH TOTAL DO PERÍODO (Soma da Contribuição por País, 2015-2024)")
print("="*70)
print(ihh_total_periodo_export.to_frame())
print("-" * 35)
print(ihh_total_periodo_import.to_frame())
print("="*70)

print("\n\n" + "="*70)
print("RESULTADOS - Índices Anuais por País (VCR, VCRS, NEI)")
print("="*70)
try:
    print(df_calc[['VCR', 'VCRS', 'NEI']].unstack('periodo'))
except Exception as e:
    print(f"Não foi possível pivotar a tabela. Exibindo formato longo:\n{e}\n")
    print(df_calc[['VCR', 'VCRS', 'NEI']])


print("\n\n" + "="*70)
print("RESULTADO - CAGR (2015-2024) por País")
print("="*70)
print(cagr)

print("\n\n" + "="*70)
print("RESULTADOS - Contribuição IHH EXPORTADORES por País e Ano (s_j^2 * 100^2)")
print("="*70)
try:
    print(df_calc['IHH_EXPORTADORES_CONTRIBUICAO'].unstack('periodo'))
except Exception as e:
    print(f"Não foi possível pivotar a tabela. Exibindo formato longo:\n{e}\n")
    print(df_calc['IHH_EXPORTADORES_CONTRIBUICAO'])

print("\n\n" + "="*70)
print("RESULTADOS - Contribuição IHH IMPORTADORES por País e Ano (s_j^2 * 100^2)")
print("="*70)
try:
    print(df_calc['IHH_IMPORTADORES_CONTRIBUICAO'].unstack('periodo'))
except Exception as e:
    print(f"Não foi possível pivotar a tabela. Exibindo formato longo:\n{e}\n")
    print(df_calc['IHH_IMPORTADORES_CONTRIBUICAO'])

print("\n\n" + "="*70)
print("RESULTADO - IHH TOTAL ANUAL (Soma da Concentração de Mercado) por Ano")
print("="*70)
print(ihh_exportadores_por_ano.to_frame())
print("-" * 70)
print(ihh_importadores_por_ano.to_frame())
print("="*70)

print("\n\nCálculos concluídos.")


# --- 8. TRANSFORMANDO DADOS EM DF ---

# ... (df_vcr, df_vcrs, df_nei, df_cagr) ...
df_vcr = df_calc['VCR'].unstack('periodo')
df_vcrs = df_calc['VCRS'].unstack('periodo')
df_nei = df_calc['NEI'].unstack('periodo')
df_cagr = cagr.to_frame()

# --- IHH CONTRIBUIÇÃO (Por País/Ano) ---
df_ihh_exportadores_contribuicao = df_calc['IHH_EXPORTADORES_CONTRIBUICAO'].unstack('periodo')
df_ihh_importadores_contribuicao = df_calc['IHH_IMPORTADORES_CONTRIBUICAO'].unstack('periodo')

# --- IHH TOTAL ANUAL ---
df_ihh_exportadores_total = ihh_exportadores_por_ano.to_frame()
df_ihh_importadores_total = ihh_importadores_por_ano.to_frame()

# --- NOVO: IHH TOTAL DO PERÍODO (Soma da Contribuição por País) ---
df_ihh_soma_periodo_export = ihh_total_periodo_export.to_frame()
df_ihh_soma_periodo_import = ihh_total_periodo_import.to_frame()


print("\n\nCálculos concluídos. Preparando para salvar em Excel...")


# --- 9. SALVAR OS RESULTADOS EM ARQUIVO EXCEL ---

# Define o nome do arquivo Excel de saída
nome_arquivo_excel = "analise_indices_cacau_completo.xlsx"

# Define um formato de string para alta precisão 
formato_precisao_total = "%.5f"


# Cria o "escritor" de Excel usando o context manager
with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:

    # Índices VCR, VCRS, NEI, CAGR
    df_vcr.to_excel(writer, sheet_name='VCR', float_format=formato_precisao_total) 
    df_vcrs.to_excel(writer, sheet_name='VCRS', float_format=formato_precisao_total) 
    df_nei.to_excel(writer, sheet_name='NEI', float_format=formato_precisao_total) 
    df_cagr.to_excel(writer, sheet_name='CAGR', float_format=formato_precisao_total)

    # IHH - Exportadores
    df_ihh_exportadores_contribuicao.to_excel(writer, sheet_name='IHH_EXPORT_PAIS', float_format=formato_precisao_total)
    df_ihh_exportadores_total.to_excel(writer, sheet_name='IHH_EXPORT_TOTAL_ANUAL', float_format=formato_precisao_total)

    # IHH - Importadores
    df_ihh_importadores_contribuicao.to_excel(writer, sheet_name='IHH_IMPORT_PAIS', float_format=formato_precisao_total)
    df_ihh_importadores_total.to_excel(writer, sheet_name='IHH_IMPORT_TOTAL_ANUAL', float_format=formato_precisao_total)

    # IHH Total do Período (Soma da Contribuição por País)
    df_ihh_soma_periodo_export.to_excel(writer, sheet_name='IHH_SOMA_PERIODO_EXPORT', float_format=formato_precisao_total)
    df_ihh_soma_periodo_import.to_excel(writer, sheet_name='IHH_SOMA_PERIODO_IMPORT', float_format=formato_precisao_total)


print("\n\n" + "="*50)
print(f"SUCESSO! Todos os DataFrames foram salvos em:")
print(f"'{nome_arquivo_excel}'")
print("Novas abas adicionadas com a SOMA da Contribuição IHH por país no período:")
print("- 'IHH_SOMA_PERIODO_EXPORT'")
print("- 'IHH_SOMA_PERIODO_IMPORT'")
print("="*50)