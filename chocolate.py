import requests
import pandas as pd
import os  
from dotenv import load_dotenv  
import numpy as np 


load_dotenv()
token = os.environ.get("MEU_TOKEN_API")
headers = {'Authorization': 'Bearer {}'.format(token)}

# --- API CALL (pesquisa/produto) ---
response = requests.get('https://laboratoriodefinancas.com/api/v2/pesquisa/produto', headers=headers)
dados = response.json()
df = pd.DataFrame(dados) 
# 00 para Mundo e 18 para cacau
df[df["cod2"].isin(["00", "18"])]


# --- API CALL (pesquisa/operacao) ---
params = {
'id_produto': ('1365', '179') # 'Chocolate' e 'TOTAL'
}

api = 'https://laboratoriodefinancas.com/api/v2/pesquisa/operacao'
print("Buscando dados da API... Isso pode demorar um pouco.")
response = requests.get(api, params=params, headers=headers)
dados = response.json()
df = pd.DataFrame(dados)
print("Dados recebidos.")

#países a serem utilizados 
countries = [
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

periods = ['2020', '2021', '2022', '2023', '2024'] # anos a serem utilizados

#--- MUNDO ---
#filtros
df = df[df["nome_pais"].isin(countries)]
df_world = df[df["id_pais"]==1]

'''
DADOS DE EXPORTAÇÃO (MUNDO)
'''
df_world_exported_chocolate = df_world[
    (df_world["tipo_operacao"] == "Exported") &       
    (df_world["descricao"] == "Chocolate and other food preparations containing cocoa") &
    (df_world["periodo"].isin(periods))
]

#filtro para exportação total do mundo (todos os commodites)
df_world_exported_total = df_world[
    (df_world["tipo_operacao"] == "Exported") &       
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
]

'''
DADOS DE IMPORTAÇÃO (MUNDO)
'''
df_world_imported_chocolate = df_world[
    (df_world["tipo_operacao"] == "Imported") &       
    (df_world["descricao"] == "Chocolate and other food preparations containing cocoa") &
    (df_world["periodo"].isin(periods))
]

#filtro para exportação total do mundo (todos os commodites)
df_world_imported_total = df_world[
    (df_world["tipo_operacao"] == "Imported") &       
    (df_world["descricao"] == "TOTAL - All products") &
    (df_world["periodo"].isin(periods))
]

# --- PAÍSES ---
'''
DADOS DE EXPORTAÇÃO (PAÍSES)
'''
#países exported para chocolate
df_paises_exported_chocolate = df[
    (df["nome_pais"].isin(countries)) &               
    (df["tipo_operacao"]=="Exported") &         
    (df["descricao"] == "Chocolate and other food preparations containing cocoa") &           
    (df["periodo"].astype(str).isin(periods))         
]

#países exported para todas os commodites
df_paises__exported_commodites = df[
    (df["nome_pais"].isin(countries)) &               
    (df["tipo_operacao"]=="Exported") &         
    (df["descricao"] == "TOTAL - All products") &           
    (df["periodo"].astype(str).isin(periods))         
]

'''
DADOS DE IMPORTAÇÃO (PAÍSES)
'''
#países imported para chocolate
df_paises_imported_chocolate= df[
    (df["nome_pais"].isin(countries)) &               
    (df["tipo_operacao"]=="Imported") &         
    (df["descricao"] == "Chocolate and other food preparations containing cocoa") &           
    (df["periodo"].astype(str).isin(periods))         
]

#países imported para todas os commodites
df_paises__imported_commodites = df[
    (df["nome_pais"].isin(countries)) &               
    (df["tipo_operacao"]=="Imported") &         
    (df["descricao"] == "TOTAL - All products") &           
    (df["periodo"].astype(str).isin(periods))         
]


# --- CÁLCULOS DOS ÍNDICES ---


# --- 1. PREPARAÇÃO DOS DADOS ---

# Converte as colunas de valor para numérico, tratando erros
def safe_to_numeric(series):
    return pd.to_numeric(series, errors='coerce')

# Adiciona .copy() para evitar SettingWithCopyWarning
df_world_exported_chocolate = df_world_exported_chocolate.copy()
df_world_exported_total = df_world_exported_total.copy()
df_paises_exported_chocolate = df_paises_exported_chocolate.copy()
df_paises__exported_commodites = df_paises__exported_commodites.copy()
df_paises_imported_chocolate = df_paises_imported_chocolate.copy()


# Mundo
df_world_exported_chocolate['valor'] = safe_to_numeric(df_world_exported_chocolate['valor'])
df_world_exported_total['valor'] = safe_to_numeric(df_world_exported_total['valor'])

# Países - Exportação
df_paises_exported_chocolate['valor'] = safe_to_numeric(df_paises_exported_chocolate['valor'])
df_paises__exported_commodites['valor'] = safe_to_numeric(df_paises__exported_commodites['valor'])

# Países - Importação
df_paises_imported_chocolate['valor'] = safe_to_numeric(df_paises_imported_chocolate['valor'])


# --- 2. CRIAÇÃO DOS COMPONENTES DE CÁLCULO ---


# (X_ij) Exportações de Chocolate por País (j) e Ano
X_ij = df_paises_exported_chocolate.set_index(['nome_pais', 'periodo'])['valor'].rename('X_ij')

# (X_tj) Exportações Totais por País (j) e Ano
X_tj = df_paises__exported_commodites.set_index(['nome_pais', 'periodo'])['valor'].rename('X_tj')

# (M_ij) Importações de Chocolate por País (j) e Ano
M_ij = df_paises_imported_chocolate.set_index(['nome_pais', 'periodo'])['valor'].rename('M_ij')

# (X_iw) Exportações Mundiais de Chocolate por Ano
# Esta era a (Correção 2): Usar o DataFrame do MUNDO, não dos países
X_iw = df_world_exported_chocolate.set_index('periodo')['valor'].rename('X_iw')

# (X_tw) Exportações Totais Mundiais por Ano
X_tw = df_world_exported_total.set_index('periodo')['valor'].rename('X_tw')


# --- 3. COMBINAÇÃO DOS DADOS ---


# Combina os dados dos países
df_calc = pd.concat([X_ij, X_tj, M_ij], axis=1)

# Adiciona os dados mundiais, fazendo o merge pelo 'periodo'
df_calc = df_calc.reset_index().merge(X_iw, on='periodo').merge(X_tw, on='periodo')
df_calc = df_calc.set_index(['nome_pais', 'periodo'])

# Preenche valores NaN (países que podem não importar, por ex.) com 0 para evitar erros
df_calc = df_calc.fillna(0)

# Remove "World" da lista de países para não calcular VCR para ele mesmo
df_calc = df_calc.drop(index='World', level='nome_pais', errors='ignore')


# --- 4. CÁLCULO DOS ÍNDICES (VCR, VCRS, NEI) ---

print("Calculando VCR, VCRS, NEI...")

# VCR (Vantagem Comparativa Revelada)
# VCR = (X_ij / X_tj) / (X_iw / X_tw)
participacao_pais_no_chocolate = df_calc['X_ij'] / df_calc['X_tj']
participacao_mundo_no_chocolate = df_calc['X_iw'] / df_calc['X_tw']
df_calc['VCR'] = participacao_pais_no_chocolate / participacao_mundo_no_chocolate

# VCRS (Vantagem Comparativa Revelada Simétrica)
# VCRS = (VCR - 1) / (VCR + 1)
df_calc['VCRS'] = (df_calc['VCR'] - 1) / (df_calc['VCR'] + 1)

# NEI (Índice de Exportação Líquida)
# NEI = (X_ij - M_ij) / (X_ij + M_ij)
# Usamos np.where para evitar divisão por zero se X+M == 0
df_calc['NEI'] = np.where(
    (df_calc['X_ij'] + df_calc['M_ij']) == 0, 
    0, 
    (df_calc['X_ij'] - df_calc['M_ij']) / (df_calc['X_ij'] + df_calc['M_ij'])
)

# Substitui valores infinitos (caso X_tj ou X_tw sejam 0) por NaN
df_calc.replace([np.inf, -np.inf], np.nan, inplace=True)


# --- 5. CÁLCULO DO CAGR (Taxa de Crescimento Anual Composta) ---

print("\n--- Calculando CAGR (2020-2024) para Exportações de Chocolate ---")

# Pivota os dados de exportação de chocolate (X_ij)
df_pivot_cagr = X_ij.unstack('periodo')

# [VERIFICAÇÃO] Valores de 2020 e 2024 usados pelo Python:
print("\n[VERIFICAÇÃO] Valores de 2020 e 2024 usados pelo Python:")
print(df_pivot_cagr[['2020', '2024']])


# Seleciona os anos inicial e final
valor_inicial = df_pivot_cagr['2020']
valor_final = df_pivot_cagr['2024']
anos = 2024 - 2020

# Fórmula CAGR = ((Valor Final / Valor Inicial) ** (1 / N_Anos)) - 1
cagr = ((valor_final / valor_inicial) ** (1 / anos)) - 1  
cagr = cagr.rename('CAGR (2020-2024)').drop('World', errors='ignore')


# --- 6. CÁLCULO DO IHH (Índice Herfindahl-Hirschman) ---

print("\n--- Calculando IHH (Concentração de Mercado) por Ano ---")


df_calc['market_share'] = (df_calc['X_ij'] / df_calc['X_iw'])
df_calc['market_share_pct_sq'] = (df_calc['market_share'] * 100) ** 2

# Soma os quadrados das participações de todos os países (exceto "World") por ano
ihh_por_ano = df_calc.groupby('periodo')['market_share_pct_sq'].sum().rename('IHH')


# --- 7. EXIBIÇÃO DOS RESULTADOS ---

print("\n\n" + "="*50)
print("RESULTADOS - Índices Anuais por País (VCR, VCRS, NEI)")
print("="*50)
# Usamos o 'unstack' para facilitar a visualização
try:
    print(df_calc[['VCR', 'VCRS', 'NEI']].unstack('periodo'))
except Exception as e:
    print(f"Não foi possível pivotar a tabela. Exibindo formato longo:\n{e}\n")
    print(df_calc[['VCR', 'VCRS', 'NEI']])


print("\n\n" + "="*50)
print("RESULTADO - CAGR (2020-2024) por País")
print("="*50)
print(cagr)


print("\n\n" + "="*50)
print("RESULTADO - IHH (Concentração de Mercado) por Ano")
print("="*50)
print(ihh_por_ano)

print("\n\nCálculos concluídos.")



# --- 8. TRANSFORMANDO DADOS EM DF ---


# --- DataFrame 'df_vcr' ---
print("\n--- Criando DataFrame 'df_vcr' (Formato Pivotado) ---")
# Seleciona a coluna VCR e pivota os períodos para colunas
df_vcr = df_calc['VCR'].unstack('periodo')
print(df_vcr)

# --- DataFrame 'df_vcrs' ---
print("\n--- Criando DataFrame 'df_vcrs' (Formato Pivotado) ---")
# Seleciona a coluna VCRS e pivota os períodos para colunas
df_vcrs = df_calc['VCRS'].unstack('periodo')
print(df_vcrs)

# --- DataFrame 'df_nei' ---
print("\n--- Criando DataFrame 'df_nei' (Formato Pivotado) ---")
# Seleciona a coluna NEI e pivota os períodos para colunas
df_nei = df_calc['NEI'].unstack('periodo')
print(df_nei)

# --- DataFrame 'df_cagr' ---
# 'cagr' é uma Series, convertemos para DataFrame
print("\n--- DataFrame 'df_cagr' ---")
df_cagr = cagr.to_frame()
print(df_cagr)

# --- DataFrame 'df_ihh' ---
# 'ihh_por_ano' é uma Series, convertemos para DataFrame
print("\n--- DataFrame 'df_ihh' ---")
df_ihh = ihh_por_ano.to_frame()
print(df_ihh)


print("\n\nCálculos concluídos. Preparando para salvar em Excel...")


# --- 9. SALVAR OS RESULTADOS EM ARQUIVO EXCEL ---

# Define o nome do arquivo Excel de saída
nome_arquivo_excel = "analise_indices_chocolate.xlsx"

# Define um formato de string para alta precisão 
formato_precisao_total = "%.5f"


# Cria o "escritor" de Excel usando o context manager
with pd.ExcelWriter(nome_arquivo_excel, engine='openpyxl') as writer:
    
    # Escreve cada DataFrame em uma aba (sheet) diferente
    # Adicionamos o 'float_format' para evitar arredondamento VISUAL no Excel
    
    df_vcr.to_excel(writer, sheet_name='VCR', 
                    float_format=formato_precisao_total)
                    
    df_vcrs.to_excel(writer, sheet_name='VCRS', 
                     float_format=formato_precisao_total)
                     
    df_nei.to_excel(writer, sheet_name='NEI', 
                    float_format=formato_precisao_total)
                     
    df_cagr.to_excel(writer, sheet_name='CAGR', 
                     float_format=formato_precisao_total)
                     
    df_ihh.to_excel(writer, sheet_name='IHH', 
                    float_format=formato_precisao_total)

print("\n\n" + "="*50)
print(f"SUCESSO! Os 5 DataFrames foram salvos em:")
print(f"'{nome_arquivo_excel}'")
print("Cada índice está em uma aba separada e com precisão total.")
print("="*50)