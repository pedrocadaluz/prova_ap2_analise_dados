import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------
# 1. SETUP E CHAMADA À API (MANTIDO)
# ----------------------------------------------------------------------

token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ0b2tlbl90eXBlIjoiYWNjZXNzIiwiZXhwIjoxNzYzODEwNjQ5LCJpYXQiOjE3NjEyMTg2NDksImp0aSI6IjZkYTVlMjFkYzlhMzRjYWE4NTY2MjhlM2VkMGE5MjYzIiwidXNlcl9pZCI6IjQyIn0.kkpvpIkneag5Ijvy4N_5HdlvxNAsHR6ejTZHeS6Kc60"
headers = {'Authorization': 'Bearer {}'.format(token)}
params = {'id_produto': ('173', '1365')}
api = 'https://laboratoriodefinancas.com/api/v2/pesquisa/operacao'

try:
    response = requests.get(api, params=params, headers=headers)
    response.raise_for_status()
    dados = response.json()
    df = pd.DataFrame(dados)
    df['periodo'] = df['periodo'].astype(str)
except requests.exceptions.RequestException as e:
    print(f"ERRO CRÍTICO: Não foi possível carregar os dados principais da API. Usando DataFrame Vazio. {e}")
    df = pd.DataFrame(columns=['nome_pais', 'id_pais', 'tipo_operacao', 'descricao', 'periodo', 'valor'])

countries = [
    "World", "Germany", "Netherlands", "Côte d'Ivoire", "Belgium", "Italy",
    "Poland", "France", "United States of America", "Canada", "Malaysia",
    "Ghana", "Ecuador", "Indonesia", "United Kingdom", "Switzerland"
]
periods = ['2020', '2021', '2022', '2023', '2024']

df = df[df["nome_pais"].isin(countries)]
df_world = df[df["id_pais"]==1]

def filter_df(base_df, operation, description):
    return base_df[
        (base_df["tipo_operacao"] == operation) &
        (base_df["descricao"] == description) &
        (base_df["periodo"].astype(str).isin(periods))
    ]

# DataFrames do Mundo e Países
df_world_exported_cacau = filter_df(df_world, "Exported", "Cocoa and cocoa preparations")
df_world_exported_total = filter_df(df_world, "Exported", "TOTAL - All products")
df_paises_exported_cacau = filter_df(df, "Exported", "Cocoa and cocoa preparations")
df_paises__exported_commodites = filter_df(df, "Exported", "TOTAL - All products")
df_paises_imported_cacau = filter_df(df, "Imported", "Cocoa and cocoa preparations")
df_paises__imported_commodites = filter_df(df, "Imported", "TOTAL - All products")
df_world_imported_cacau = filter_df(df_world, "Imported", "Cocoa and cocoa preparations")
df_world_imported_total = filter_df(df_world, "Imported", "TOTAL - All products")

# ----------------------------------------------------------------------
# 3. FUNÇÕES DOS INDICADORES (NOVOS CÁLCULOS DE CRESCIMENTO)
# ----------------------------------------------------------------------

def calculate_growth_metrics_2020_2024(df_data, value_col='valor', period_col='periodo', initial_year=2020, final_year=2024):
    """Calcula Diferença, Diferença %, Cresc. em % e CAGR fixos (2020-2024) por País."""
    if df_data.empty: return pd.DataFrame(columns=['Diferença', 'Diferença %', 'Cresc. em %', 'CAGR'])

    df_data = df_data.copy()
    df_data[period_col] = pd.to_numeric(df_data[period_col], errors='coerce')
    
    df_agg = df_data.groupby(['nome_pais', period_col])[value_col].sum().reset_index()
    df_pivot = df_agg.pivot(index='nome_pais', columns=period_col, values=value_col).fillna(0)
    
    T = final_year - initial_year # T = 4 anos

    # Obter valores de 2020 e 2024 (usa .get para lidar com países sem dados no ano)
    V_initial = df_pivot.get(initial_year, pd.Series(0, index=df_pivot.index))
    V_final = df_pivot.get(final_year, pd.Series(0, index=df_pivot.index))
    
    # DataFrame de resultados
    df_results = pd.DataFrame(index=df_pivot.index)

    # 1. Diferença (V2024 - V2020)
    df_results['Diferença'] = V_final - V_initial

    # Define a condição de divisor zero para V2024 (para Diferença % e Cresc. em %) e V2020 (para CAGR)
    cond_vf_nao_zero = V_final != 0
    cond_vi_nao_zero = V_initial != 0
    
    # 2. Diferença % (V2024 - V2020) / V2024
    df_results['Diferença %'] = np.where(
        cond_vf_nao_zero,
        (V_final - V_initial) / V_final,
        np.nan
    )
    
    # 3. Cresc. em % (Diferença / 4) / V2024
    df_results['Cresc. em %'] = np.where(
        cond_vf_nao_zero,
        ((V_final - V_initial) / T) / V_final,
        np.nan
    )
    
    # 4. CAGR Padrão: (Vf / Vi)^(1/T) - 1
    df_results['CAGR'] = np.where(
        cond_vi_nao_zero & (T > 0),
        np.power(V_final / V_initial, 1/T) - 1,
        np.nan
    )
    
    # Limpar valores extremos
    df_results = df_results.replace([np.inf, -np.inf], np.nan)
    return df_results


# --- (Funções VCR, NEI, IHH e AGR Anual Omitidas por brevidade, mas devem estar no seu código) ---
# ... (Seu código completo com VCR, NEI, IHH e AGR)
# ----------------------------------------------------------------------

# 4. APLICAÇÃO DOS CÁLCULOS
# ----------------------------------------------------------------------

# A. Novos Cálculos de Crescimento (Cacau Exportado)
df_novos_indicadores_crescimento = calculate_growth_metrics_2020_2024(df_paises_exported_cacau)

# B. VCR e VCRS (usando sua função anterior - mantido o cálculo por País/Ano)
# df_vcr_annual = calculate_vcr_annual(...)

# C. NEI (usando sua função anterior - mantido o cálculo por País/Ano)
# df_nei_paises = calculate_nei(...)

# D. IHH (usando sua função anterior - mantido o cálculo por Ano)
# ihh_exported_cacau = calculate_ihh(...)


# ----------------------------------------------------------------------
# 5. EXIBIÇÃO DOS NOVOS RESULTADOS DE CRESCIMENTO
# ----------------------------------------------------------------------

print("\n=========================================================")
print("NOVOS INDICADORES DE CRESCIMENTO (2020 vs 2024) - CACAU EXPORTADO")
print("=========================================================")

print("\nCálculos de Diferença e CAGR Fixo (2020-2024):")
print("-" * 50)
print(df_novos_indicadores_crescimento.to_string(float_format=lambda x: f'{x:,.4f}' if abs(x) < 1 else f'{x:,.0f}'))

# Notas sobre os cálculos (para você):
print("\nNota sobre as Fórmulas Implementadas:")
print("1. Diferença: V2024 - V2020")
print("2. Diferença %: (V2024 - V2020) / V2024")
print("3. Cresc. em %: ((V2024 - V2020) / 4) / V2024")
print("4. CAGR: (V2024 / V2020) ^ (1/4) - 1")
print("Valores NaN resultam de divisão por zero (V2024=0 ou V2020=0).") 