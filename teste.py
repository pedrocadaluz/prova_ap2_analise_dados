import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------------------------
# 1. SETUP E CHAMADA À API
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
    print(f"ERRO CRÍTICO: Não foi possível carregar os dados principais da API. {e}")
    # Cria um DataFrame vazio se a API falhar para evitar quebrar o código
    df = pd.DataFrame(columns=['nome_pais', 'id_pais', 'tipo_operacao', 'descricao', 'periodo', 'valor'])

countries = ["World", "Germany", "Netherlands", "Côte d'Ivoire", "Belgium", "Italy", "Poland", "France", "United States of America", "Canada", "Malaysia", "Ghana", "Ecuador", "Indonesia", "United Kingdom", "Switzerland"]
periods = ['2020', '2021', '2022', '2023', '2024']

df = df[df["nome_pais"].isin(countries)]
df_world = df[df["id_pais"]==1]

def filter_df(base_df, operation, description):
    return base_df[
        (base_df["tipo_operacao"] == operation) &
        (base_df["descricao"] == description) &
        (base_df["periodo"].astype(str).isin(periods))
    ]

# DataFrames do Mundo
df_world_exported_cacau = filter_df(df_world, "Exported", "Cocoa and cocoa preparations")
df_world_exported_total = filter_df(df_world, "Exported", "TOTAL - All products")
df_world_imported_cacau = filter_df(df_world, "Imported", "Cocoa and cocoa preparations")
df_world_imported_total = filter_df(df_world, "Imported", "TOTAL - All products")

# DataFrames dos Países
df_paises_exported_cacau = filter_df(df, "Exported", "Cocoa and cocoa preparations")
df_paises__exported_commodites = filter_df(df, "Exported", "TOTAL - All products")
df_paises_imported_cacau = filter_df(df, "Imported", "Cocoa and cocoa preparations")
df_paises__imported_commodites = filter_df(df, "Imported", "TOTAL - All products")


# ----------------------------------------------------------------------
# 2. FUNÇÕES DOS INDICADORES
# ----------------------------------------------------------------------

def calculate_cagr_core(df_data, value_col='valor', period_col='periodo'):
    if df_data.empty: return np.nan
    df_data = df_data.copy()
    df_data[period_col] = pd.to_numeric(df_data[period_col], errors='coerce')
    df_data = df_data.dropna(subset=[period_col])
    df_agg = df_data.groupby(period_col)[value_col].sum().reset_index()
    if df_agg.shape[0] < 2: return 0.0
    df_agg = df_agg.sort_values(by=period_col)
    V_final = df_agg.iloc[-1][value_col]
    V_initial = df_agg.iloc[0][value_col]
    T = df_agg.iloc[-1][period_col] - df_agg.iloc[0][period_col]
    if T <= 0 or V_initial == 0: return 0.0
    return ((V_final / V_initial) ** (1/T)) - 1

def calculate_vcr_annual(df_paises_cacau, df_paises_total, df_world_cacau, df_world_total, value_col='valor'):
    exp_cacau_paises_annual = df_paises_cacau.groupby(['nome_pais', 'periodo'])[value_col].sum().rename('E_i_j_t')
    exp_total_paises_annual = df_paises_total.groupby(['nome_pais', 'periodo'])[value_col].sum().rename('E_i_T_t')
    exp_cacau_world_annual = df_world_cacau.groupby('periodo')[value_col].sum().rename('E_W_j_t')
    exp_total_world_annual = df_world_total.groupby('periodo')[value_col].sum().rename('E_W_T_t')

    df_vcr_annual = pd.merge(exp_cacau_paises_annual, exp_total_paises_annual, on=['nome_pais', 'periodo'], how='outer').fillna(0)
    df_world_annual = pd.merge(exp_cacau_world_annual.reset_index(), exp_total_world_annual.reset_index(), on='periodo', how='outer').fillna(0)
    df_vcr_annual = pd.merge(df_vcr_annual.reset_index(), df_world_annual, on='periodo', how='left').set_index(['nome_pais', 'periodo']).fillna(0)
    
    share_pais = np.where(df_vcr_annual['E_i_T_t'] != 0, df_vcr_annual['E_i_j_t'] / df_vcr_annual['E_i_T_t'], 0)
    share_world = np.where(df_vcr_annual['E_W_T_t'] != 0, df_vcr_annual['E_W_j_t'] / df_vcr_annual['E_W_T_t'], 0)
    
    df_vcr_annual['VCR'] = np.where(share_world != 0, share_pais / share_world, 0)
    df_vcr_annual['VCRS'] = (df_vcr_annual['VCR'] - 1) / (df_vcr_annual['VCR'] + 1)
    
    df_vcr_annual = df_vcr_annual.replace([np.inf, -np.inf, np.nan], 0)
    return df_vcr_annual[['VCR', 'VCRS']]

def calculate_annual_growth_rate(df_data, value_col='valor', period_col='periodo'):
    """Calcula a Taxa de Crescimento Anual (AGR - Year-over-Year) para cada país e ano."""
    if df_data.empty:
        return pd.Series(dtype=float, name='AGR')

    df_agg = df_data.groupby(['nome_pais', period_col])[value_col].sum().reset_index()
    df_agg[period_col] = pd.to_numeric(df_agg[period_col], errors='coerce')
    df_agg = df_agg.sort_values(by=['nome_pais', period_col])
    
    # Aplica a variação percentual (YoY) dentro de cada grupo (País)
    df_agg['AGR'] = df_agg.groupby('nome_pais', group_keys=False)[value_col].pct_change()
    
    # Ajuste: Retorna o DataFrame com as colunas de agrupamento antes de setar o índice
    # df_agr agora contém 'nome_pais', 'periodo', 'valor', 'AGR'
    return df_agg[['nome_pais', period_col, 'AGR']].set_index(['nome_pais', period_col])['AGR'].fillna(0)


def calculate_nei(df_exp, df_imp, value_col='valor'):
    exp_agg = df_exp.groupby(['nome_pais', 'periodo'])[value_col].sum().rename('Export')
    imp_agg = df_imp.groupby(['nome_pais', 'periodo'])[value_col].sum().rename('Import')
    df_merged = pd.merge(exp_agg, imp_agg, on=['nome_pais', 'periodo'], how='outer').fillna(0)
    df_merged['Total'] = df_merged['Export'] + df_merged['Import']
    df_merged['NEI'] = np.where(df_merged['Total'] != 0, 
                                (df_merged['Export'] - df_merged['Import']) / df_merged['Total'], 
                                0)
    return df_merged['NEI'].rename('NEI')

def calculate_ihh(df_data, value_col='valor', period_col='periodo'):
    if df_data.empty: return pd.Series(dtype=float, name='IHH')
    df_merged = df_data.groupby(period_col).apply(
        lambda x: (x[value_col] / x[value_col].sum()) ** 2
    ).reset_index(level=0, name='share_sq')
    ihh_per_period = df_merged.groupby(period_col)['share_sq'].sum() * 10000
    return ihh_per_period.rename('IHH')

# ----------------------------------------------------------------------
# 3. APLICAÇÃO DOS CÁLCULOS
# ----------------------------------------------------------------------

# A. VCR e VCRS (Por País e Ano)
df_vcr_annual = calculate_vcr_annual(
    df_paises_exported_cacau, df_paises__exported_commodites,
    df_world_exported_cacau, df_world_exported_total
)

# B. AGR (Taxa de Crescimento Anual - Por País e Ano)
# O erro foi corrigido nesta linha, garantindo que a coluna 'periodo' seja preservada.
agr_cacau_exp = calculate_annual_growth_rate(df_paises_exported_cacau).to_frame('AGR_Cacau_Exp')
agr_total_exp = calculate_annual_growth_rate(df_paises__exported_commodites).to_frame('AGR_Total_Exp')
df_agr_paises = pd.concat([agr_cacau_exp, agr_total_exp], axis=1).sort_index()


# C. NEI (Net Export Index - Por País e Ano)
df_nei_cacau = calculate_nei(df_paises_exported_cacau, df_paises_imported_cacau).to_frame('NEI_Cacau')
df_nei_total = calculate_nei(df_paises__exported_commodites, df_paises__imported_commodites).to_frame('NEI_Total')
df_nei_paises = pd.concat([df_nei_cacau, df_nei_total], axis=1).sort_index()


# D. IHH (Concentração de Mercado - Por Ano)
ihh_exported_cacau = calculate_ihh(df_paises_exported_cacau)
ihh_imported_cacau = calculate_ihh(df_paises_imported_cacau)


# ----------------------------------------------------------------------
# 4. EXIBIÇÃO DOS RESULTADOS FINAIS
# ----------------------------------------------------------------------

def display_indicator(df, title, pivot=False):
    print(f"\n{title}")
    print("-" * len(title))
    if df.empty:
        print("  [DataFrame Vazio - Verifique a API ou os filtros]")
        return
        
    df_display = df.copy()
    
    if pivot:
        df_display = df_display.unstack(level='periodo')
        if isinstance(df_display.columns, pd.MultiIndex):
            df_display.columns = [f"{col[0]}_{col[1]}" for col in df_display.columns]

    print(df_display.round(4).to_string())

print("\n=========================================================")
print("RESULTADOS DOS INDICADORES POR PAÍS E PERÍODO")
print("=========================================================")

# 1. VCR e VCRS (País e Ano)
display_indicator(df_vcr_annual, "1. VCR e VCRS (Vantagem Comparativa - POR PAÍS E ANO)", pivot=True)

# 2. AGR (Taxa de Crescimento Anual - País e Ano)
display_indicator(df_agr_paises, "2. AGR (Taxa de Crescimento Anual YoY - POR PAÍS E ANO)", pivot=True)
print("\n*AGR (Annual Growth Rate) = Variação Percentual Ano contra Ano. O primeiro ano tem AGR = 0.")

# 3. NEI (Net Export Index - País e Ano)
display_indicator(df_nei_paises, "3. NEI (Índice de Comércio Líquido - POR PAÍS E ANO)", pivot=True)


# 4. IHH (Concentração de Mercado - Por Ano)
print("\n4. IHH (Índice Herfindahl-Hirschman)")
print("-----------------------------------")
print("*O IHH mede concentração de mercado e SÓ PODE ser calculado por ANO (para o grupo de países).")
print("\nIHH - Exportação de Cacau (Concentração da amostra de países):")
print(ihh_exported_cacau.round(2).to_string())

print("\nIHH - Importação de Cacau (Concentração da amostra de países):")
print(ihh_imported_cacau.round(2).to_string())