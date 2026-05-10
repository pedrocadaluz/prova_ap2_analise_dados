import pandas as pd
import statsmodels.formula.api as smf
import numpy as np

# --- 1. Carregar e Verificar Dados ---
# Este script assume que o arquivo 'transformed_data_wide_format.csv' 
# (criado na etapa de transformação dos dados) está disponível.

try:
    df_wide = pd.read_excel("C:\\pedro_ibmec\\quarto_semestre\\inferencia_estatistica\\ap2\\programacao\\api_itc\\analise.xlsx", sheet_name='base_regressao')
except FileNotFoundError:
    print("ERRO: O arquivo 'transformed_data_wide_format.csv' não foi encontrado.")
    print("Por favor, execute o código de transformação de dados primeiro.")
    exit()

# Definir variáveis
dependent_variable = 'ano2024'
regressors = [
    'ano2023', 'ano2022', 'ano2021', 'ano2020', 
    'ano2019', 'ano2018', 'ano2017', 'ano2016', 'ano2015'
]

final_table_data = []

# Loop para executar a Regressão OLS e extrair as estatísticas
for regressor in regressors:
    formula = f'{dependent_variable} ~ {regressor}'
    
    model = smf.ols(formula, data=df_wide)
    results = model.fit()
    
    # Extrair estatísticas do modelo
    r_squared = results.rsquared
    f_stat = results.fvalue
    prob_f = results.f_pvalue
    n_obs = results.nobs
    
    # Extrair estatísticas do Regressor (X)
    coef_beta1 = results.params[regressor]
    std_err_beta1 = results.bse[regressor]
    
    # Extrair estatísticas do Intercepto (_cons)
    coef_cons = results.params['Intercept']
    std_err_cons = results.bse['Intercept']
    
    # Adicionar dados para a estrutura da tabela final
    final_table_data.append({
        'Regressão': regressor,
        'R-quadrado': r_squared,
        'F(1, 13)': f_stat,
        'Prob > F': prob_f,
        'N. Obs': n_obs,
        'Coeficiente (ano...)': coef_beta1,
        'Erro Padrão (ano...)': std_err_beta1,
        'Coeficiente (_cons)': coef_cons,
        'Erro Padrão (_cons)': std_err_cons
    })

# Converter a lista de resultados em um DataFrame
df_summary_final = pd.DataFrame(final_table_data)

# Renomear colunas para o formato final
df_summary_final.columns = [
    'Regressão', 'R-quadrado', 'F(1, 13)', 'Prob > F', 'N. Obs',
    'Coeficiente (ano...)', 'Erro Padrão (ano...)', 
    'Coeficiente (_cons)', 'Erro Padrão (_cons)'
]

# Definir o formato desejado (4 casas decimais para R2 e P-valor; 6 casas para coefs/erros)
formatting_dict = {
    'R-quadrado': '{:.4f}',
    'F(1, 13)': '{:.2f}',
    'Prob > F': '{:.4f}',
    'N. Obs': '{:.0f}',
    'Coeficiente (ano...)': '{:.6f}',
    'Erro Padrão (ano...)': '{:.6f}',
    'Coeficiente (_cons)': '{:.6f}',
    'Erro Padrão (_cons)': '{:.6f}'
}

# Aplicar o formato
for col, fmt in formatting_dict.items():
    if col in df_summary_final.columns:
        df_summary_final[col] = df_summary_final[col].apply(lambda x: fmt.format(x) if pd.notna(x) else '')

# Imprimir a tabela no console
print("\n--- Tabela Resumo das Regressões OLS (Formato de Matriz) ---")
print("Variável Dependente: ano2024 (em todas as regressões)")
print(df_summary_final.to_markdown(index=False))

# Salvar a tabela final em um arquivo CSV
df_summary_final.to_excel("regressao_chocolate.xlsx", index=False)