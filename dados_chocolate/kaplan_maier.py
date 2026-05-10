import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test
import matplotlib.pyplot as plt

# Nome do arquivo carregado
# file_name = "C:\\pedro_ibmec\\quarto_semestre\\inferencia_estatistica\\ap2\\programacao\\api_itc\\analise.xlsx"

# 1. Importar Dados (Equivalente ao 'import excel')
# O arquivo principal é lido diretamente
df = pd.read_excel("C:\\pedro_ibmec\\quarto_semestre\\inferencia_estatistica\\ap2\\programacao\\api_itc\\analise.xlsx", sheet_name='Kaplan Maier')

# 2. Manipulação de Variáveis (Equivalente a 'gen' e 'replace')
# --- STATA: 3. gen codigo = 1
df['codigo'] = 1
# --- STATA: 10. replace codigo = 0 if RSCA > 0
# Define o indicador de evento/falha: 1=Falha, 0=Censura
df.loc[df['RSCA'] > 0, 'codigo'] = 0
# --- STATA: 11. gen ano1 = anos - 14
df['ano1'] = df['anos'] - 14

# 3. Definir Variáveis T e E (Equivalente ao 'stset ano1, failure(codigo)')
T = df['ano1']   # Tempo de Sobrevivência
E = df['codigo'] # Status do Evento (1=Falha, 0=Censura)



kmf_geral = KaplanMeierFitter()
kmf_geral.fit(T, event_observed=E, label='Kaplan-Meier Geral')


# Tabela (sts list)
print("## 📊 Resultados Kaplan-Meier (Geral) - STS LIST (Primeiros 10 Períodos)")
tabela = kmf_geral.event_table
print(tabela.head())

#SOBREVIVENCIA GERAL
# Gráfico (sts graph)
plt.figure(figsize=(8, 6))
kmf_geral.plot_survival_function(ci_show=True)
plt.title('Curva de Sobrevivência de Kaplan-Meier (Geral)')
plt.xlabel('Tempo de Análise (ano1)')
plt.ylabel('Probabilidade de Sobrevivência')
plt.grid(True, linestyle='--', alpha=0.6)
#plt.savefig('kaplan_meier_geral.png') # Salva o gráfico geral
plt.show()

# ----------------------------------------------------------------------
# --- 3. ANÁLISE KAPLAN-MEIER POR PAÍS (GRÁFICOS SEPARADOS) ---

kmf_pais = KaplanMeierFitter()
paises = df['Pais'].unique()

print("\n## 🗺️ Curvas Kaplan-Meier por País (Gráficos Separados)")

# ESTA É A MUDANÇA: Novo gráfico dentro do loop para cada país
for pais in paises:
    mask = df['Pais'] == pais
    
    # Novo objeto Figure e Axes para cada país
    plt.figure(figsize=(8, 6))
    
    # Ajusta e plota o modelo para o grupo atual
    kmf_pais.fit(T[mask], E[mask], label=pais)
    kmf_pais.plot_survival_function(ci_show=True)
    
    plt.title(f'Curva de Sobrevivência - {pais}')
    plt.xlabel('Tempo de Análise (ano1)')
    plt.ylabel('Probabilidade de Sobrevivência')
    plt.grid(True, linestyle='--', alpha=0.6)
    
    # Salva o gráfico com um nome único
    safe_pais_name = pais.replace(' ', '_').replace('.', '')
    #plt.savefig(f'kaplan_meier_{safe_pais_name}.png') 
    plt.show()
    
# ----------------------------------------------------------------------
# --- 4. ANÁLISE POR GRUPO E TESTES DE HIPÓTESE ---

# Separar dados pelos grupos A e B
T_A = df.loc[df['Grupo'] == 'A', 'ano1']
E_A = df.loc[df['Grupo'] == 'A', 'codigo']
T_B = df.loc[df['Grupo'] == 'B', 'ano1']
E_B = df.loc[df['Grupo'] == 'B', 'codigo']

# Gráfico KM por Grupo (sts graph, by(Grupo) separate)
kmf_grupo = KaplanMeierFitter()
plt.figure(figsize=(8, 6))
kmf_grupo.fit(T_A, E_A, label='Grupo A').plot_survival_function(ci_show=True)
kmf_grupo.fit(T_B, E_B, label='Grupo B').plot_survival_function(ci_show=True)
plt.title('Curvas de Sobrevivência de Kaplan-Meier por Grupo')
plt.xlabel('Tempo de Análise (ano1)')
plt.ylabel('Probabilidade de Sobrevivência')
plt.grid(True, linestyle='--', alpha=0.6)
plt.legend()
#plt.savefig('kaplan_meier_grupo.png')
plt.show()

# ----------------------------------------------------------------------
# --- 5. TESTES DE HIPÓTESE (LOG-RANK E WILCOXON) ---

# --- STATA: sts test Grupo, logrank
results_logrank = logrank_test(T_A, T_B, E_A, E_B)

print("\n## 🧪 Teste Log-Rank (Grupo A vs. Grupo B)")
print(f"Estatística Qui-quadrado: {results_logrank.test_statistic:.2f} (STATA: 10.05)")
print(f"Valor p (P>chi2): {results_logrank.p_value:.4f} (STATA: 0.0015)")


# --- STATA: sts test Grupo, wilcoxon
# SOLUÇÃO PARA O SEU ERRO DE IMPORTAÇÃO: 
# Usamos logrank_test com ponderação 'wilcoxon' (Wilcoxon-Breslow-Gehan).
results_wilcoxon = logrank_test(T_A, T_B, E_A, E_B, weightings='wilcoxon')

print("\n## 🧪 Teste Wilcoxon-Breslow-Gehan (Grupo A vs. Grupo B)")
print(f"Estatística Qui-quadrado: {results_wilcoxon.test_statistic:.2f} (STATA: 7.84)")
print(f"Valor p (P>chi2): {results_wilcoxon.p_value:.4f} (STATA: 0.0051)") 