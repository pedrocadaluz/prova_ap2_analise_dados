import pandas as pd

#filtro para os países
df_Germany = df[df["nome_pais"].isin(["Germany"])]
df_Germany_exported = df_Germany[df_Germany["tipo_operacao"]=="Exported"]

# 1. Definir as listas de filtros
countries = [
    "World", "Germany", "Netherlands", "Côte d'Ivoire", "Belgium", "Italy",
    "Poland", "France", "United States of America", "Canada", "Malaysia",
    "Ghana", "Ecuador", "Indonesia", "United Kingdom", "Switzerland"
]

operations = ["Imported", "Exported"]

descriptions = ["Cocoa and cocoa preparations", "TOTAL - All products"]

# Anos como string, para garantir compatibilidade com a coluna 'periodo'
periods = ['2020', '2021', '2022', '2023', '2024']

# 2. Aplicar o filtro consolidado usando .isin() e o operador lógico & (AND)
df_final_consolidado = df[
    (df["nome_pais"].isin(countries)) &                  # Filtro de Países
    (df["tipo_operacao"].isin(operations)) &             # Filtro de Operações (Imported/Exported)
    (df["descricao"].isin(descriptions)) &               # Filtro de Descrição
    (df["periodo"].astype(str).isin(periods))            # Filtro de Período (com conversão para string para segurança)
]