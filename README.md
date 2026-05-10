# Análise econômica do mercado internacional de cacau e chocolate

Artigo de referência: [`cacau_artigo.pdf`](cacau_artigo.pdf) (versão congresso) e [`final_artigo.pdf`](final_artigo.pdf) (versão completa).

---

## O que o projeto faz

Mede a competitividade internacional e a estrutura de mercado do cacau (HS 18) e do chocolate (HS 1806) entre 2015 e 2024, para os 15 maiores exportadores de cacau. Os dados vêm da API do Trade Map via `laboratoriodefinancas.com`.

**Países analisados:** Belgium, Canada, Côte d'Ivoire, Ecuador, France, Germany, Ghana, Indonesia, Italy, Malaysia, Netherlands, Poland, Switzerland, United Kingdom, United States of America.

---

## Estrutura do código e o que cada parte produz

### `dados_cacau/`

| Script | O que calcula | Saída |
|---|---|---|
| `cacau.py` | VCR, VCRS, NEI, CAGR e IHH (exportação e importação) | `analise_indices_cacau.xlsx` |
| `teste_regressao.py` | Regressão OLS da VCRS (ano 2024 ~ anos anteriores) | `regression_summary_matrix_table.xlsx` |
| `kaplan_maier.py` | Curvas de sobrevivência e testes Log-Rank / Wilcoxon | gráficos (usa `regressao_vcrs.xlsx`) |

### `dados_chocolate/`

| Script | O que calcula | Saída |
|---|---|---|
| `chocolate.py` | VCR, VCRS, NEI, CAGR e IHH (exportação e importação) | `analise_indices_chocolate.xlsx` |
| `regressao.py` | Regressão OLS da VCRS (ano 2024 ~ anos anteriores) | `regressao_chocolate.xlsx` |
| `kaplan_maier.py` | Curvas de sobrevivência e testes Log-Rank / Wilcoxon | gráficos (usa `analise.xlsx`) |

---

## Índices calculados

| Índice | O que mede |
|---|---|
| **VCR** | Vantagem Comparativa Revelada (Balassa 1965) |
| **VCRS** | Versão simétrica do VCR, varia entre −1 e 1 |
| **NEI** | Exportação Líquida: +1 = exportador puro, −1 = importador puro |
| **IHH** | Concentração de mercado por ano (contribuição por país + total) |
| **CAGR** | Taxa de crescimento anual composta das exportações (2015–2024) |
| **Regressão OLS** | Estabilidade da VCRS: β > 1 = divergência (especialização crescente) |
| **Kaplan–Meier** | Persistência da vantagem (probabilidade de manter VCRS > 0 ao longo do tempo) |

---

## Resultados principais

**Cacau:** países em desenvolvimento (Costa do Marfim, Gana, Equador) dominam com VCRS acima de 0,8. A sobrevivência da vantagem comparativa é alta e estável (Grupo B: ~0,96 ao final do período). Países desenvolvidos perdem vantagem rapidamente no grão (Grupo A: ~0,45).

**Chocolate:** a competitividade migra para países desenvolvidos — Polônia, Bélgica e Itália lideram com VCRS acima de 0,6. Países em desenvolvimento praticamente desaparecem (VCRS < 0). O padrão de sobrevivência se repete: Grupo B mais estável, Grupo A mais volátil.

> O IHH e o NEI juntos mostram que países desenvolvidos (Alemanha, Holanda, Bélgica) operam como nodos logísticos e industriais, não como produtores agrícolas — confirmando a captura de valor agregado ao longo da cadeia global.

---

## Como rodar

```bash
git clone <repo>
cd api_itc
uv sync
```

Defina o token da API no arquivo `.env`:

```
MEU_TOKEN_API=seu_token_aqui
```

Execute na ordem:

```bash
uv run dados_cacau/cacau.py
uv run dados_cacau/teste_regressao.py
uv run dados_cacau/kaplan_maier.py

uv run dados_chocolate/chocolate.py
uv run dados_chocolate/regressao.py
uv run dados_chocolate/kaplan_maier.py
```
