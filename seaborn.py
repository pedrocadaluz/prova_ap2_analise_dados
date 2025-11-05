import seaborn as sns
import matplotlib as plt
import pandas as pd

df = sns.load_dataset("tips")

sns.histplot(data = df, x = "total_bill")

sns.boxplot(data=df, x="total_bill")

df_numerico = df.select_dtypes(include = "number")
df_corr = df_numerico.corr()
sns.heatmap(df_corr, annot=True)

sns.lineplot(data=df, x="day", y="tip")

sns.barplot(data = df, x = "day", y = "tip")
sns.countplot(data = df, x = "day", hue = "sex")        