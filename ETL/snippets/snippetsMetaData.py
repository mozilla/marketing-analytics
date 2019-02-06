import pandas as pd

file = '/Users/gkaberere/spark-warehouse/testSnippet/snippetsData/asrsnippets Jan 18th.csv'


df1 = pd.read_csv(file, sep='|', header=0, index_col=None, usecols=None, skiprows=None, encoding='utf-8')
print(df1.head(20))