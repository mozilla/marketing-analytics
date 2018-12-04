import pandas as pd

file1 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov19.csv"
file2 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov20.csv"
file3 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov21.csv"
file4 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov22.csv"
file5 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov23.csv"
file6 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov24.csv"
file7 = "/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov25.csv"

df1 = pd.read_csv(file1, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df2 = pd.read_csv(file2, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df3 = pd.read_csv(file3, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df4 = pd.read_csv(file4, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df5 = pd.read_csv(file5, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df6 = pd.read_csv(file6, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')
df7 = pd.read_csv(file7, sep=',', header=0, skiprows=None, usecols=None, encoding='utf-8')


combinedDF = df1.append([df2, df3, df4, df5, df6, df7], ignore_index=True)

combinedDF.to_csv('/users/gkaberere/spark-warehouse/testSnippet/testSnippet/snippetsNightlyTestDataNov19-25.csv', encoding='utf-8', index=False, header=True)

combinedDF