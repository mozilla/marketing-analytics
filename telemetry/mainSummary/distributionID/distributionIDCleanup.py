import pandas as pd

# Open Files and store in dataframes
file = '/Users/gkaberere/spark-warehouse/adHoc/adHoc/distributionIDPerf20180930-20181122.csv/distributionIDPerf20180930-20181122.csv'

df = pd.read_csv(file, sep=',', header=0, index_col=None, usecols=None, skiprows=None, encoding='utf-8')

dfSoftonic = df.loc[df['distribution_id'].isin(['softonic-002', 'softonic-003'])]

softonicFile = '/Users/gkaberere/spark-warehouse/adHoc/adHoc/softonic.csv'
with open(softonicFile, 'w') as file:
    columnNames = ('submission_date_s3', 'distribution_id', 'country', 'DAU', 'activeDAU', 'totalURI', 'searches','installs')
    dfSoftonic.to_csv(file, index=False, header=True, encoding='utf-8')

