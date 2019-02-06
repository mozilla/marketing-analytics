import json

#data = {"redash_api": "https://sql.telemetry.mozilla.org/api/queries/60856/results.csv?api_key=gIXykstcSKJR0nMSeMnvG4NBpt91UVkLaZYqqWvL",
        #"gcp_bucket": "gs://snippets-data-transfer/daily-tracking-data/"}

with open('/Users/gkaberere/Google Drive/Github/marketing-analytics/ETL/snippets/snippetsEnvVariables.json', 'r') as jsonFile:
    print(json.load(jsonFile)['redash_api'])
    print(json.load(jsonFile)


