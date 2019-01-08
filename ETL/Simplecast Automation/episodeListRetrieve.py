import requests
import json
import csv
import datetime

# 1 Get list of episodes
# 1a Connect to simplecast API

episodes = requests.get('https://api.simplecast.com/v1/podcasts/3077/episodes.json',auth=('sc_Zo4GBBRmjLvHjVokrqBtcw',''))
print(episodes.text)

print('File retrieved\n')

# 1b Read episodes data and store in a variable txtData
txtData = episodes.text

# 1c Load Json data using Json load and create variable to store keys
data = json.loads(txtData)
keys = data[0].keys()

# 2 Save file in csv format
# 2a Set output filename using run date as version

currentDate = datetime.datetime.now()
outputFileName = 'episodeList'+currentDate.strftime(" %Y-%m-%d %H-%M")+'.csv'
print(outputFileName)

with open(outputFileName, 'w', newline='') as outputFile:
    dict_writer = csv.DictWriter(outputFile, keys)
    dict_writer.writeheader()
    dict_writer.writerows(data)
print('List of episodes successfully written\n')