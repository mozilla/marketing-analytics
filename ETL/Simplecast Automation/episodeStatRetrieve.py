import requests
import json
import csv
import datetime

# 1 Define url end point and parameters

urlEndPoint = 'https://api.simplecast.com/v1/podcasts/3077/statistics/episodes.json'
token = 'sc_Zo4GBBRmjLvHjVokrqBtcw'
episode_id = '94167'

joined = urlEndPoint+'?episode_id='+episode_id+', auth=('+token+',"''"'))'
print(joined)

# 2 Read episode ID from the episode list file
# 3 Create API URL link for each episode ID
# 4 Access API and retrieve data for each episode. Append episode ID for each retrieve.
# 5 Combine all files into one csv
# 6 Close
# 7 Load into google bigquery