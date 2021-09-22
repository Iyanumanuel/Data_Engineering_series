import pandas as pd
import json

url = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"

json_data = pd.read_json(url)
json_data.head(2)