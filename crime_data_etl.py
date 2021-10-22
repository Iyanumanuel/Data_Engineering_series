#import necessary libraries
import pandas as pd
import sqlalchemy
import json
from datetime import datetime
from sqlalchemy import create_engine
from decouple import config

#database configuration
db_conn = config('conn', default= '')
data_url = config('data_url', default= '')
engine = create_engine(db_conn)

#data source
url = data_url
json_data = pd.read_json(url)

#dropping unused columns
json_data = json_data[json_data.columns.drop(list(json_data.filter(regex='@computed')))].drop(columns= 'location')
       
#adding a load_time field to track changes
json_data['load_time'] = datetime.now()

#loading to sql database
json_data.to_sql('crime_data', engine,  if_exists= 'replace')
print('Successfully sent to sql db at', datetime.now())