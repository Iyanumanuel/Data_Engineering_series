#import necessary libraries
import pandas as pd
import sqlalchemy
import json
from sqlalchemy import create_engine
from decouple import config

db_conn = config('conn', default= '')
data_url = config('data_url', default= '')

engine = create_engine(db_conn)

url = data_url

json_data = pd.read_json(url)

column_list = ['id', 'case_number', 'date', 'block', 'iucr', 'primary_type',
       'description', 'location_description', 'arrest', 'domestic', 'beat',
       'district', 'ward', '1munity_area', 'fbi_code', 'x_coordinate',
       'y_coordinate', 'year', 'updated_on', 'latitude', 'longitude',
       'location', 'computed_region_awaf_s7ux', 'computed_region_6mkv_f3dw',
       'computed_region_vrxf_vc4k', 'computed_region_bdys_3d7i',
       'computed_region_43wa_7qmu', 'computed_region_rpca_8um6',
       'computed_region_d9mm_jgwp', 'computed_region_d3ds_rm58']
       
json_data.columns = column_list

json_data = json_data.astype(str)

json_data.to_sql('crime_data', engine,  if_exists= 'replace')