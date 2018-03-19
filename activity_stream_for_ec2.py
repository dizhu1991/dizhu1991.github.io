# import sys; sys.path.append('/home/ec2-user/.local/lib/python3.6/site-packages/')
# The first line of import is to make it run on AWS ec2

import pandas as pd
from stravalib import Client
import numpy as np
import schedule
import time
import csv
import boto3
import os
from requests.exceptions import HTTPError
from sqlalchemy import create_engine
import psycopg2
from ratelimit import rate_limited # complying with rate limiting requirements

"""
This file uploads .csv files that track detailed streams of an activity. 
It is scheduled to run on ec2 periodically. The rate limiting problem has not been fully solved;
although I use the Python library ratelimit to comply my code with rate limiting requirements, 
sometimes it doesn't work and an error occurred.
"""

# 11 types of activity streams
activity_stream_type = ["time", "latlng", "distance", "altitude", "velocity_smooth",
                        "heartrate", "cadence", "watts", "temp", "moving", "grade_smooth"]

# access the AWS S3
ac_key = 'ackey'
secret_ac_key = 'secret_ac_key'
# hard-coded keys, which will be omitted here.

session_me = boto3.Session(
    aws_access_key_id=ac_key,
    aws_secret_access_key=secret_ac_key,
    # aws_session_token=SESSION_TOKEN,
)

s3 = session_me.client('s3')
s3s = session_me.resource('s3')


# connect to the PostgreSQL server, which will also be omitted
engine = create_engine('postgresql://username:passpord@port/database')

# pipeline: connect the S3 and postegresql server - get the token list - update the 
# activities to the database - for every activity, update its stream to S3 buckets


# psycopg2 connection. I guess it's better to harmonise it with sqlalchemy engine, but for now it's fine.

conn = psycopg2.connect(host="host", database="database", user="user", password="password")
cur = conn.cursor()


token_list = pd.read_sql_query("SELECT token FROM athlete_token", engine)
token_list = token_list['token'].tolist() # a list of tokens from the database 


retrieve_header = ['athlete_id', 'activity_id', 'stream_type', 'stream_retrieved']



RetrieveSituation = [] # empty list to store (possible) updated retrieved-or-not



def InitialiseRetrieve(mydata, activity_id, athlete):
    # client1 = Client(access_token = token)
    # athlete1 = client1.get_athlete()
    # athlete1_id = athlete1.id
    mydata.append((athlete.id, activity_id, 'activity', False))
    mydata.append((athlete.id, activity_id, 'effort', False))
    mydata.append((athlete.id, activity_id, 'route', False))
    mydata.append((athlete.id, activity_id, 'segment', False))


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def UpdateTables(token): # update the tables based on access token of an athlete
    
    mydata = [] # empty_list
    client1 = Client(access_token = token)
    athlete1 = client1.get_athlete()
    athlete1_id = athlete1.id
    
    # read the activities from the table 'athlete_activity'
    activity_frame = pd.read_sql_query('SELECT activity_id FROM athlete_activity WHERE athlete_id=%s' % (athlete1_id,), con=engine)
    activity_table_list = activity_frame['activity_id'].tolist()
    
    # read the activities from the table 'stream_retrieved_or_not'
    stream_frame = pd.read_sql_query('SELECT DISTINCT activity_id FROM stream_retrieved_or_not WHERE athlete_id=1429715' , con=engine)
    stream_table_list = activity_frame['activity_id'].tolist()
    
    update_list = list(set(activity_table_list) - set(stream_table_list)) # Difference between the two
    
    if len(update_list): # loop through the difference set of activities if it's not empty
        # if the list is non-empty
        for ac in update_list:
            try: # get streams from an activity of a client
                activity_streams = client1.get_activity_streams(ac, types=activity_stream_type, resolution='medium')
            except HTTPError as err:
                activity_streams = []
            if len(activity_streams): # if there are streams for this activity
                stream_data = pd.DataFrame(columns=activity_stream_type) # []
                for types in activity_stream_type:
                    if types in activity_streams.keys():   # activity_streams.keys():
                        stream_data[types] = activity_streams[types].data
                stream_data.insert(loc=0, column='activity_id', value=ac)
                tmp_filename = 'tmp_stream.csv' # save it
                stream_data.to_csv(tmp_filename, encoding='utf-8', index=False)
                # upload the tmp .csv file to the S3 bucket
                bucket_name = 'dg-strava-streams'
                object_name = 'activity/%d.csv' % (ac)
                try:
                    s3s.Object(bucket_name, object_name).put(Body=open(tmp_filename, 'rb'))
                except Exception as error:
                    print(error)
        


                
                
                #s3.upload_file(tmp_filename, 'dg-strava-streams', 'activity/%d.csv' % (ac))
                # os.remove(tmp_filename) # delete the temp file
                
                # update the stream retrievement table, activity True
                mydata.append((athlete1.id, ac, 'activity', True))
                mydata.append((athlete1.id, ac, 'effort', False))
                mydata.append((athlete1.id, ac, 'route', False))
                mydata.append((athlete1.id, ac, 'segment', False))
            else:
                # update the stream retrievement table, activity False
                mydata.append((athlete1.id, ac, 'activity', False))
                mydata.append((athlete1.id, ac, 'effort', False))
                mydata.append((athlete1.id, ac, 'route', False))
                mydata.append((athlete1.id, ac, 'segment', False))
                
        retrieve_data = pd.DataFrame(mydata, columns=retrieve_header)
        retrieve_data.to_sql('stream_retrieved_or_not', engine, index=False, if_exists='append')
        
    else:
        print("No need to update.")


for tk in token_list:
    UpdateTables(tk)

