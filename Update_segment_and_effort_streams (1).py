
# coding: utf-8

# In[2]:


# import sys; sys.path.append('/home/ec2-user/.local/lib/python3.6/site-packages/')
# The first line of import is to make it run on AWS ec2
from stravalib import Client
import pandas as pd
import numpy as np
import time
import csv
import json
import boto3
import os
from requests.exceptions import HTTPError
from sqlalchemy import create_engine
import psycopg2
from ratelimit import rate_limited # complying with rate limiting requirements 


# In[3]:


# 11 types of activity streams
activity_stream_type = ["time", "latlng", "distance", "altitude", "velocity_smooth", "heartrate", "cadence", "watts", "temp", "moving", "grade_smooth"]


# In[4]:


# access the AWS S3
ac_key = 'AKIAJLJ66OZ5EED6N73Q'
secret_ac_key = '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'
# hard-coded keys

session_me = boto3.Session(
    aws_access_key_id=ac_key,
    aws_secret_access_key=secret_ac_key,
    # aws_session_token=SESSION_TOKEN,
)

s3 = session_me.client('s3')
s3s = session_me.resource('s3')


# In[5]:


# connect to the Postgresql server
engine = create_engine('postgresql://dgadmin:d0YUKIujJ1pFKw90eSJ0@172.29.11.105:5432/athletes_home')

# pipeline: connect the S3 and postegresql server - get the token list - update the 
# activities to the database - for every activity, update its stream to S3 buckets


# In[6]:


# psycopg2 connection. I guess it's better to harmonise it with sqlalchemy engine, but for now it's fine.

conn = psycopg2.connect(host="172.29.11.105", database="athletes_home", user="dgadmin", password="d0YUKIujJ1pFKw90eSJ0")
cur = conn.cursor()


# In[7]:


token_list = pd.read_sql_query("SELECT token FROM athlete_token", engine)
token_list = token_list['token'].tolist() # a list of tokens from the database 
token_list = token_list


# In[8]:


token_list 


# In[9]:


retrieve_header = ['athlete_id', 'activity_id', 'stream_type', 'stream_retrieved']


# In[10]:


RetrieveSituation = [] # empty list to store (possible) updated retrieved-or-not


# In[11]:


bucket_name = "dg-strava-streams"


# In[15]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def AcListUpdate(athlete_id): # athlete_id
    total_ac_frame = pd.read_sql_query("SELECT activity_id FROM athlete_activity WHERE athlete_id=%d AND stream_type='effort' AND stream_retrieved=FALSE " %(athlete_id), engine)
    total_ac_list = total_ac_frame['activity_id'].tolist()
    
    current_ac_frame = pd.read_sql_query("SELECT DISTINCT activity_id FROM stream_retrieved_or_not WHERE athlete_id=%d" %(athlete_id), engine)
    current_ac_list = current_ac_frame['activity_id'].tolist()
    
    #ac_list is the list of activities whose streams need updating 
    ac_list = list(set(total_ac_list) - set(current_ac_list)) 
    return ac_list


# In[13]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def AcListForSg(athlete_id): # athlete_id
    activity_frame = pd.read_sql_query("SELECT activity_id FROM stream_retrieved_or_not WHERE athlete_id=%d AND stream_type='segment' AND stream_retrieved=FALSE " %(athlete_id), engine)
    ac_list = activity_frame['activity_id'].tolist()
    return ac_list


# In[14]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def UploadAc(client1, ac):
    try:
        ac_streams = client1.get_activity_streams(ac, types=activity_stream_type, resolution='medium')
    except HTTPError as e:
        ac_streams = []
                    
    if len(ac_streams):
        ac_data = pd.DataFrame(columns=activity_stream_type)
        for types in activity_stream_type:
            if types in ac_streams.keys():
                ac_data[types] = ac_streams[types].data
        ac_data.insert(loc=0, column='activity_id', value=ac)
                        
        tmp_file = "tmp2.json"
        json_str = ac_data.to_json(orient="index") # to json string
        with open(tmp_file, 'w') as f:
            json.dump(json_str, f)
        object_ac = 'activity/%d.json' % (ac)
        try:
                            
        except Exception as error:
            print(error)
        print("Streams of activity %d uploaded." % (ac))
        
        os.remove(tmp_file)
        return 1
    else:
        print("There is no stream for activity %d." %(ac))
        return 0   


# In[160]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def UploadSe(client1, se):
    try:
        se_streams = client1.get_effort_streams(se, types=activity_stream_type, resolution='medium')
    except HTTPError as e:
        se_streams = []
                    
    if len(se_streams):
        se_data = pd.DataFrame(columns=activity_stream_type)
        for types in activity_stream_type:
            if types in se_streams.keys():
                se_data[types] = se_streams[types].data
        se_data.insert(loc=0, column='segment_effort_id', value=se)
                        
        tmp_file = "tmp.json"
        json_str = se_data.to_json(orient="index") # to json string
        with open(tmp_file, 'w') as f:
            json.dump(json_str, f)
        object_se = 'effort/%d.json' % (se)
        try:
            s3s.Object(bucket_name, object_se).put(Body=open(tmp_file, 'rb'))
                            
        except Exception as error:
            print(error)
        print("Streams of segment effort %d uploaded." % (se))
        
        os.remove(tmp_file)
        return 1
    else:
        print("There is no stream for segment effort %d." %(se))
        return 0
           


# In[161]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def UploadSg(client1, se):
    try:
        se_streams = client1.get_segment_streams(se, types=activity_stream_type, resolution='medium')
    except HTTPError as e:
        se_streams = []
                    
    if len(se_streams):
        se_data = pd.DataFrame(columns=activity_stream_type)
        for types in activity_stream_type:
            if types in se_streams.keys():
                se_data[types] = se_streams[types].data
        se_data.insert(loc=0, column='segment_id', value=se)
                        
        tmp_file = "tmp1.json"
        json_str = se_data.to_json(orient="index") # to json string
        with open(tmp_file, 'w') as f:
            json.dump(json_str, f)
        object_se = 'segment/%d.json' % (se)
        try:
            s3s.Object(bucket_name, object_se).put(Body=open(tmp_file, 'rb'))
                            
        except Exception as error:
            print(error)
        print("Streams of segment %d uploaded." % (se))
        
        os.remove(tmp_file)
        return 1
    else:
        print("There is no stream for segment %d." %(se))
        return 0
           


# In[162]:


a_quarter = 900 # 15 minutes

@rate_limited(600, a_quarter) # limiting the rate of API calling to 600 per 15 minutes
def UpdateTables(token): # update the tables based on access token of an athlete
    client1 = Client(access_token = token)
    ttk = ("\'" + token + "\'")
    # athlete1 = client1.get_athlete()
    athlete_frame = pd.read_sql_query("SELECT athlete_id FROM athlete_token WHERE token=%s" % (ttk), con=engine)
    athlete1_id = athlete_frame['athlete_id'].tolist()
    
    ac_list = AcListUpdate(athlete1_id[0])   
    # get the different sets so that we can loop through them and get the streams
    
    if len(ac_list): 
        # if the list is non-empty
        for ac in ac_list:
            mydata = []
            print("Updating the activity streams of activity %d:" % (ac))
            ac_status = UploadAc(client1, ac)
            if ac_status:
                mydata.append((athlete1_id[0], ac, 'activity', True))
                print("Activity streams of %d updated." % (ac))
            else:
                mydata.append((athlete1.id, ac, 'activity', False))
                print("There is no activity streams in %d." % (ac))
            
            print("Updating the effort streams of activity %d:" % (ac))
            se_frame = pd.read_sql_query("SELECT segment_effort_id se FROM segment_efforts where activity_id=%d" % (ac), con=engine)
            se_list = se_frame['se'].tolist() # segment effort id's of this activity
            
            sg_frame = pd.read_sql_query("SELECT segment_id FROM segment_efforts where activity_id=%d" % (ac), con=engine)
            sg_list = sg_frame['segment_id'].tolist() # segment id's of this activity
            
            if len(se_list):
                tmp = 0
                for se in se_list:
                    se_tmp = UploadSe(client1, se)
                    tmp = tmp + se_tmp
                if tmp: # at least one effort in an activity has streams and is updated
                    mydata.append((athlete1_id[0], ac, 'effort', True))
                else:
                    mydata.append((athlete1_id[0], ac, 'effort', False))
                    # cur.execute("UPDATE stream_retrieved_or_not SET stream_retrieved=TRUE WHERE stream_type='effort' AND activity_id=%d;COMMIT;" % (ac))
            else:
                print("Activity %d has no segment efforts." % (ac))
                mydata.append((athlete1_id[0], ac, 'effort', False))
                
            if len(sg_list): # if there are segments in this activity
                tmp1 = 0
                for sg in sg_list:
                    sg_tmp = UploadSg(client1, sg)
                    tmp1 = tmp1 + sg_tmp
                if tmp1: # at least one segment in an activity has streams and is updated
                    mydata.append((athlete1_id[0], ac, 'segment', True))
                else:
                    mydata.append((athlete1_id[0], ac, 'segment', False))
                    # cur.execute("UPDATE stream_retrieved_or_not SET stream_retrieved=TRUE WHERE stream_type='segment' AND activity_id=%d;COMMIT;" % (ac))
            else:
                print("Activity %d has no segments." % (ac))
                mydata.append((athlete1_id[0], ac, 'segment', False))
        
            mydata.append((athlete1_id[0], ac, 'route', False))   
            
            myframe = pd.DataFrame(mydata, columns=retrieve_header)
            myframe.to_sql('stream_retrieved_or_not', engine, index=False, if_exists='append')


# In[163]:


for tk in token_list:
    UpdateTables(tk)


# In[1]:


list_test = [1,2,3,4,5,6]

def real_test(a):
    if a in list_test:
        print("%d is in the list." %(a))
    else: 
        print("%d is not in the list." % (a))


# In[2]:


real_test(4)


# In[3]:


real_test(9)

