# import sys; sys.path.append('/home/ec2-user/.local/lib/python3.6/site-packages/')
# The first line of import is to make it run on AWS ec2. 
from stravalib import Client
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

"""
This file updates activities of a Strava user with his/her access token. When the code runs, it 
first extracts the latest active date of the user from the database, and then tries to find all 
the activities after that date. If an zero-length list is returned, then there is no need to update 
the activities; if a non-empty list is returned, then the activity attributes will be framed into a 
pandas DataFrame, and appended to the database. 

This file uses the Python library stravalib to extract user data.
"""

engine = create_engine('sql')
# This is to connect to the PostgreSQL database. I will omit the database address here.

def activity_append(data, x, athlete):
    """
    :param data: a list of activity details
    :param x: an stravalib activity object, whose attributes will be extracted into a pd dataframe
    :param athlete: an stravalib athlete object
    :return: there is no return value for this function
    """
    x_distance = x.distance
    x_teg = x.total_elevation_gain
    x_mt = x.moving_time
    x_et = x.elapsed_time
    x_as = x.average_speed
    x_ms = x.max_speed
    # dates
    x_sd = x.start_date
    x_sdl = x.start_date_local
    # latitude and longitude
    x_sll = x.start_latlng
    x_ell = x.end_latlng
    if x_sll is None:
        x_sll_lat = None
        x_sll_lon = None
    else:
        x_sll_lat = x_sll.lat
        x_sll_lon = x_sll.lon

    if x_ell is None:
        x_ell_lat = None
        x_ell_lon = None
    else:
        x_ell_lat = x_ell.lat
        x_ell_lon = x_ell.lon
    # map
    x_map = x.map
    athlete_id = athlete.id
    # append information of one activity to data
    data.append((athlete_id, x.id, x.resource_state, x.external_id, x.upload_id, x.name,
                 x.description, float(x_distance), x_mt.seconds, x_et.seconds,
                 float(x_teg),
                 x.elev_high, x.elev_low, x.type, x_sd.isoformat(),
                 x_sdl.isoformat(),
                 str(x.timezone), x_sll_lat, x_sll_lon, x_ell_lat, x_ell_lon,
                 x.location_city,
                 x.location_state, x.location_country, x.achievement_count, x.pr_count,
                 x.kudos_count, x.comment_count, x.athlete_count, x.photo_count, x.total_photo_count,
                 x.photos, x_map.id, x_map.resource_state, x.trainer, x.commute,
                 x.manual, x.private, x.device_name, x.embed_token,
                 x.flagged,
                 x.workout_type, x.gear_id, x.gear, float(x_as), float(x_ms),
                 x.average_cadence,
                 x.average_temp, x.average_watts, x.max_watts, x.weighted_average_watts,
                 x.kilojoules,
                 x.device_watts, x.has_heartrate, x.average_heartrate, x.max_heartrate,
                 x.calories,
                 x.suffer_score, x.has_kudoed, x.segment_efforts, x.splits_metric,
                 x.splits_standard, x.laps,
                 x.best_efforts))

# data_header is all the attributes of atrava activity
data_header =['athlete_id', 'activity_id', 'resource_state', 'external_id', 'upload_id','activity_name',
             'description', 'distance', 'moving_time', 'elapsed_time', 'total_elevation_gain',
             'elev_high', 'elev_low', 'type', 'start_date', 'start_date_local',
             'timezone', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude',
             'location_city', 'location_state', 'location_country', 'achievement_count', 'pr_count',
             'kudos_count', 'comment_count', 'athlete_count', 'photo_count', 'total_photo_count',
             'photos', 'map_id', 'map_resource_state', 'trainer', 'commute',
             'manual', 'private', 'device_name', 'embed_token', 'flagged',
             'workout_type', 'gear_id', 'gear', 'average_speed', 'max_speed',
             'average_cadence', 'average_temp', 'average_watts', 'max_watts', 'weighted_average_watts',
             'kilojoules', 'device_watts', 'has_heartrate', 'average_heartrate', 'max_heartrate',
             'calories', 'suffer_score', 'has_kudoed', 'segment_efforts', 'splits_metric',
             'splits_standard', 'laps', 'best_efforts']

number_columns = len(data_header)
# print(number_columns)

def athlete_info_add(mydata, token):
    # mydata is a list of data that will be transformed into a pandas DataFrame later.
    # Token is the access token of an athlete
    # First, get the client and athlete info with the token. Then get the latest date from
    # the database. 
    
    client1 = Client(access_token=token) # call the API with access token
    athlete1 = client1.get_athlete()
    athlete1_id = athlete1.id
    
    # read the date of the latest activity into Python using sql query
    a=pd.read_sql_query('SELECT max(start_date) latest_date FROM athlete_activity WHERE athlete_id=%s' % (athlete1_id,) ,con=engine)
    most_recent = a['latest_date'].tolist() # from a dataframe to a single-element list
    # get activities after the latest date
    if len(most_recent):
        ac_after = client1.get_activities(after=most_recent[0])
    else:
        ac_after = client1.get_activities(after='1990-12-13T04:56:10') # an early enough date to get all activity in an old-to-new order
    
    if ac_after: # if there are activities after the latest date in the database
        for x in ac_after:
            activity_append(mydata, x, athlete1)


token_list = pd.read_sql_query('SELECT token FROM athlete_token', engine)
token_list = token_list['token'].tolist() # a list of user access tokens from the database's token table 



athlete_list_update = [] # an empty list to store updated activity information

for tk in token_list:
    athlete_info_add(athlete_list_update, tk)

print(len(athlete_list_update))

if len(athlete_list_update) == 0:
    print('No new rows added.')
else:
    athlete_update = pd.DataFrame(athlete_list_update)
    athlete_update.columns = data_header
    num_new_rows = athlete_update.shape[0]
    athlete_update.to_sql('athlete_activity', engine, index=False, if_exists='append')
    print('%d new rows added.' % (num_new_rows))
