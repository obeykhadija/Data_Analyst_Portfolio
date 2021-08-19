#IMPORTS
import datetime
import json
import sqlite3

import pandas as pd
import requests
import sqlalchemy
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql.ddl import CreateTable

DATABASE_LOCATION = 'sqlite:////Users\obeyk\Desktop\Spotify ETL\my_played_tracks.sqlite'
USER_ID = ''                                                                                    #Spotify Username
TOKEN = ''                                                                                      #OAuth token

#TRANSFORM (i.e. Validate)
def valid_data_check(df) -> bool:
    #Check if DataFrame is empty
    if df.empty:
        print('No songs downloaded. Finishing Execution.')
        return False

    #Ensure primary key is unique
    if df['played_at'].is_unique:
        pass
    else:
        raise Exception ('Primary Key check is violated')
    
    #Check for null values
    if df.isnull().values.any():
        raise Exception ('Null values found')
    
    #Check that all the songs are from yesterday
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df['timestamp'].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception ("At least one of the returned songs does not have a yesterday's timestamp")
    
    return True


if __name__ == '__main__':
    headers = {
        'GET':'https://api.spotify.com/v1/me/player/recently-played?limit=10&after=1484811043508',
        'Accept':'application/json',
        'Content-Type':'application/json',
        'Authorization': 'Bearer {token}'.format(token=TOKEN)
    }
#EXTRACTION
# Convert time to Unix timestamp in miliseconds     
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get('https://api.spotify.com/v1/me/player/recently-played?after={time}'.format(time=yesterday_unix_timestamp), headers = headers)
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song['track']['name'])
        artist_names.append(song['track']['album']['artists'][0]['name'])
        played_at_list.append(song['played_at'])
        timestamps.append(song['played_at'][0:10])

    song_dict ={
        'song_name':song_names,
        'artist_name':artist_names,
        'played_at':played_at_list,
        'timestamp':timestamps
    }

    song_df = pd.DataFrame(song_dict, columns=['song_name', 'artist_name', 'played_at', 'timestamp'])

#LOAD 
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = '''
        CREATE TABLE IF NOT EXISTS my_played_tracks(
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200),
            timestamp VARCHAR(200),
            CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
        )
    '''
    cursor.execute(sql_query)
    print('Database created successfully.')

    try:
        song_df.to_sql('my_played_tracks', engine, if_exists='append', index=False)
    except:
        print('Database already exists')
    
    conn.close()
    print('Connection to Database closed')
