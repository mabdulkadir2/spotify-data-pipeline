# importing libraries
import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO


# album transformation function
def albums(rap_caviar):
    ## list
    album_list = []

    ## creating a dataframe that appends specific json nests we want from a dictionary into a list
    for row in rap_caviar['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_artist_name = row['track']['album']['artists'][0]['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_external_urls = row['track']['album']['external_urls']['spotify']
        album_elements = {'album_id':album_id, 
                      'album_name':album_name, 
                      'album_artist_name':album_artist_name, 
                      'album_release_date':album_release_date,
                      'album_total_tracks':album_total_tracks, 
                      'album_external_urls':album_external_urls}
        album_list.append(album_elements) # append dictionary within list    
    
    return album_list # returning list of all albums



# songs transformation function
def songs(rap_caviar):
    # creating a list for the songs
    song_list = []

    # songs transformation
    for row in rap_caviar['items']:
        song_name = row['track']['name']
        song_id = row['track']['id']
        song_elements = {'song_id' : song_id, 
                     "song_name" : song_name}
        song_list.append(song_elements)
        
    return song_list # returning list of all songs
       
    

# artist transformation function
def artists(rap_caviar):
    # artist transformation
    artists_list = []

    # artist transformation
    for row in rap_caviar['items']:
        artists_names = row['track']['album']['artists'][0]['name']
        artists_id = row['track']['artists'][0]['id']
        artist_elements = {'artist_id' : artists_id, 
                       'artist_name' : artists_names}
        artists_list.append(artist_elements)
   
    return artists_list # returning list of all artists



# main
def lambda_handler(event, context):
    s3 = boto3.client('s3') # s3
    Bucket = 'spotify-etl-project-endtoend' # s3 bucket
    Key = 'raw_data/to_processed/' # the key in s3 bucket
     
    spotify_list = [] # spotfiy list data
    spotify_keys = [] # keys in s3 bucket list
    album_list = [] # album list
    song_list = [] # song list
    artist_list = [] # artist list
    
    # getting contents in s3 buckets and storing them within lists
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            result = s3.get_object(Bucket = Bucket, Key = file_key)
            content = result['Body']
            jsonObj = json.loads(content.read())
            spotify_list.append(jsonObj)
            spotify_keys.append(file_key)
            
    # looping previous functions
    for data in spotify_list:
        album_list = albums(data)
        song_list = songs(data)
        artist_list = artists(data)
    
    # turning lists into data frames
    albums_df = pd.DataFrame(album_list)
    songs_df = pd.DataFrame(song_list)
    artists_df = pd.DataFrame(artist_list)
    
    # converting album dataframe into csv format and loading into s3 bucket
    albums_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'
    album_buffer = StringIO()
    albums_df.to_csv(album_buffer, index = False)
    album_content = album_buffer.getvalue()
    s3.put_object(Bucket = Bucket, Key = albums_key, Body = album_content)
    
    # converting song dataframe into csv format and loading into s3 bucket
    songs_key = 'transformed_data/songs_data/songs_transformed_' + str(datetime.now()) + '.csv'
    song_buffer = StringIO()
    songs_df.to_csv(song_buffer, index = False)
    song_content = song_buffer.getvalue()
    s3.put_object(Bucket = Bucket, Key = songs_key, Body = song_content)
    
    # converting artist dataframe into csv format and loading into s3 bucket
    artist_key = 'transformed_data/artist_data/artists_transformed_' + str(datetime.now()) + '.csv'
    artist_buffer = StringIO()
    artists_df.to_csv(artist_buffer, index = False)
    artist_content = artist_buffer.getvalue()
    s3.put_object(Bucket = Bucket, Key = artist_key, Body = artist_content)
    
    # using resource to copy from one bucket to another
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : key
        }
        s3.copy_object(
            CopySource = copy_source, 
            Bucket = Bucket, 
            Key = 'raw_data/processed/' + key.split("/")[-1])
        