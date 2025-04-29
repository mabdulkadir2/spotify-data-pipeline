import os
import boto3
import json
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from datetime import datetime

def lambda_handler(event, context):
    # client key & secret
    client_key = os.getenv('client_id')
    client_secret = os.getenv('client_secret')
    
    # credentials
    credentials = spotipy.SpotifyClientCredentials(client_id = client_key, 
                                                   client_secret = client_secret)
                                                   
    # access variable
    spotify = spotipy.Spotify(client_credentials_manager = credentials)
    
    # going into detail on rapcaviar, grabbing url
    rap_caviar_url = 'https://open.spotify.com/playlist/37i9dQZF1DX0XUsuxWHRQd'

    # grabbing uri next
    uri = rap_caviar_url.split('/')[4]
    
    # full json output of today's playlist
    rap_caviar = spotify.playlist_tracks(uri)
    
    # name of file in s3 bucket
    file_name = 'spotify_raw_' + str(datetime.now()) + '.json'
    
    # data extraction & storing inside to processed folder
    client = boto3.client('s3')
    client.put_object(
        Bucket = 'spotify-etl-project-endtoend',
        Key = 'raw_data/to_processed/' + file_name,
        Body = json.dumps(rap_caviar))

    ## output
    print(rap_caviar)