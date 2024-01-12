import pandas as pd
import numpy as np

def get_song_ids(df: pd.DataFrame):
    """
    Get the ID of the songs
    """
    import time
    
    list_of_ids = []
    
    # First, we are creating chunks:
    chunk_size = 50
    
    for start in range(0, len(df), chunk_size):
        chunk = df[start:start+chunk_size]
        
        for index, row in chunk.iterrows():
            try:
                search_song = sp.search(q=row['title']+" "+row['artist'],limit=1)
                #search_song = sp.search(q=df['title'][1]+" "+df['artist'][1],limit=1)
                #search_song = sp.search(q=row['title'], limit=1)
                song_id = search_song['tracks']['items'][0]['id']
                list_of_ids.append(song_id)
            
            except:
                print("Song not found!")
                list_of_ids.append("")
                
        print("Sleeping a bit before getting the next ids")
        time.sleep(10)
        
    return list_of_ids
    
    
    
def add_audio_features(df, audio_features_df):
    """
    Concats a given dataframe with the audio features dataframe and return the extended data frame. 
    """
    
    final_df = pd.concat([df, audio_features_df], axis=1)
    
    return final_df
    
def get_song_ids(df: pd.DataFrame):
    """
    Using spotipy.search to get IDs of songs stored in the df.
    
    The input df should contain a column with song 'title' and song 'artist'.
    
    Returns a list with the song IDs.
    """
    import time
    
    list_of_ids = []
    
    # define a chunk size
    chunk_size = 50
    
    for start in range(0, len(df), chunk_size):
        chunk = df[start:start+chunk_size]
        
        for index, row in chunk.iterrows():
            try:
                search_song = sp.search(q=row['title']+" "+row['artist'],limit=1)
                song_id = search_song['tracks']['items'][0]['id']
                list_of_ids.append(song_id)
            
            except:
                print("Song not found!")
                list_of_ids.append("")
                
        print(f"Processed {start+chunk_size} songs. Now sleeping a bit.")
        time.sleep(10)
        
    return list_of_ids
    
    
    
def get_audio_features(list_of_song_ids: list):
    """
    Using the song IDs stored in a list to get the audio features out of the Spotify Database.
    Performs a bulk request of 50 IDs at once to retrieve the audio features.
    Returns a dataframe with the corresponding audio features.
    """
    import time
    from config import client_id, client_secret
    import spotipy as sp
    import json
    from spotipy.oauth2 import SpotifyClientCredentials

    sp = sp.Spotify(auth_manager=SpotifyClientCredentials(client_id=client_id,client_secret=client_secret))
   
    #feature_list = []
    feature_df = pd.DataFrame()

    # define a chunk size
    chunk_size = min(len(list_of_song_ids), 50)
    
    for start in range(0, len(list_of_song_ids), chunk_size):
        #for start in range(0,100,50) -> chunk_size is the increment. second loop will start from chunk_size
        try:
            features = sp.audio_features(tracks=list_of_song_ids[start:start+chunk_size])
            #features = sp.audio_features(tracks='id1, id2, ..., id49')
        
            for f in features:
                df_temp = pd.DataFrame([f])
                feature_df = pd.concat([feature_df, df_temp], ignore_index=True)
        
        except:
            print("Error processing tracks")

    print(f"Processed {start+chunk_size} songs. Now sleeping a bit.")
    time.sleep(10)
    
    return feature_df
    
    
    
def add_audio_features(df, audio_features_df):
    """
    Concats a given dataframe with the audio features dataframe and return the extended data frame. 
    """
    
    final_df = pd.concat([df, audio_features_df], axis=1)
    
    return final_df
    