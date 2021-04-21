import pandas as pd
import numpy as np
from tqdm import tqdm
import os

import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import json

def spotipy_instance():
    # Get credential from file
    with open("credentials.json","r") as f:
        credentials = json.loads(f.read())

    # Request Client Credentials
    auth_manager = SpotifyClientCredentials(client_id=credentials["client_id"], client_secret=credentials["client_secret"])
    sp = spotipy.Spotify(auth_manager=auth_manager)

    return sp


def get_starting_index(url_data_path):

    file_exist = os.path.exists(url_data_path)

    if file_exist:
        index_df = pd.read_csv(url_data_path)
        starting_index = len(index_df)
    else:
        starting_index = 0
    
    return starting_index, file_exist

def main():

    url_data_path = "dataset/tracks_url.csv"
    starting_index,file_exist = get_starting_index(url_data_path)

    df = pd.read_csv("raw_dataset/tracks.csv")
    new_df = df.drop(columns=["name","popularity","duration_ms","artists","id_artists","release_date"])
    new_df["sample_mp3_url"]=""

    new_df=new_df[starting_index:]

    sp = spotipy_instance()

    for batch_i in tqdm(range(len(new_df["id"])//50+1)):
        tracks = new_df["id"][batch_i*50:batch_i*50+50]
        result = sp.tracks(tracks=tracks)
        for i in range(len(result["tracks"])):
            new_df.loc[batch_i*50+i,"sample_mp3_url"]=result["tracks"][i]["preview_url"]
        if batch_i == 0:
            header = not file_exist
        else:
            header = False
        new_df[batch_i*50:batch_i*50+50].to_csv("dataset/tracks_url.csv",mode='a', index=False, header=header)

if __name__ == "__main__":
    main()