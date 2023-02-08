import os
import spotipy
import json
import pandas as pd
import re
from spotipy.oauth2 import SpotifyClientCredentials


class MySpotify(object):

    def __init__(self, genre='k-pop'):
        self.authenticate()
        self.genre = genre

    def authenticate(self):
        my_id = 'c5d1c91cb16f4232b0e2795b2e0ccb9b'
        secret_key = '76bbde0512854278be331b88c148d864'
        client_credentials_manager = SpotifyClientCredentials(
            client_id=my_id, client_secret=secret_key)
        self.sp = spotipy.Spotify(
            client_credentials_manager=client_credentials_manager)

    def artist_genre(self):
        artist = []

        for i in range(812, 962, 50):
            artist += self.sp.search(q='genre:'+self.genre, limit=50,
                                     type='artist', offset=i)['artists']['items']
        return artist

    def album_by_artists(self, id):
        albums = []
        ab = self.sp.artist_albums(id, limit=50)

        if ab['total'] > 50:
            for j in range(0, ab['total'], 50):
                albums += self.sp.artist_albums(
                    id, limit=50, offset=j)['items']
        else:
            albums += self.sp.artist_albums(
                id, limit=50)['items']
        return albums

    def songs_albums(self, id):
        songs = []
        ab = self.sp.album_tracks(id, limit=50)

        if ab['total'] > 50:
            for j in range(0, ab['total'], 50):
                songs += self.sp.album_tracks(
                    id, limit=50, offset=j)['items']
        else:
            songs += self.sp.album_tracks(
                id, limit=50)['items']
        return songs

    def track_feature(self, id_track):
        feature = self.sp.audio_features(id_track)
        return feature[0]

    def track_popularity(self, id):
        popularity = self.sp.track(id)['popularity']
        return popularity


if __name__ == "__main__":

    pass
