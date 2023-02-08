from kafka import KafkaProducer
import SpotifyGetData
import pickle
import time
import json


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


spotipyGetData = SpotifyGetData.MySpotify()
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=json_serializer)
topic_track = 'tracks'
topic_artist = 'artists'
topic_album = 'albums'
artists = spotipyGetData.artist_genre()
i = 0
for artist in artists:
    i += 1
    print(i)
    print('__________')
    # producer.send(topic_artist, artist)
    id = artist['id']
    albums = spotipyGetData.album_by_artists(id)
    for album in albums:
        producer.send(topic_album, album)
        id_album = album['id']
        tracks = spotipyGetData.songs_albums(id_album)
        for track in tracks:
            id_track = track["id"]
            track_feture = spotipyGetData.track_feature(id_track=id_track)
            track["feature"] = track_feture
            track["popularity"] = spotipyGetData.track_popularity(id_track)
            track["release_year"] = album['release_date'][:4]
            producer.send(topic_track, track)
