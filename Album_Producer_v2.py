from kafka import KafkaProducer
import SpotifyGetData
import pickle
import time
import json


def json_serializer(data):
    return json.dumps(data).encode('utf-8')


spotipyGetData = SpotifyGetData.MySpotify()
# producer = KafkaProducer(bootstrap_servers='localhost:9092',
#                          value_serializer=json_serializer)
topic_album = 'albums'

artists = spotipyGetData.artist_genre()
i = 0
for artist in artists:
    i = i+1
    # print(i)
    print(artist['id'])
    id = artist['id']
    albums = spotipyGetData.album_by_artists(id)
    for album in albums:
        print(album)
        # print('+++++++++++=')
        # producer.send(topic_album, album)
    print('-----------------------------')
