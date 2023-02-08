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
topic_album = 'albums'
albums = spotipyGetData.albumn()

i = 0
for album in albums:
    i = i+1
    print(i)
    print(album)
    print('-----------------------------')
    producer.send(topic_album, album)
    time.sleep(1)
