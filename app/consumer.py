from kafka import KafkaConsumer
import json

import __tasks__ as tasks

consumer = KafkaConsumer('topic',bootstrap_servers=['localhost:9092'])

print ('Start consuming')

for message in consumer:
    video_json = json.loads(message.value.decode('utf-8'))
    print ("message", video_json)
    #div=json.loads(message.value).decode('utf-8')
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          video_json))
    tasks.save_crisps_to_db( video_json )
    crisps, crisps_duration = tasks.get_crisps( video_json['videoId'], video_json['duration'] )
    tasks.save_processed_crisps_to_db( video_json['videoId'], crisps_duration, crisps )