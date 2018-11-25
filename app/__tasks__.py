import __database__ as database
import __operations__ as operations

def save_crisps_to_db(video_json):
    video_docs = database.connect('ShortOkPlz', 'videoDocs')
    video_doc = video_docs.find_one({
                    "videoId" : video_json['videoId']
                })
    if not( video_doc ):
        video_docs.insert_one({
            "videoId" : video_json['videoId'],
            "duration" : video_json['duration']
        })

    for crisp in video_json['crisps']:
        print("crisp=", crisp)
        bucket = operations.belongs_to_bucket(
                    crisp,
                    video_json['videoId'],
                    video_docs
                )
        if bucket:
            operations.add_to_bucket(
                crisp,
                video_json['videoId'],
                video_docs,
                bucket
            )
        else:
            operations.create_bucket(
                crisp,
                video_json['videoId'],
                video_docs
            )

def get_crisps(video_id, video_duration):
    crisps_duration_limit = 0.35 * video_duration
    crisps_duration = 0
    crisps = []
    video_docs = database.connect('ShortOkPlz', 'videoDocs')
    buckets = operations.get_sorted_buckets( video_docs, video_id )
    for bucket in buckets:
        if crisps_duration_limit > 0:
            superset_index = operations.get_superset(bucket, crisps)
            if superset_index != None:
                print("inside superset: ")
                crisps_duration_limit += crisps[superset_index]['duration']
                crisps_duration -= crisps[superset_index]['duration']
                crisps[superset_index] = operations.convert_to_crisp(bucket)
            else:
                overlap = operations.get_overlap(bucket, crisps)
                print("overlap:", overlap)
                if overlap != None:
                    bucket = operations.remove_overlap(bucket, overlap)
                    print("subtract:", bucket)
                    crisps.append( operations.convert_to_crisp(bucket) )
                else:
                    print("inside new")
                    crisps.append( operations.convert_to_crisp(bucket) )
            crisps_duration_limit -= bucket['duration']
            crisps_duration += bucket['duration']
    print('crisps', crisps, crisps_duration_limit, crisps_duration)
    return crisps, crisps_duration

def save_processed_crisps_to_db(videoId, crisps_duration, crisps):
    crisp_docs = database.connect('ShortOkPlz', 'crisps')
    crisp_doc = crisp_docs.find_one({
        "videoId": videoId
    })
    if not(crisp_doc):
        crisp_docs.insert_one({
            "videoId" : videoId,
            "duration" : crisps_duration,
            "summary" : crisps
        })
    else:
        crisp_docs.update(
            { 'videoId' : videoId },
            { '$set' : { "summary" : crisps } }
        )