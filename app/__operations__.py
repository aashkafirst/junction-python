from sklearn.metrics.pairwise import pairwise_distances
import numpy as np

import __kmedoids__ as kmedoids

BUCKET_LEVELS_LIMIT = 5

def get_bucket_levels(bucket):
    lower_integer = ((bucket.split(":")[0]).split(",")[0]).split("[")[1]
    lower_fractional = (bucket.split(":")[0]).split(",")[1]
    lower = float( lower_integer + "." + lower_fractional)

    upper_integer = (bucket.split(":")[1]).split(",")[0]
    upper_fractional = ((bucket.split(":")[1]).split(",")[1]).split("]")[0]
    upper = float( upper_integer + "." + upper_fractional )

    return ({
        "lower": lower,
        "upper": upper
    })


def check_falls_in_bucket(bucket, drop):
    bucket_levels = get_bucket_levels(bucket)
    lower = bucket_levels['lower']
    upper = bucket_levels['upper']
    print("", lower, ":", upper)

    if ((lower - BUCKET_LEVELS_LIMIT) <= drop['start'] <= (lower + BUCKET_LEVELS_LIMIT)
        and (upper - BUCKET_LEVELS_LIMIT) <= drop['end'] <= (upper + BUCKET_LEVELS_LIMIT)):
        return True

    return False


def belongs_to_bucket(drop, video_id, video_docs):
    buckets = video_docs.find_one(
        { "videoId": video_id },
        { "duration": 0, "_id": 0, "videoId": 0 }
    )

    for bucket in buckets:
        print("bucket from equal=", bucket)
        falls_in_bucket = check_falls_in_bucket(bucket, drop)
        print("ans=", falls_in_bucket)
        if falls_in_bucket:
            return bucket

    return None

def get_new_bucket_name(video_id, video_docs, bucket):
    drops = video_docs.find_one(
                { "videoId" : video_id },
                { bucket : 1 }
            )[bucket]
    print ("drops:",drops)
    no_of_drops = len(drops)
    print ("length: ", no_of_drops)
    if no_of_drops == 2:
        if drops[0]['start'] <= drops[1]['start'] <= drops[0]['end'] and drops[0]['start'] <= drops[1]['end'] <= drops[0]['end']:
            lower = str(drops[0]['start']).split('.')
            upper = str(drops[0]['end']).split('.')
        else:
            lower = str(drops[1]['start']).split('.')
            upper = str(drops[1]['end']).split('.')
        new_bucket_name = "[" + lower[0] + "," + lower[1] + ":" + upper[0] + "," + upper[1] + "]"
    else:
        bucket = []
        for drop in drops:
            print ("drop:",drop)
            bucket.append([ drop['start'], drop['end'] ])
        bucket = np.array(bucket)
        print('bucket', bucket)
        distance_matrix = pairwise_distances(bucket, metric='euclidean')
        medoid = kmedoids.kMedoids(distance_matrix, 1)
        print('medoid', medoid)
        new_bucket_name = bucket[medoid[0]]
        print(type(new_bucket_name), new_bucket_name[0], new_bucket_name[1])
        lower = str( new_bucket_name[0] ).split('.')
        upper = str( new_bucket_name[1] ).split('.')
        new_bucket_name = "[" + lower[0] + "," + lower[1] + ":" + upper[0] + "," + upper[1] + "]"
        print("newBucketName=", new_bucket_name)
    return new_bucket_name

def rename_bucket(video_id, video_docs, bucket, new_bucket_name):
    if bucket != new_bucket_name:
        video_docs.update(
            {'videoId': video_id},
            {'$rename': { bucket : new_bucket_name } }
        )

def add_to_bucket(drop, video_id, video_docs, bucket):
    video_docs.update(
        { "videoId" : video_id },
        { "$push" : { bucket : drop }}
    )
    new_bucket_name = get_new_bucket_name(video_id, video_docs, bucket)
    rename_bucket(video_id, video_docs, bucket, new_bucket_name)

def create_bucket(drop, video_id, video_docs):
    lower = str( drop['start'] ).split('.')
    upper = str( drop['end'] ).split('.')
    bucket_name = "[" + lower[0] + "," + lower[1] + ":" + upper[0] + "," + upper[1] + "]"
    print ("bucket_name=", bucket_name)
    video_docs.update(
        { 'videoId': video_id },
        { '$push': { bucket_name : drop } }
    )

def get_all_buckets(video_docs, video_id):
    buckets_dict = []
    i = 0
    buckets = video_docs.find_one(
        {"videoId": video_id},
        {"duration": 0, "_id": 0, "videoId": 0}
    )
    bucket_list = list(buckets)
    for bucket in buckets:
        buckets_dict.append(
            {
                'levels' : get_bucket_levels(bucket),
                'views' : len( video_docs.find_one( {"videoId": video_id}, {bucket_list[i]: 1} )[bucket_list[i]] ),
                'duration' : 0
            }
        )
        buckets_dict[i]['duration'] = buckets_dict[i]['levels']['upper'] - buckets_dict[i]['levels']['lower']
        i = i + 1
    print(buckets_dict)
    return buckets_dict

def get_sorted_buckets(video_docs, video_id):
    buckets = get_all_buckets(video_docs, video_id)
    sorted_buckets = []
    print("buckets from getSortedBuckets=", buckets)
    print("len of buckets", len(buckets))
    for i in range(0, len(buckets) - 1):
        for j in range(i + 1, len(buckets)):
            views_1 = buckets[i]['views']
            views_2 = buckets[j]['views']
            print("views", views_1, views_2)
            if views_1 < views_2:
                temp = buckets[i]
                buckets[i] = buckets[j]
                buckets[j] = temp
                print("swapped=", buckets[i], buckets[j], buckets)
    print("sorted buckets from getSortedBuckets=", buckets)
    return buckets

def convert_to_crisp(bucket):
    return({
        'start' : bucket['levels']['lower'],
        'end' : bucket['levels']['upper'],
        'duration' : bucket['levels']['upper'] - bucket['levels']['lower'],
        'views' : bucket['views']
    })

def get_superset(bucket, crisps):
    i=0
    for crisp in crisps:
        if crisp['start'] <= bucket['levels']['lower'] <= crisp['end'] and crisp['start'] <= bucket['levels']['upper'] <= crisp['end']:
            if crisp['views'] == bucket['views']:
                return i
        i=i+1
    return None

def get_overlap(bucket, crisps):
    i=0
    for crisp in crisps:
        if crisp['start'] <= bucket['levels']['lower'] <= crisp['end']:
            if crisp['views'] == bucket['views']:
                return({
                    'lower' : bucket['levels']['lower'],
                    'upper' : crisp['end']
                })
        if crisp['start'] <= bucket['levels']['upper'] <= crisp['end']:
            if crisp['views'] == bucket['views']:
                return ({
                    'lower': crisp['start'],
                    'upper': bucket['levels']['upper']
                })
        i=i+1
    return None

def remove_overlap(bucket, overlap):
    if overlap['levels']['lower'] == bucket['levels']['lower']:
        bucket['levels'] = {
            'lower': overlap['levels']['upper'],
            'upper': bucket['levels']['upper']
        }
    if overlap['levels']['upper'] == bucket['levels']['upper']:
        bucket['levels'] = {
            'lower' : bucket['levels']['lower'],
            'upper' : overlap['levels']['lower']
        }
    bucket['duration'] = bucket['levels']['upper'] - bucket['levels']['lower']
    return bucket