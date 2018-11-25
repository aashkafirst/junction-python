from flask import Flask, request
from kafka import KafkaProducer
from kafka.errors import KafkaError
import json, logging, traceback

app = Flask(__name__)

def set_logging_to_file(filename):
    logging.basicConfig(
        filename = filename,
        filemode = 'a+',
        level = logging.DEBUG,
        format = '%(asctime)s - %(levelname)s - %(message)s',
    )

@app.route('/')
def home():
    return "home"

@app.route('/processCrisps', methods=["POST"])
def process_crisps():
    print(request.json)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    d = {
            'videoId': 'RN-vzCl6Bvg',
            'duration': 1909.46900000001,
            'crisps': [
                { "start" : 110.0, "end" : 158.0 },
                { "start" : 277.0, "end" : 367.0 },
                { "start" : 455.0, "end" : 541.0 },
                { "start" : 556.0, "end" : 653.0 },
                { "start" : 725.0, "end" : 740.0 },
                { "start" : 929.0, "end" : 974.0 },
                { "start" : 1050.0, "end" : 1096.0 },
                { "start" : 1169.0, "end" : 1260.0 },
                { "start" : 1289.0, "end" : 1299.0 },
                { "start" : 1412.0, "end" : 1547.0 }
            ]
        }
    video_json = json.dumps(request.json).encode('utf-8')
    #d = json.dumps(d).encode('utf-8')
    future = producer.send('topic', video_json)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError:
        set_logging_to_file('./logs/kafka.log')
        logging.error(traceback.format_exc())
        pass

    return "hi"

if __name__ == "__main__":
    app.run()