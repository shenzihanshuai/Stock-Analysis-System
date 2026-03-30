from gevent import monkey
monkey.patch_all()

import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from kafka import KafkaConsumer

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")
thread = None
consumer = KafkaConsumer(
    'stock_indicators',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='flask-consumer',
    auto_offset_reset='latest',
    enable_auto_commit=True
)
# for msg in consumer:
#     data_list = msg.value 
#     print(data_list)

def background_thread():
    print("Kafka consumer started")
    try:
        for msg in consumer:
            data = msg.value
            # print("Received data:", data)
            socketio.emit('test_message', data)
    except Exception as e:
        print("Kafka consumer error:", e)


@socketio.on('test_connect')
def connect(message):
    print(message)
    global thread
    if thread is None:
        thread = socketio.start_background_task(target=background_thread)
    socketio.emit('connected', {'data': 'Connected'})


@app.route("/")
def handle_mes():
    return render_template("index.html")


if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', debug=True)