from confluent_kafka import Consumer, KafkaError
import csv
import os
from datetime import datetime


csv_file = "data.csv"

conf = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'my-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

topic = 'conn-events'  
consumer.subscribe([topic])


while True:
    msg = consumer.poll(1.0)
    
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            print('Reached end of partition')
        else:
            print(f'Error: {msg.error()}')
    else:
        print(f'received {msg.value().decode("utf-8")}')
        data_price = [(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), msg.value().decode("utf-8"))]
        if not os.path.exists(csv_file):
            with open(csv_file, mode="w", newline="") as file:
                writer = csv.writer(file)
                writer.writerow(["time", "price"])  # Header
                writer.writerows(data_price)
        else:
            with open(csv_file, mode="a", newline="") as file:
                writer = csv.writer(file)
                writer.writerows(data_price)

