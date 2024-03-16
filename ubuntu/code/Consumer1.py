from kafka import KafkaConsumer, KafkaProducer
import json
import time

def consumer1():
    consumer = KafkaConsumer('users_info', bootstrap_servers=['broker:29092'])
    producer = KafkaProducer(bootstrap_servers=['broker:29092'])
    for message in consumer:
        data = json.loads(message.value)
        data['timestamp'] = time.time()
        print(data)
        producer.send('users_info_timestamp', json.dumps(data).encode('utf-8'))

if __name__ == "__main__":
    consumer1()