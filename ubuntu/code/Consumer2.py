from kafka import KafkaConsumer, KafkaProducer
import json
from faker import Faker

def consumer2():
    consumer = KafkaConsumer('users_info_timestamp', bootstrap_servers=['broker:29092'])
    producer = KafkaProducer(bootstrap_servers=['broker:29092'])
    fake = Faker()
    
    for message in consumer:
        data = json.loads(message.value)
        data['label'] = fake.job()
        print(data)
        producer.send('users_info_timestamp_label', json.dumps(data).encode('utf-8'))

if __name__ == "__main__":
    consumer2()
