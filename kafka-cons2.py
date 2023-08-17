import json
from confluent_kafka import Consumer
import requests

# Kafka broker address and topic name
bootstrap_servers = 'localhost:9092'
topic_name = 'orange_users_topic1'

# Backend server URL
backend_url = 'http://127.0.0.1:8000/api/data/'  # Replace with your actual backend server URL

# Create Kafka consumer configuration
consumer_config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'your_consumer_group_id',
    'auto.offset.reset': 'earliest',  # Start consuming from the beginning of the topic
    'enable.auto.commit': False  # Disable automatic offset commits
}

# Create Kafka consumer
consumer = Consumer(consumer_config)

# Subscribe to the Kafka topic
consumer.subscribe([topic_name])

try:
    order_of_arrival = 0  # Initialize the order of arrival counter
    while True:
        msg = consumer.poll(timeout=1.0)
        print('reading')
        if msg is None:
            print("continuing")
            continue
        elif msg.error():
            print(f'consumer error: {msg.error()}')
            break
        else:
            value = msg.value().decode('utf-8')

            #print(f'Received message: {value}')
            item = json.loads(value)
            item['arrival'] = order_of_arrival
            userid = item['User Id']
            name = item['First Name']
            order_of_arrival += 1 
            #notif=model.predict(msg.value()) # Increment the order of arrival counter
            
            # Create a dictionary with userid and name
            data_to_send = {
                'userid': userid,
                'name': name,
                
            }
            print(data_to_send)

            # Send the data as a POST request to the backend server
            response = requests.post(backend_url, json=data_to_send)
            if response.status_code == 200:
                print("Data sent to backend successfully")
            else:
                print(f"Failed to send data to backend. Status code: {response.status_code}")

        consumer.commit(asynchronous=False)
except KeyboardInterrupt:
    # Gracefully stop the consumer on keyboard interrupt
    consumer.close()
