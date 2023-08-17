from confluent_kafka import Producer


import json

# Kafka broker address and topic name
bootstrap_servers = 'localhost:9092'
topic_name = 'orange_users_topic1'

# Create Kafka producer configuration
producer_config = {
    'bootstrap.servers': bootstrap_servers
}

# Create Kafka producer
producer = Producer(producer_config)

# Path to the JSON file
json_file_path = 'people-100.json'

# Function to handle delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# Read JSON file and send each record as a message to Kafka topic
with open(json_file_path, 'r') as file:
    records = json.load(file)

    for record in records:
        # Convert the record to JSON string
        message = json.dumps(record)

        # Produce the message to Kafka topic
        producer.produce(topic_name, message, callback=delivery_report)

        # close producer
        producer.flush()
