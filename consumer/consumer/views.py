from kafka import KafkaProducer, KafkaConsumer
from django.http import HttpResponse

def my_view(request):
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Send a message to a Kafka topic
    topic = 'my_topic'
    message = 'Hello, Kafka!'
    producer.send(topic, message.encode('utf-8'))

    # Other view code...

    return HttpResponse('Message sent to Kafka')

def send_message(request):
    # Create a Kafka producer
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # Send a message to a Kafka topic
    topic = 'my_topic'
    message = 'Hello, Kafka!'
    producer.send(topic, message.encode('utf-8'))

    return HttpResponse('Message sent to Kafka')

def receive_message(request):
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'my_topic',
        bootstrap_servers='localhost:9092',
        group_id='my_consumer_group',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: x.decode('utf-8')
    )

    # Consume messages from the Kafka topic
    messages = []
    for message in consumer:
        # Process the message
        messages.append(message.value)

    consumer.close()

    # Display the consumed messages on the screen
    response = "<br>".join(messages)
    return HttpResponse(response)


