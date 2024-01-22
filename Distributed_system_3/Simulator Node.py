import pika
import json
from random import shuffle
import sys

N, k = [int(arg) for arg in sys.argv[1:]]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='simulator')

channel.exchange_declare(exchange='replica_node', exchange_type='topic')

accumulated_messages = []


def callback(channel, method, properties, body):
    global accumulated_messages
    accumulated_messages.append(json.loads(body))
    if len(accumulated_messages) >= N:
        for replica in range(k):
            shuffle(accumulated_messages)
            for message in accumulated_messages:
                channel.basic_publish(
                    exchange='replica_node', routing_key=f'id.{replica}', body=json.dumps(message))
        accumulated_messages = []


channel.basic_consume(
    queue='simulator',
    on_message_callback=callback,
    auto_ack=True)

channel.start_consuming()
