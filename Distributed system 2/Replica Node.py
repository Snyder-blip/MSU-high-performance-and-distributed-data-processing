import pika
import json
import sys

current_end = 0
variables = {'x': 0, 'y': 0, 'z': 0}

variables['x'], variables['y'], variables['z'] = [
    float(arg) for arg in sys.argv[1:-1]]
my_id = int(sys.argv[-1])

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.exchange_declare(exchange='replica_node', exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='replica_node',
                   queue=queue_name, routing_key=f'id.{my_id}')


def update_variables(update):
    global variables
    if update['command'] == 'add':
        variables[update['variable']] += update['value']
    elif update['command'] == 'mul':
        variables[update['variable']] *= update['value']


updates = {}


def process_update(update):
    global variables
    global current_end
    global updates
    updates[update['id']] = update
    if (current_end == update['id']):
        id = update['id']
        while id in updates:
            update_variables(updates.pop(id))
            print(
                f'I\'m Replica. My ID: {my_id}, update ID: {id}, variables: {variables}')
            id += 1
        current_end = id


def callback(ch, method, properties, body):
    update = json.loads(body)
    process_update(update)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
