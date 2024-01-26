import pika
import json
import sys

update_id = 0
variables = {'x': 0, 'y': 0, 'z': 0}

am_i_leader = bool(sys.argv[1])
my_id, previous_id, next_id = [int(arg) for arg in sys.argv[2:5]]
variables['x'], variables['y'], variables['z'] = [
    float(arg) for arg in sys.argv[5:]]

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='commands')
channel.queue_declare(queue='simulator')

def update_variables(update):
    global variables
    if update['command'] == 'add':
        variables[update['variable']] += update['value']
    elif update['command'] == 'mul':
        variables[update['variable']] *= update['value']


def callback(ch, method, properties, body):
    global update_id
    update = json.loads(body)
    update['id'] = update_id
    update_id += 1

    update_variables(update)
    print(f'I\'m Primary. Updated variables: {variables}')

    channel.basic_publish(
        exchange='', routing_key='simulator', body=json.dumps(update))
    print('I\'m Primary. Sent command and update number {} to Simulator Node'.format(
        update['id']))


print(f'I\'m Primary. Initial values of variables: {variables}')

channel.basic_consume(
    queue='commands', on_message_callback=callback, auto_ack=True)

print('I\'m Primary. Waiting for the command. To exit press CTRL+C')
channel.start_consuming()
