import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

channel.queue_declare(queue='commands')


def send_command(command):
    channel.basic_publish(exchange='',
                          routing_key='commands',
                          body=json.dumps(command))
    print(f'Command sent: {command}')


while True:
    command_type, variable, value = input(
        'Enter the command type (\'add\' or \'mul\'),\n' + 'a variable (\'x\', \'y\' or \'z\'),\n' + 'and value: ').split()
    value = float(value)
    if command_type not in ['add', 'mul']:
        print('Invalid command type. Valid values: \'add\' or \'mul\'.')
        continue
    if variable not in ['x', 'y', 'z']:
        print('Invalid variable name. Valid values: \'x\', \'y\' or \'z\'.')
        continue

    command = {'command': command_type, 'variable': variable, 'value': value}
    send_command(command)

connection.close()
