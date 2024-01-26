import sys
import json
import pika
import numpy as np

broker_host, file_name, storage_procs = sys.argv[1:]
storage_procs = int(storage_procs)

with open(file_name, encoding='utf-8') as file:
    data = json.load(file)

feature_sizes = np.empty(len(data['features']), dtype=np.int64)
feature_ids = np.empty(len(data['features']), dtype=np.int64)

current_id = 0
for i in range(feature_sizes.size):
    feature_sizes[i] = len(data['features'][i]['geometry']['coordinates'])
    feature_ids[i] = current_id
    current_id += 1

id_name = {}
for i in range(feature_sizes.size):
    temp = data['features'][i]['properties']
    id_name[int(feature_ids[i])] = temp['name'] if 'name' in temp else ''

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=broker_host))
channel = connection.channel()

channel.exchange_declare(exchange='etl_to_manager', exchange_type='direct')
channel.exchange_declare(
    exchange='etl_to_storage_process',
    exchange_type='topic')

# Publish to Manager
channel.basic_publish(
    exchange='etl_to_manager', routing_key='', body=json.dumps(id_name))

coords_per_process = int(feature_sizes.sum()) // storage_procs
iteration = 0
temp_dict = {}
for i in range(feature_sizes.size):
    for j in range(feature_sizes[i]):
        process_id = iteration // coords_per_process
        coords = data['features'][i]['geometry']['coordinates'][j]

        if not temp_dict:
            temp_dict[process_id] = []
        if next(iter(temp_dict)) != process_id:
            # Publish to Manager
            channel.basic_publish(
                exchange='etl_to_manager',
                routing_key='',
                body=json.dumps(temp_dict))

            temp_dict.clear()
        else:
            value = next(iter(temp_dict.values()))
            if value:
                if value[-1] != int(feature_ids[i]):
                    value.append(int(feature_ids[i]))
            else:
                value.append(int(feature_ids[i]))

        # Publish to Storage Process
        channel.basic_publish(exchange='etl_to_storage_process',
                              routing_key='storage_process.{id}'.format(id=process_id),
                              body=json.dumps({int(feature_ids[i]): coords}))

        iteration += 1

channel.basic_publish(
    exchange='etl_to_manager', routing_key='', body=json.dumps(temp_dict))

temp_dict.clear()
for process_id in range(storage_procs):
    channel.basic_publish(exchange='etl_to_storage_process',
                          routing_key='storage_process.{id}'.format(id=process_id),
                          body=json.dumps({'Status': 'End'}))

connection.close()
