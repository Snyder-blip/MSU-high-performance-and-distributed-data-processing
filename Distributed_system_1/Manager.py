import sys
import json
import pika
import uuid

broker_host, storage_procs = sys.argv[1:]
storage_procs = int(storage_procs)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=broker_host))
channel = connection.channel()

# Consume from ETL
channel.exchange_declare(exchange='etl_to_manager', exchange_type='direct')

result = channel.queue_declare('', exclusive=True)
ETL_queue_name = result.method.queue

channel.queue_bind(
    exchange='etl_to_manager', queue=ETL_queue_name, routing_key='')

id_name = {}


def Manager_id_name_callback(channel, method, properties, body):
    global id_name
    id_name = json.loads(body)
    channel.stop_consuming()


print(' [*] Waiting for \'id_name\'')
channel.basic_consume(
    queue=ETL_queue_name,
    on_message_callback=Manager_id_name_callback,
    auto_ack=True)
channel.start_consuming()

process_features = {}


def Manager_process_features_callback(channel, method, properties, body):
    global process_features
    temp_dict = json.loads(body)
    process_features[next(iter(temp_dict))] = next(iter(temp_dict.values()))
    if len(process_features) >= storage_procs:
        channel.stop_consuming()


print(' [*] Waiting for \'process_features\'')
channel.basic_consume(
    queue=ETL_queue_name,
    on_message_callback=Manager_process_features_callback,
    auto_ack=True)
channel.start_consuming()

# Storage Process communication
class RpcStorageProcess(object):
    def __init__(self, process_id):
        self.process_id = process_id
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=broker_host))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, request):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='manager_storage_process_rpc_{id}'.format(
                id=self.process_id),
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(request))
        self.connection.process_data_events(time_limit=None)
        return json.loads(self.response)


# Client communication
feature_processes = {}
for key, values in process_features.items():
    for value in values:
        if value in feature_processes:
            feature_processes[value].append(key)
        else:
            feature_processes[value] = [key]


def get_merged_response(feature_id, feature_processes):
    merged_response = []
    for process_id in feature_processes[feature_id]:
        rpc = RpcStorageProcess(process_id)
        response = rpc.call({'Feature id': str(feature_id)})
        merged_response += next(iter(response.values()))
    return merged_response


channel.queue_declare(queue='client_to_manager_rpc')


def on_request(ch, method, props, body):
    request = json.loads(body)['Request']
    if request[0] == 'GetNames':
        response = id_name
    if request[0] == 'GetCoords':
        response = {'Coords': get_merged_response(
            int(request[1]), feature_processes)}
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id),
        body=json.dumps(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='client_to_manager_rpc',
    on_message_callback=on_request)

print(" [x] Awaiting Client RPC requests")
channel.start_consuming()
