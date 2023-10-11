import sys
import json
import pika

broker_host, my_id = sys.argv[1:]
my_id = int(my_id)

storage = {}

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=broker_host))
channel = connection.channel()

# Consume from ETL
channel.exchange_declare(
    exchange='etl_to_storage_process',
    exchange_type='topic')

result = channel.queue_declare('', exclusive=True)
ETL_queue_name = result.method.queue

channel.queue_bind(
    exchange='etl_to_storage_process',
    queue=ETL_queue_name,
    routing_key='storage_process.{id}'.format(
        id=my_id))


def ETL_callback(channel, method, properties, body):
    global storage
    result = json.loads(body)
    if result == {'Status': 'End'}:
        channel.stop_consuming()
        return
    coords = result
    key = next(iter(coords))
    value = next(iter(coords.values()))
    if key in storage:
        storage[key].append(value)
    else:
        storage[key] = [value]


print(' [*] Waiting for coords from ETL')
channel.basic_consume(
    queue=ETL_queue_name, on_message_callback=ETL_callback, auto_ack=True)
channel.start_consuming()

# Manager communication
channel.queue_declare(
    queue='manager_storage_process_rpc_{id}'.format(
        id=my_id))


def on_request(ch, method, props, body):
    request = json.loads(body)['Feature id']
    response = {
        '{id}'.format(
            id=my_id): storage[request] if request in storage else []}

    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id),
        body=json.dumps(response))
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(
    queue='manager_storage_process_rpc_{id}'.format(
        id=my_id), on_message_callback=on_request)

print(" [x] Awaiting Manager RPC requests")
channel.start_consuming()
