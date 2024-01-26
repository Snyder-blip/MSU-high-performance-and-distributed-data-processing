import sys
import json
import pika
import uuid

broker_host = sys.argv[1]

# Manager communication
class RpcClient(object):
    def __init__(self):
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
            routing_key='client_to_manager_rpc',
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(request))
        self.connection.process_data_events(time_limit=None)
        return json.loads(self.response)


print('Available queries:\n \
    - \'GetCoords\': id\n \
    - \'GetNames\': add \'--ids\' option for names with ids')

print('Type \'Exit\' for exit')

rpc = RpcClient()
while True:
    query = input('Entery query: ').split()
    if query[0] == 'GetCoords':
        response = rpc.call({'Request': [query[0], query[1]]})
        print(response['Coords'])
    if query[0] == 'GetNames':
        response = rpc.call({'Request': [query[0]]})
        if len(query) == 2 and query[1] == '--ids':
            print(response)
        else:
            print(list(filter(None, set(response.values()))))
    if query[0] == 'Exit' or query[0] == 'exit':
        break
