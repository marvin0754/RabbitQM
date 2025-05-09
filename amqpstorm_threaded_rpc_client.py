"""
This is a simple example on how to use Flask and Asynchronous RPC calls.

I kept this simple, but if you want to use this properly you will need
to expand the concept.

Things that are not included in this example.
    - Reconnection strategy.

    - Consider implementing utility functionality for checking and getting
      responses.

        def has_response(correlation_id)
        def get_response(correlation_id)

Apache/wsgi configuration.
    - Each process you start with apache will create a new connection to
      RabbitMQ.

    - I would recommend depending on the size of the payload that you have
      about 100 threads per process. If the payload is larger, it might be
      worth to keep a lower thread count per process.

For questions feel free to email me: me@eandersson.net
"""
__author__ = 'eandersson'

import threading
from time import sleep
from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient:
    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.rpc_queue = rpc_queue
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.open()

    def open(self):
        self.connection = amqpstorm.Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._start_consume_thread()

    def _start_consume_thread(self):
        thread = threading.Thread(target=self._consume)
        thread.daemon = True
        thread.start()

    def _consume(self):
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id

# Instancias de cliente RPC
RPC_CLIENT1 = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')
RPC_CLIENT2 = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')
RPC_CLIENT3 = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')


@app.route('/caso1')
def caso_1():
    corr_id = RPC_CLIENT1.send_request("Hola desde Caso 1")
    while RPC_CLIENT1.queue[corr_id] is None:
        sleep(0.1)
    return f"Respuesta del Servidor al Caso 1: {RPC_CLIENT1.queue[corr_id]}"

@app.route('/caso2')
def caso_2():
    corr_id = RPC_CLIENT1.send_request("")
    while RPC_CLIENT1.queue[corr_id] is None:
        sleep(0.1)
    return f"Respuesta del Servidor al Caso 2 (mensaje vacío): {RPC_CLIENT1.queue[corr_id]}"

@app.route('/caso3')
def caso_3():
    mensaje_largo = "¡@#%&" + "Texto muy largo..." * 1000
    corr_id = RPC_CLIENT1.send_request(mensaje_largo)
    while RPC_CLIENT1.queue[corr_id] is None:
        sleep(0.1)
    return f"Respuesta del Servidor al Caso 3 (mensaje largo): {RPC_CLIENT1.queue[corr_id]}"

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=True, threaded=True)