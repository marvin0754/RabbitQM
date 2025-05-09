# main.py
from flask import Flask
import threading
from time import sleep
import amqpstorm
from amqpstorm import Message

# ========== Servidor RPC ==========
def iniciar_servidor_rpc():
    def on_request(message):
        body = message.body  # Ya viene como str
        print(f"[x] Recibido: {body}")
        response = f"Respuesta recibida correctamente: {body}"
        message.channel.basic.publish(
            payload=response,
            exchange='',
            routing_key=message.reply_to,
            properties={'correlation_id': message.correlation_id}
        )
        message.ack()

    connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')
    channel = connection.channel()
    channel.queue.declare('rpc_queue')
    channel.basic.consume(on_request, queue='rpc_queue')
    print(" [x] Servidor RPC iniciado")
    channel.start_consuming()

# Iniciar el hilo del servidor RPC
threading.Thread(target=iniciar_servidor_rpc, daemon=True).start()

# ========== Cliente RPC ==========
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
        threading.Thread(target=self.channel.start_consuming, daemon=True).start()

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id

rpc_client = RpcClient('127.0.0.1', 'guest', 'guest', 'rpc_queue')

# ========== Flask ==========
app = Flask(__name__)

@app.route('/')
def index():
    return "Aplicación RPC funcionando."

@app.route('/caso1')
def caso1():
    corr_id = rpc_client.send_request("Caso 1")
    while rpc_client.queue[corr_id] is None:
        sleep(0.1)
    return f"Respuesta del servidor: {rpc_client.queue[corr_id]}"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
