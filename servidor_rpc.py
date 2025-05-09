import amqpstorm
from amqpstorm import Message

def on_request(message: Message):
    body = message.body
    print(f"[x] Recibido: {body}")

    # Aquí generamos la respuesta, por ejemplo, simplemente reflejando el mensaje recibido
    response = f"Recibido tu mensaje: {body}"

    # Enviar la respuesta manualmente usando la reply_to y correlation_id
    message.channel.basic.publish(
        body=response,
        routing_key=message.reply_to,
        properties={
            'correlation_id': message.correlation_id
        }
    )

    # Acknowledge (confirmar) el mensaje recibido
    message.ack()

# Conexión y consumo
connection = amqpstorm.Connection('127.0.0.1', 'guest', 'guest')
channel = connection.channel()
channel.queue.declare(queue='rpc_queue')

channel.basic.consume(on_request, queue='rpc_queue', no_ack=False)

print(" [x] Esperando solicitudes RPC")
channel.start_consuming()