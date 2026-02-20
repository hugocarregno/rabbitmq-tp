import pika
import json
import random
from datetime import datetime

# Conexión a RabbitMQ
credentials = pika.PlainCredentials('admin', 'admin')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost',credentials=credentials))
channel = connection.channel()

# Declarar cola durable
channel.queue_declare(queue='tareas', durable=True)

# Enviar 10 mensajes
for i in range(1, 11):
    mensaje = {
        "id": i,
        "descripcion": f"Tarea número {i}",
        "fecha": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "dificultad": random.randint(1, 3)
    }

    channel.basic_publish(
        exchange='',
        routing_key='tareas',
        body=json.dumps(mensaje),
        properties=pika.BasicProperties(
            delivery_mode=2  # Mensaje persistente
        )
    )

    print(f"Enviado: {mensaje}")

connection.close()
