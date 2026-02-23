import pika, json, random
from datetime import datetime

from connection import get_channel

connection, channel = get_channel()

# Enviar 10 mensajes
for i in range(1, 11):
    mensaje = {
        "id": i,
        "descripcion": f"Tarea número {i} - " + "x" * 500,
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
