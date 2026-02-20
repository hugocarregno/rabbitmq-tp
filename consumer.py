import pika
import json
import time
import sys
import os

credentials = pika.PlainCredentials('admin', 'admin')

def main():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host='localhost',
            credentials=credentials
        )
    )

    channel = connection.channel()
    channel.queue_declare(queue='tareas', durable=True)

    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        try:
            mensaje = json.loads(body)
            print(f"Procesando: {mensaje}")
            
            dificultad = mensaje.get('dificultad', 1)
            time.sleep(dificultad)
            
            print(f"Finalizado: {mensaje['id']}")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            print(f"Error procesando la tarea: {e}")
            # Si hay error, rechazamos negativamente y no volvemos a encolar
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    channel.basic_consume(
        queue='tareas',
        on_message_callback=callback
    )

    print("Esperando mensajes...")
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\nInterrumpido por el usuario')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)