import json, time, sys, os
from connection import get_channel

def main():
    connection, channel = get_channel()

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