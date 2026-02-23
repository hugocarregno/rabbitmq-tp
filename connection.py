import pika
import ssl
import os
from dotenv import load_dotenv

load_dotenv()

def get_channel():
    url = os.environ.get("CLOUDAMQP_URL")

    if url and "localhost" not in url:
        print("Conectando a CloudAMQP...")
        params = pika.URLParameters(url)
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        params.ssl_options = pika.SSLOptions(context)
    else:
        print("Conectando a RabbitMQ local...")
        params = pika.ConnectionParameters(
            host='localhost',
            credentials=pika.PlainCredentials('admin', 'admin')
        )

    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue='tareas', durable=True)
    return connection, channel