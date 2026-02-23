# 🐇 RabbitMQ — Sistemas Paralelos y Distribuidos

Implementación de una arquitectura de mensajería asíncrona con RabbitMQ, Docker y Python.

---

## 📁 Estructura del proyecto

```
rabbitmq-tp/
├── .env                  # Variables de entorno (no subir al repo)
├── .gitignore
├── docker-compose.yml    # Solo RabbitMQ
├── connection.py         # Lógica de conexión compartida
├── producer.py           # Envía mensajes a la cola
├── consumer.py           # Procesa mensajes de la cola
└── README.md
```

---

## ✅ Requisitos previos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) instalado y corriendo
- [Python 3.11+](https://www.python.org/downloads/) instalado
- Pip funcionando (`python -m pip --version`)

---

## ⚙️ Instalación

### 1. Clonar o descargar el proyecto

```cmd
cd Documents
git clone <url-del-repo>
cd rabbitmq-tp
```

### 2. Instalar dependencias de Python

```cmd
python -m pip install pika python-dotenv
```

### 3. Crear el archivo `.env`

Crear un archivo llamado `.env` en la carpeta del proyecto (sin extensión `.txt`).

**Para usar RabbitMQ local con Docker:**
```
CLOUDAMQP_URL=amqp://admin:admin@localhost/
```

**Para usar CloudAMQP (nube):**
```
CLOUDAMQP_URL=amqps://usuario:password@leopard.lmq.cloudamqp.com/usuario
```

> ⚠️ En Windows, crear el archivo desde la terminal para evitar que se guarde como `.env.txt`:
> ```cmd
> echo CLOUDAMQP_URL=amqp://admin:admin@localhost/ > .env
> ```

---

## 🚀 Ejecución

### Paso 1 — Levantar RabbitMQ con Docker

```cmd
docker-compose up -d
```

Verificar que el contenedor está corriendo:
```cmd
docker ps
```

El panel de administración queda disponible en:
```
http://localhost:15672
Usuario: admin
Password: admin
```

---

### Paso 2 — Levantar un consumidor

Abrir una terminal en la carpeta del proyecto:

```cmd
python consumer.py
```

Debe mostrar:
```
Conectando a RabbitMQ local...
Esperando mensajes...
```

---

### Paso 3 — Ejecutar el productor

Abrir **otra terminal** en la carpeta del proyecto:

```cmd
python producer.py
```

El consumidor del Paso 2 empezará a procesar los mensajes automáticamente.

---

## 🔬 Pruebas

### Prueba 1 — Envío y recepción básica

1 productor + 1 consumidor. Verificar que los 10 mensajes son procesados.

```cmd
python consumer.py   # terminal 1
python producer.py   # terminal 2
```

---

### Prueba 2 — Distribución entre múltiples consumidores

Abrir 3 terminales con consumidores antes de ejecutar el productor:

```cmd
python consumer.py   # terminal 1
python consumer.py   # terminal 2
python consumer.py   # terminal 3
python producer.py   # terminal 4
```

Observar en `http://localhost:15672` cómo los mensajes se distribuyen en round-robin.

---

### Prueba 3 — Reentrega de mensajes sin ACK

1. Levantar un consumidor
2. Ejecutar el productor
3. Mientras el consumidor procesa, cerrarlo con `Ctrl+C`
4. Verificar en el panel que los mensajes vuelven a la cola
5. Levantar un nuevo consumidor y confirmar que retoma los mensajes

---

### Prueba 4 — Persistencia tras reinicio del broker

1. Ejecutar el productor sin ningún consumidor activo
2. Verificar en el panel que los mensajes están en cola
3. Reiniciar RabbitMQ:
```cmd
docker-compose down
docker-compose up -d
```
4. Levantar un consumidor y verificar que los mensajes siguen ahí

---

## 🌐 Cambiar entre local y CloudAMQP

Solo modificar el archivo `.env` sin tocar el código:

**Local:**
```
CLOUDAMQP_URL=amqp://admin:admin@localhost/
```

**Nube:**
```
CLOUDAMQP_URL=amqps://usuario:password@leopard.lmq.cloudamqp.com/usuario
```

---

## 🔄 Comandos útiles de Docker

```cmd
# Levantar RabbitMQ
docker-compose up -d

# Ver logs de RabbitMQ
docker-compose logs -f rabbitmq

# Detener y borrar contenedor (conserva mensajes)
docker-compose down

# Detener y borrar todo incluyendo mensajes guardados
docker-compose down -v

# Reinicio completo desde cero
docker-compose down -v
docker-compose up -d
```

---

## 🛑 Problemas comunes

**`pip` no se reconoce como comando**
```cmd
python -m pip install pika python-dotenv
```

**El `.env` se guardó como `.env.txt`**
```cmd
rename .env.txt .env
```

**Error de conexión SSL con CloudAMQP**
Verificar que la URL en el `.env` no tiene comillas y empieza con `amqps://`.

**Los mensajes no llegan al consumidor**
Verificar que RabbitMQ está corriendo con `docker ps` y que el Management UI responde en `http://localhost:15672`.

---

## 👥 Integrantes

- Barroso, Gonzalo
- Carreño, Hugo Ezequiel
- Velasco, Benjamin
