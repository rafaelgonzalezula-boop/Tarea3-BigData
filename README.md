# Tarea3-BigData
Procesamiento de Datos con Apache Spark y Kafka - Caso SECOP Contratos Electrónicos
# Solución de Procesamiento de Datos con Apache Spark y Kafka

**Caso de Estudio:** SECOP - Contratos Electrónicos (Datos Abiertos de Colombia)  
**Curso:** Big Data (UNAD)

---

## Solución

Este proyecto desarrolla una solución integral de Big Data enfocada en el análisis de contratos públicos de Colombia (**SECOP Contratos Electrónicos**). La arquitectura combina el procesamiento de datos masivos en lote (*batch*) y el procesamiento de eventos en tiempo real (*streaming*):

1. **Procesamiento en Lote (Batch):**
   - **Módulo:** `tarea3.py`
   - **Tecnología:** Apache Hadoop (HDFS) y PySpark (Spark DataFrames).
   - **Funcionalidad:** Carga el conjunto de datos masivo desde HDFS (`hdfs://localhost:9000/Tarea3/rows.csv`), realiza la limpieza de datos, analiza la estructura del esquema y ejecuta consultas analíticas y agregaciones estadísticas sobre los contratos registrados.

2. **Procesamiento en Tiempo Real (Streaming):**
   - **Módulos:** `kafka_producer.py` (Productor) y `spark_streaming_consumer.py` (Consumidor).
   - **Tecnología:** Apache Kafka y Spark Structured Streaming.
   - **Funcionalidad:** 
     - El **productor** simula la generación e ingesta continua de eventos de adjudicación de contratos en formato JSON hacia el *topic* de Kafka `secop_contratos`.
     - El **consumidor** procesa el flujo de mensajes recibidos de Kafka en ventanas temporales de 1 minuto, calculando métricas acumuladas en tiempo real (suma total del valor contratado y cantidad de contratos adjudicados por departamento).

---

## Requisitos del Sistema e Instalación

Para ejecutar esta solución se requiere la siguiente infraestructura configurada:
- **Sistema Operativo:** Ubuntu Linux (entorno virtualizado).
- **Hadoop:** Versión 3.3.x con HDFS activo.
- **Apache Spark:** Versión 3.5.x.
- **Apache Kafka:** Versión 3.x.
- **Python 3:** Con la librería `kafka-python` instalada.

---

## Instrucciones para la Ejecución

Sigue este paso a paso para ejecutar los componentes de la solución en la máquina virtual:

### Paso 1: Iniciar los servicios del Clúster (Hadoop y Kafka)
Abre la terminal e inicia los servicios requeridos:
- Iniciar HDFS y YARN: `start-all.sh`
- Iniciar servidor de ZooKeeper: `sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &`
- Iniciar servidor de Kafka: `sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &`

### Paso 2: Crear el Topic en Kafka
Crea el canal de transmisión de eventos ejecutando en la terminal:
- Comando de creación del topic: `/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic secop_contratos`

### Paso 3: Ejecutar el Procesamiento en Lote (Batch)
Para realizar el análisis estático desde HDFS con PySpark ejecuta en la terminal:
- Comando de ejecución batch: `python3 tarea3.py`

### Paso 4: Ejecutar el Procesamiento en Tiempo Real (Streaming)
1. En la **Terminal 1**, inicia el transmisor de eventos (Productor):
   - Comando del productor: `python3 kafka_producer.py`

2. En la **Terminal 2**, ejecuta el procesador de tiempo real (Consumidor de Spark):
   - Comando del consumidor: `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py`
