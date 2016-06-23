import kafka

print("kafka-python version: {}".format(kafka.__version__))

if kafka.__version__ == "0.9.5":
    from kafka.client import KafkaClient
    from kafka.consumer import SimpleConsumer
    from kafka.producer import KeyedProducer
else:
    from kafka.client import SimpleClient as KafkaClient
    from kafka.consumer import SimpleConsumer
    from kafka.producer import KeyedProducer

import time
import json

KAFKA_URL = '192.168.10.6:9092'
KAFKA_GROUP = 'kafka_python_perf'
KAFKA_TOPIC = 'raw-events'

NUM_MESSAGES = 100000
SIZE_MSG = 369


def write():
    k_client = KafkaClient(KAFKA_URL)
    p = KeyedProducer(k_client,
                      async=False,
                      req_acks=KeyedProducer.ACK_AFTER_LOCAL_WRITE,
                      ack_timeout=2000)
    messages = []
    for i in xrange(NUM_MESSAGES):
        message = json.dumps({'msg': 'X' * SIZE_MSG})
        messages.append(message)
        if len(messages) >= 500:
            key = int(time.time() * 1000)
            p.send_messages(KAFKA_TOPIC, str(key), *messages)
            messages = []
    key = int(time.time() * 1000)
    p.send_messages(KAFKA_TOPIC, str(key), *messages)


def read(fetch_size):
    k_client = KafkaClient(KAFKA_URL)
    c = SimpleConsumer(k_client,
                       KAFKA_GROUP,
                       KAFKA_TOPIC,
                       auto_commit=False,
                       fetch_size_bytes=fetch_size,
                       buffer_size=fetch_size,
                       max_buffer_size=None)
    start_time = None
    start_time = time.time()

    i = 0
    for msg in c:
        i = i + 1
        if i >= NUM_MESSAGES:
            break

    end_time = time.time()
    elapsed_time = end_time - start_time
    num_messages_per_sec = NUM_MESSAGES / elapsed_time

    print("fetch size {} -> {} messages per second"
          .format(fetch_size, num_messages_per_sec))

write()
for fetch_size in xrange((1024 * 50), (1024 * 1024 * 2), (1024 * 25)):
    read(fetch_size)
