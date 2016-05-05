import kafka

import time
# import json

KAFKA_URL = 'localhost:9092'
KAFKA_GROUP = 'kafka_python_test'
KAFKA_TOPIC = 'metrics'

k_client = kafka.client.KafkaClient(KAFKA_URL)
c = kafka.consumer.SimpleConsumer(k_client,
                                  KAFKA_GROUP,
                                  KAFKA_TOPIC,
                                  auto_commit=False,
                                  fetch_size_bytes=1024*1024,
                                  buffer_size=1024*1024,
                                  max_buffer_size=None)

runtime = 60

start_time = 0
end_time = 0

total_msgs = 0

for msg in c:
    if start_time == 0:
        start_time = time.time()
    if time.time() - start_time > 60:
        end_time = time.time()
        break
    total_msgs += 1

elapsed_time = end_time - start_time
print("messages per second: {}".format(total_msgs/elapsed_time))
