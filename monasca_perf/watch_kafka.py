import kafka

import json
import time

KAFKA_URL = 'localhost:9092'
KAFKA_GROUP = 'kafka_python_test'
KAFKA_TOPIC = 'metrics'

k_client = kafka.client.KafkaClient(KAFKA_URL)
c = kafka.consumer.SimpleConsumer(k_client,
                                  KAFKA_GROUP,
                                  KAFKA_TOPIC,
                                  auto_commit=False,
                                  fetch_size_bytes=1024 * 1024,
                                  buffer_size=1024 * 1024,
                                  max_buffer_size=None)

c.seek(0, 0)

runtime = 60

start_time = 0
end_time = 0

total_msgs = 0

hostnames = set()

by_tenant = {}

metrics_out = open("raw_metrics", "w")

for msg in c:
    total_msgs += 1

    metrics_out.write(msg.message.value)

    metric = json.loads(msg.message.value)

    tenant_id = metric['meta']['tenantId']
    name = metric['metric']['name']

    if tenant_id not in by_tenant:
        by_tenant[tenant_id] = {}

    if name not in by_tenant[tenant_id]:
        by_tenant[tenant_id][name] = 1
    else:
        by_tenant[tenant_id][name] += 1

    if start_time == 0:
        start_time = time.time()
    if time.time() - start_time > 60:
        end_time = time.time()
        break

metrics_out.close()

elapsed_time = end_time - start_time
print("messages per second: {}".format(total_msgs / elapsed_time))

for tenant, metrics in by_tenant.iteritems():
    tentant_metrics = 0
    print("tenant: {}".format(tenant))
    for k, v in metrics.iteritems():
        tentant_metrics += v
    print("total metrics: {}".format(tentant_metrics))
    print("metrics per second: {}".format(tentant_metrics / elapsed_time))

    for k, v in metrics.iteritems():
        print("  {} -> {}".format(k, v))
    print("\n")
