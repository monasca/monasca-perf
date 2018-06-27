import sys
from decimal import *
import csv
import argparse
import socket, time, datetime

from pykafka import KafkaClient

from kafka import KafkaProducer, KafkaConsumer
from kafka import SimpleClient, SimpleProducer, SimpleConsumer

import confluent_kafka

from abc import ABCMeta, abstractmethod

msg_count = 100000
kafka_host = "127.0.0.1:9092"
zookeeper_host = "127.0.0.1:2181"

def confluentAck(err, msg):

    if err is not None:
        print "Failed to deliver message: {}: {}".format(msg, err)
#   else:
#       print "Message produced: {}".format(msg)


class PythonClient:
    __metaclass__ = ABCMeta
    msg = "This is the message used to store into kafka" + str(time.time)
    msgCount = 0
    config = {"topic" : "", "kafkaHost": "", "zookeeperHost": "", "use_rdkafka": False, "kafkaSync": False, "min_queued_messages": 0, "num_msg": 0}
    beforeFlushTime = 0
    timeDict = {}

    def __init__(self):
        timeTpl = {'startTime': 0, 'flushTime': 0, 'stopTime': 0}
        self.timeDict["producer"] = timeTpl.copy()
        self.timeDict["consumer"] = timeTpl.copy()

    @abstractmethod
    def createProducer(self): pass

    @abstractmethod
    def createConsumer(self): pass

    @abstractmethod
    def produce(self): pass

    @abstractmethod
    def consume(self, num_msg): pass

    @abstractmethod
    def initCount(self): pass

    def startTimer(self, timer):
        timer['startTime'] =  time.time()

    def beforeFlushTimer(self, timer):
        timer['beforeFlushTime'] =  time.time()

    def stopTimer(self, timer):
        timer['stopTime'] =  time.time()

    def printTime(self, results):
        usedTime = 0
        timeTillFlush = 0

        timerPtr = self.timeDict['producer']
        if timerPtr['startTime'] > 0:
            # print producer time
            usedTime = timerPtr['stopTime'] - timerPtr['startTime']
            if timerPtr['beforeFlushTime'] > 0:
                timeTillFlush = timerPtr['beforeFlushTime'] - timerPtr['startTime']

        print "    Produce {} messages took {} seconds".format(self.msgCount, usedTime)
        results["duration_till_flush"] = str(round(timeTillFlush, 3)).ljust(6)
        results["duration"] = str(round(usedTime, 3)).ljust(6)
        results["msg_per_sec"] = round(self.msgCount/float(usedTime), 1)

        timerPtr = self.timeDict['consumer']
        if timerPtr['startTime'] > 0:
            # print producer time
            usedTime = timerPtr['stopTime'] - timerPtr['startTime']

        print "    Consume {} messages took {} seconds".format(self.msgCount, usedTime)
        results["duration_cons"] = str(round(usedTime, 3)).ljust(6)
        if usedTime > 0:
            results["msg_per_sec_cons"] = round(self.msgCount/float(usedTime), 1)
        else:
            results["msg_per_sec_cons"] = 0

    def prtProgress(self, count, prtProgress=10000):
       if (count>0) and (count % prtProgress == 0):
           if (count == prtProgress):
               sys.stdout.write('    ')
           sys.stdout.write('.')
           sys.stdout.flush()


class KafkaPythonClient(PythonClient):
    def __init__(self,topic="tst-pykafka",kafkaHost = kafka_host, zookeeperHost=zookeeper_host):
        self.config["topic"] = topic
        self.config["kafkaHost"] = kafkaHost
        self.config["zookeeperHost"] = zookeeperHost
        super(KafkaPythonClient, self).__init__()

    def createProducer(self, kafkaSync):
        self.config["kafkaSync"] = kafkaSync
        self.producer = KafkaProducer(bootstrap_servers=self.config["kafkaHost"])

    def createConsumer(self):
        self.consumer = KafkaConsumer(bootstrap_servers=self.config["kafkaHost"], enable_auto_commit=True, auto_offset_reset='latest',consumer_timeout_ms=1000)
        self.consumer.subscribe([self.config["topic"]])

    def produce(self, num_msg=20000):
        self.msgCount = num_msg
        for x in range (self.msgCount):
            self.prtProgress(x, 10000)
            result = self.producer.send(self.config["topic"], self.msg)
            if self.config["kafkaSync"] == True:
                # block for "synchronous" mode:
                try:
                    result_metadata = result.get(timeout=10)
                except KafkaError:
                    print "*** KAFKA ERROR ***"
                    pass
        if (x >= 10000):
            sys.stdout.write('\n')

    def consume(self, num_msg):
        count = 0
        for message in self.consumer:
            count += 1
            self.prtProgress(count, 10000)
        sys.stdout.write('\n')
        if num_msg >  0:
            if count != num_msg:
                print "ERROR: KafkaPythonClient.consume: # of messages not as expected, read: {}, expected: {}".format(count, num_msg)
        return count

    def startProducer(self): pass

    def stopProducer(self):
        self.beforeFlushTimer(self.timeDict['producer'])
        if self.config["kafkaSync"] == False:
            self.producer.flush()

    def stopConsumer(self): pass

    def initCount(self):
        self.consume(0)
#       for p in self.consumer.partitions_for_topic(self.config['topic']):
#           tp = TopicPartition(self.config['topic'], p)
#           self.consumer.assign([tp])
#           committed = self.consumer.committed(tp)
#           consumer.seek_to_end(tp)

    def finalize(self): pass


class KafkaPythonClientSimple(PythonClient):
    def __init__(self,topic="tst-pykafka", consumerGroup="perftest", kafkaHost=kafka_host, zookeeperHost=zookeeper_host):
        self.config["topic"] = topic
        self.config["kafkaHost"] = kafkaHost
        self.config["zookeeperHost"] = zookeeperHost
        self.config["consumerGroup"] = consumerGroup
        self.client = SimpleClient(self.config["kafkaHost"])
        super(KafkaPythonClientSimple, self).__init__()

    def createProducer(self, kafkaSync):
        self.config["kafkaSync"] = kafkaSync
        if self.config["kafkaSync"] == True:
            self.producer = SimpleProducer(self.client, async=False)
        else:
            print "ENOIMPL: async not impl. for kafka-python-simple"

    def createConsumer(self):
        self.consumer = SimpleConsumer(self.client,
            topic=self.config["topic"],
            group=self.config["consumerGroup"],
            auto_commit= True,
            max_buffer_size=3000000,
            iter_timeout=5)

    def produce(self, num_msg=20000):
        self.msgCount = num_msg
        for x in range (self.msgCount):
            self.prtProgress(x, 10000)
            self.producer.send_messages(self.config["topic"], self.msg)
        if (x >= 10000):
            sys.stdout.write('\n')

    def consume(self, num_msg=0):
        count = 0
        while True:
            message=self.consumer.get_message(block=False, timeout=1)
#       don't use this construct "for message in self.consumer:" instead of "while..." - much slower!
            if message is None:
#               print "consume, msg is None"
                break
            if len(message) == 0:
#               print "consume, len(msg) is 0"
                break
            count += 1
            self.prtProgress(count, 10000)
        sys.stdout.write('\n')
        if num_msg >  0:
            if count != num_msg:
                print "ERROR: KafkaPythonClientSimple.consume: # of messages not as expected, read: {}, expected: {}".format(count, num_msg)
        return count

    def startProducer(self):
        pass

    def stopProducer(self):
        self.beforeFlushTimer(self.timeDict['producer'])
        self.producer.stop()

    def stopConsumer(self): pass

    def initCount(self):
        self.consume(0)

    def finalize(self): pass


class PyKafkaClient(PythonClient):
    def __init__(self,topic="tst-pykafka", consumerGroup="perftest", kafkaHost=kafka_host, zookeeperHost=zookeeper_host):
        self.config["topic"] = topic
        self.config["kafkaHost"] = kafkaHost
        self.config["zookeeperHost"] = zookeeperHost
        self.config["consumerGroup"] = consumerGroup
        self.client = KafkaClient(hosts=self.config["kafkaHost"])
        self.topic = self.client.topics[self.config["topic"]]
        super(PyKafkaClient, self).__init__()

    def createProducer(self, kafkaSync, use_rdkafka, min_queued_messages, msgSync):
        self.config["kafkaSync"] = kafkaSync
        if self.config["kafkaSync"] == True:
            self.producer = self.topic.get_sync_producer()
        else:
            self.producer = self.topic.get_producer(use_rdkafka=use_rdkafka, min_queued_messages=min_queued_messages, sync=msgSync)

    def createConsumer(self,consumerMode, use_rdkafka):
        # TBD: pass options as params

        if consumerMode == "simple":
            self.consumer = self.topic.get_simple_consumer(consumer_group=self.config["consumerGroup"],
                                auto_commit_enable = False,
                                use_rdkafka = use_rdkafka,
                                consumer_timeout_ms=1000)
        elif consumerMode == "balanced":
            self.consumer = self.topic.get_balanced_consumer(
                                consumer_group=self.config["consumerGroup"],
                                auto_commit_enable=True,
                                zookeeper_connect=self.config['zookeeperHost'],
                                use_rdkafka = use_rdkafka,
                                consumer_timeout_ms=1000)
        else:
            print "ENOIMPL: createConsumer:invalid consumer mode {}".format(consumerMode)

    def produce(self, num_msg=20000):
        self.msgCount = num_msg
        for x in range (self.msgCount):
            self.prtProgress(x, 10000)
            self.producer.produce(self.msg)
        if (x >= 10000):
            sys.stdout.write('\n')

    def consume(self, num_msg):
        count = 0
        for message in self.consumer:
            count += 1
            self.prtProgress(count, 10000)
        self.consumer.commit_offsets()
        sys.stdout.write('\n')
        if num_msg >  0:
            if count != num_msg:
                print "ERROR: PyKafkaClient.consume: # of messages not as expected, read: {}, expected: {}".format(count, num_msg)
        return count

    def startProducer(self):
        self.producer.start()

    def stopProducer(self):
        self.beforeFlushTimer(self.timeDict['producer'])
        self.producer.stop()

    def stopConsumer(self):
        self.consumer.stop()

    def initCount(self):
        self.start_offs = self.topic.latest_available_offsets()
        self.consumer.commit_offsets()

    def finalize(self):
        self.consumer.commit_offsets()
        self.consumer.stop()


class ConfluentClient(PythonClient):
    def __init__(self,topic="tst-pykafka",kafkaHost = kafka_host, zookeeperHost=zookeeper_host):
        self.config["topic"] = topic
        self.config["kafkaHost"] = kafkaHost
        self.config["zookeeperHost"] = zookeeperHost
        super(ConfluentClient, self).__init__()

    def createProducer(self, kafkaSync):
        self.config["kafkaSync"] = kafkaSync
        config = {'bootstrap.servers': self.config["kafkaHost"],
                  'client.id': socket.gethostname(),
                  'default.topic.config': {'acks': 'all'}}

        self.producer = confluent_kafka.Producer(config)


    def produce(self, num_msg=20000):
        self.msgCount = num_msg
        for x in range (self.msgCount):
            self.prtProgress(x, 10000)
            if self.config["kafkaSync"] == False:
                self.producer.produce(self.config["topic"], value=self.msg)
            elif self.config["kafkaSync"] == True:
                self.producer.produce(self.config["topic"], value=self.msg, callback=confluentAck)

                # Wait up to 1 second for events. Callbacks will be invoked during
                # this method call if the message is acknowledged.
                self.producer.poll(1)
            else:
                print "invalid value for kafkaSync: {}".format(self.config["kafkaSync"])

        if (x >= 10000):
            sys.stdout.write('\n')

    def consume(self, num_msg):
        count = 0
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() != confluent_kafka.KafkaError._PARTITION_EOF:
                    print "   Error: {}".format(msg.error())
                break
            else:
                count += 1
                self.prtProgress(count, 10000)
        sys.stdout.write('\n')

        if num_msg >  0:
            if count != num_msg:
                print "ERROR: ConfluentClient.consume: # of messages not as expected, read: {}, expected: {}".format(count, num_msg)

        return count

    def startProducer(self): pass

    def stopProducer(self):
        self.beforeFlushTimer(self.timeDict['producer'])
        self.producer.flush(30)

    def createConsumer(self):

        self.consumerConfig = {'bootstrap.servers': self.config["kafkaHost"],
                  'client.id': socket.gethostname(),
                  'default.topic.config': {'acks': 'all'},
                  'enable.auto.commit': True,
                  'auto.commit.interval.ms':1000,
                  'group.id': "perftest",
#                 'api.version.request': False,
#                 'broker.version.fallback': '0.9.0.1',
                  'default.topic.config':
                  {
#                     'auto.offset.reset': 'smallest'
#                     'auto.offset.reset': 'earliest'
                      'auto.offset.reset': 'latest'
                  }
        }

        self.consumer = confluent_kafka.Consumer(self.consumerConfig)
        self.consumer.subscribe([self.config["topic"]])

    def initCount(self):

        # Since confluent as of now doesn't provide an API to read offsets for topics/partitions: read msges of topic
        self.consume(0)

    def stopConsumer(self): pass
    def finalize(self): pass

def producerPerfTest(results, api="pykafka", topic = "tst-pykafka",  num_msg = 20000, kafkaSync = False,
    useCLib = True, kafkaHost = kafka_host, zookeeperHost=zookeeper_host, msgLen = 100,
    min_queued_messages = 5000, sync=False, consumerMode = ""):

    if api == "pykafka":
        strConfig = "sync(API): {}, C-lib: {}, min_queued_msg: {}, cons.mode: {}, sync(async+wait): {}".format(kafkaSync, useCLib, min_queued_messages,consumerMode, sync)
        testObj = PyKafkaClient(topic=topic,kafkaHost=kafkaHost, zookeeperHost=zookeeperHost)
        testObj.createProducer(kafkaSync=kafkaSync, use_rdkafka=useCLib,min_queued_messages=min_queued_messages, msgSync=sync)
        testObj.createConsumer(consumerMode, use_rdkafka=useCLib)
    elif api == "kafka-python":
        strConfig = "sync: {}".format(kafkaSync)
        testObj = KafkaPythonClient(topic=topic,kafkaHost=kafkaHost, zookeeperHost=zookeeperHost)
        testObj.createProducer(kafkaSync=kafkaSync)
        testObj.createConsumer()
    elif api == "kafka-python-simple":
        strConfig = "sync: {}".format(kafkaSync)
        testObj = KafkaPythonClientSimple(topic=topic,kafkaHost=kafkaHost, zookeeperHost=zookeeperHost)
        testObj.createProducer(kafkaSync=kafkaSync)
        testObj.createConsumer()
    elif api == "confluent":
        strConfig = "sync: {}".format(kafkaSync)
        testObj = ConfluentClient(topic=topic,kafkaHost=kafkaHost, zookeeperHost=zookeeperHost)
        testObj.createProducer(kafkaSync=kafkaSync)
        testObj.createConsumer()
    else:
        print "ENOIMPL: api {} not implemented".format(api)

    results["api"] = api.ljust(20)
    results["num_messages"] = str(num_msg).ljust(8)
    results["config"] = strConfig.ljust(78)

    # ensure correct offsets/partitions
    print "    Skip old entries"
    num_skipped = testObj.consume(0)
    print "    skipped entries: {}".format(num_skipped)

    print "    Warm up"
    # warm up before measurement
    testObj.produce(num_msg=1000)
    testObj.stopProducer()
    # consume warm up messages
    testObj.consume(1000)

    print "    Start Test"
    testObj.startProducer()
    testObj.startTimer(testObj.timeDict['producer'])
    testObj.produce(num_msg=num_msg)
    testObj.stopProducer()
    testObj.stopTimer(testObj.timeDict['producer'])
    print "    Producer Test completed"

    # sleep 30 seconds to ensure that system doesn't have any more activities before starting consumer test
    time.sleep(30)

    startTime = time.time()
    testObj.startTimer(testObj.timeDict['consumer'])
    testObj.consume(num_msg)
    testObj.stopTimer(testObj.timeDict['consumer'])
    stopTime = time.time()
    testObj.stopConsumer()
    print "    Consumer Test completed"
    # print "    Consumption of {} messages took {} seconds".format(num_msg, (stopTime-startTime))
    testObj.printTime(results)

    # final clean-up
    testObj.finalize()

def main():
    resultArr = []
    topic = "tst-pykafka"
    kafkaHost = "127.0.0.1:9092"
    zookeeperHost = "127.0.0.1:2181"
    min_queued_messages = 1000
    msgLen = 100

    confluent = False
    pykafka = False
    kafka_python = False
    kafka_python_simple = False

    # read parameters if any:
    parser = argparse.ArgumentParser(description="Performance test for sev. kafka clients in Python")
    parser.add_argument("--kafkaCli", type=str, choices=["all","confluent", "pykafka", "kafka_python", "kafka_python_simple"])
    parser.add_argument("--kafkaVer", type=str)
    args = parser.parse_args()
    if args.kafkaCli == "confluent":
           confluent = True
    elif args.kafkaCli == "pykafka":
           pykafka = True
    elif args.kafkaCli == "kafka_python":
           kafka_python = True
    elif args.kafkaCli == "kafka_python_simple":
           kafka_python_simple = True
    else:
        # default: execute all
        confluent = True
        pykafka = True
        kafka_python = True
        kafka_python_simple = True

    kafka_old = True
    kafka_new = True
    if args.kafkaVer is not None:
       errTxt=""
       verArr = args.kafkaVer.split(".")
       if len(verArr) < 2:
          errTxt = "kafka version not well defined: {}".format(args.kafkaVer)
       elif verArr[0] != "0" and verArr[0] != "1":
          errTxt = "invalid major kafka version: {}".format(verArr[0])
       elif int(verArr[1]) < 0 or int(verArr[1]) > 99:
          errTxt = "invalid minor kafka version: {}".format(verArr[1])
       if len(errTxt) > 0:
          print errTxt
          exit(1)
       if verArr[0] == "0" and int(verArr[1]) <= 9:
           kafka_new = False
       else:
           kafka_old = False
#   else:
#      print "kafkaVer is None"

    if kafka_new == False and  confluent == True:
       print "confluent client testing disabled: works only with newer kafka versions (>=0.10)"
       confluent = False
    if kafka_old == False and  kafka_python_simple == True:
             # explicitly requested: display warning and continue
             print "WARNING: kafka_python simple client with newer kafka versions (>=0.10) requires protocol setting in server.properties (kafka): "
             print "         log.message.format=0.9. Otherwise, sporadic errors will occur, test will be aborted"

    resultEle = {"api" : "", "config": "", "num_messages": "", "duration_till_flush": 0,"duration": 0, "msg_per_sec": 0, "duration_cons": 0, "msg_per_sec_cons": 0}

    if confluent == True:
        print "### 0.1: confluent, async, cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="confluent", topic=topic, num_msg=20000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="confluent", topic=topic, num_msg=100000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages)
        print "### 0.2: confluent, sync, cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="confluent", topic=topic, num_msg=20000, kafkaSync=True, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="confluent", topic=topic, num_msg=100000, kafkaSync=True, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages)

    if pykafka == True:
        print "### pykafka, balanced consumer mode ###"

        consumerMode="balanced"
        print "### 1.1.: pykafka, async, cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=20000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=100000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)

        print "### pykafka, simple consumer mode ###"
        consumerMode="simple"
        print "### 1.2: async, cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=20000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=100000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)

        print "### 1.3.: pykafka, async, cLib, 1 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=20000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=1, consumerMode=consumerMode)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=100000, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=1, consumerMode=consumerMode)


        print "### 1.4.: pykafka, async, without cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=20000, kafkaSync=False, useCLib=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=100000, kafkaSync=False, useCLib=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)

        print "### 1.5.: pykafka, async, without cLib, 1000 queued messages, sync=True"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=2000, kafkaSync=False, useCLib=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, sync=True, consumerMode=consumerMode)

        print "### 1.6: pykafka, async, with cLib, 1000 queued messages, sync=True"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=500, kafkaSync=False, useCLib=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, sync=True, consumerMode=consumerMode)

        print "### 1.7.: pykafka, sync, no cLib, 1000 queued messages"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="pykafka", topic=topic, num_msg=2000, kafkaSync=True, useCLib=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen, min_queued_messages=min_queued_messages, consumerMode=consumerMode)

    if kafka_python == True:
        print "### 2.1.: kafka-python new, async"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python", topic=topic, num_msg=20000, kafkaSync=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python", topic=topic, num_msg=100000, kafkaSync=False,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen)

        print "### 2.2.: kafka-python new, sync"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python", topic=topic, num_msg=20000, kafkaSync=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen)
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python", topic=topic, num_msg=100000, kafkaSync=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen)

    if kafka_python_simple == True:
        print"### 2.3.: kafka-python old (simple), sync"
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python-simple", topic=topic, num_msg=20000, kafkaSync=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen )
        resultArr.append(resultEle.copy())
        producerPerfTest(resultArr[-1], api="kafka-python-simple", topic=topic, num_msg=100000, kafkaSync=True,
            kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen )

    fn = "results/kafka"
    if args.kafkaVer is not None:
        fn += args.kafkaVer
    fn += "PerfResults" + datetime.datetime.today().strftime('%Y-%m-%d-%H:%M:%s') + ".csv"
    with open(fn, 'w') as csvfile:
        fieldnames = {'api', 'config', 'num_messages', 'duration_till_flush', 'duration', 'msg_per_sec', 'duration_cons', 'msg_per_sec_cons'}
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultArr)

        for result in resultArr:
            print "API:{} CONF: {} # MSG:{} TIME TO FLUSH:{} TOTAL TIME:{} MSG/S: {} CONS_TIME:{} CONS_MSG/S:{}".format(result["api"], result["config"], result["num_messages"],
                result["duration_till_flush"], result["duration"], result["msg_per_sec"], result["duration_cons"], result["msg_per_sec_cons"])

main()
