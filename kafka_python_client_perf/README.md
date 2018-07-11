# Investigation results: comparison of throughput of different kafka python clients


kafka_python_client_perf
========================

monascaInvestigationKafkaClientAPIs.md:
--------------------------------------

The document describes the purpose of the tests, the test targets and finally the test results.

media:
-----

Images, referenced from MD document

kafka-py-client-test.py:
-----------------------

The test program used to measure the throughput with different python kafka clients.

# Preconditions

* Kafka/zookeeper installed
  The program has been tested with kafka 0.9 and kafka 1.1. A subset of tests has been executed with kafka 1.0

* Topic tst-pykafka has been created.
  Unfortunately, a topic cannot be created with the API.
  Pls. create the topic manually:
  **kafka-topics.sh --zookeeper <ip-adress>:2181 --topic tst-pykafka --partitions 1 --replaction-factor 1 --create**

* If kafka_python_simple client shall be tested with a later kafka-version (>=1.0):
Pls. set the following property:
log.message.format=0.9
This ensures that the old protocol (uncompressed) will be used.
Otherwise, errors will occur, and the test will finally fail and abort.

# Execute tests
  The program has the following parameters:
  * kafkaVer: The version that shall be tested.
    Purpose of this value is to determine if a specific kafka-client can be tested with this version.
  * kafkaCli: Specifies the kafka-client that shall be tested.
    Possible options:
    * all: Tests will be executed for all kafka-cients specified below.
    Restriction: kafka-versions needs to be compatible with the kafka-Version installed
    * confluent: will be executed only with kafka version 1.x
    * kafka_python
    * kafka_python_simple
    * pykafka
  * help: displays a help text about the options described above
  * The program assumes that kafka and zookeeper can be accessed on 127.0.0.1:9092 resp. 127.0.0.1:2181. If this doesn't apply in your case, the program needs to be adopted.

  _Note: In a docker-environment, the easiest way to achieve this to expose the default ports for both containers:
  In the relevant docker-compose file (e.g. docker-compose.yml), add the following lines to kafka resp. zookeeper service:_

    ports:  
      \- "host-port:container-port",  
    e.g. for zookeeper:
        "2181:2181"


# Program description

Basically, the program executes the following steps for the clients specified:

* Consume any existing messages for the topic
* Warm-up-phase: Produce and consume a small number of messages
* Execute a test for a smaller number of messages (usually 20,000): produce messages, then consume them
* Execute a test for a higher number of messages (usually 100,000)
* Depending on the client, tests have been executed multiple times with different parameter settings: synchronous <-> asynchronous, usage of a C-library <-> execution without C-library. Note: Many other parameters haven't been tested at all.
