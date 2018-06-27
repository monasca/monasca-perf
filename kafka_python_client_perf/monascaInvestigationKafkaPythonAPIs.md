**Investigation: Performance of different python-API’s for Kafka**

Last update: 2018/06/26

Created: 2018/05/22

Version: 1.4

Author: M. Bandorf

**Change Log**

<table>
<thead>
<tr class="header">
<th><strong>Date</strong></th>
<th><strong>Who</strong></th>
<th><strong>Ver.</strong></th>
<th><strong>Chpt.</strong></th>
<th><strong>What </strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>2018/06/26</td>
<td>M. Bandorf</td>
<td>1.4</td>
<td>All</td>
<td><p>Conversion to MD:</p>
<ul>
<li><p>Minor changes</p></li>
<li><p>Replace xls-based charts by images</p></li>
</ul></td>
</tr>
<tr class="even">
<td>2018/06/21</td>
<td>M. Bandorf</td>
<td>1.3</td>
<td><p>All</p>
<p>2,3</p></td>
<td><ul>
<li><p>Correction of spelling errors</p></li>
<li><p>Several minor changes</p></li>
<li><p>Better throughput f. kafka-python, simple consumer</p></li>
</ul></td>
</tr>
<tr class="odd">
<td>2018/06/21</td>
<td>M. Bandorf</td>
<td>1.2</td>
<td></td>
<td>Distributed for review</td>
</tr>
<tr class="even">
<td>2018/06/20</td>
<td>M. Bandorf</td>
<td>1.2</td>
<td><p>1.1,</p>
<p>2.1,</p>
<p>2.2.3</p>
<p>2.4</p></td>
<td><p>Remove restriction f. kafka-python simple consumer</p>
<ul>
<li><p>Added values f. kafka-python simple consumer</p></li>
<li><p>Res. issue w. kafka-python simple cons. and kafka 1.1 srv</p></li>
</ul>
<p>Reworked</p></td>
</tr>
<tr class="odd">
<td>2018/06/20</td>
<td>M. Bandorf</td>
<td>1.1</td>
<td></td>
<td>Distributed for review</td>
</tr>
<tr class="even">
<td>2018/06/19</td>
<td>M. Bandorf</td>
<td>1.1</td>
<td>All</td>
<td><p>Input from review by W.Bedyk:</p>
<ul>
<li><p>2.2: restructure test results</p></li>
<li><p>2.4: Adjust tables to chapter 2.2</p></li>
<li><p>2.7 (open issues): delete</p></li>
</ul></td>
</tr>
</tbody>
</table>

**  
**

Table of Contents
=================

[1 Introduction 4](#introduction)

[1.1 Motivation 4](#motivation)

[1.2 Basic approach 5](#basic-approach)

[1.3 API’s covered 5](#apis-covered)

[1.4 Test Targets 6](#test-targets)

[1.5 Test environment 6](#test-environment)

[2 Test Results 7](#test-results)

[2.1 Python clients overview 7](#python-clients-overview)

[2.2 Test results for kafka 1.1 8](#test-results-for-kafka-1.1)

[2.2.1 Producer sync. 8](#producer-sync.)

[2.2.2 Producer async. 9](#producer-async.)

[2.2.3 Consumer 10](#consumer)

[2.3 Test results for kafka 0.9 11](#test-results-for-kafka-0.9)

[2.4 Quick overview of results (kafka 1.1 only) 13](#quick-overview-of-results-kafka-1.1-only)

[2.5 Comparison of kafka 0.9 &lt;-&gt; 1.1 13](#comparison-of-kafka-0.9---1.1)

[2.6 Reference to other performance comparisons 13](#reference-to-other-performance-comparisons)

[3 Proposal 16](#proposal)

[4 Next steps 17](#next-steps)

[5 Appendix: Code Snippets 18](#appendix-code-snippets)

[5.1 Example: Confluent client 18](#example-confluent-client)

Introduction
============

Motivation
----------

Kafka out-of-the-box doesn’t provide a Python client.  
However, there are several Python clients available.

As of now, monasca is using “kafka-python” as Python-API to access kafka.  
The so-called “simple client” is used in synchronous mode.

There are mainly 2 reasons to think about a replacement of this implementation:

-   Simple client is already deprecated as of today (since v1.0.0 from 2016, Feb 15), thus:

    -   Support for this functionality is not ensured

    -   Functionality may be dropped in the near future:  
        According to kafka-python, simple client won’t be supported any longer beginning with kafka 2.x.

-   Python implementation of monasca-persister performs poorly, compared to Java implementation. Previous investigations have shown that simple-client kafka consumer is a major bottleneck.

Basic approach
--------------

In order to get a good overview about performance of different kafka python clients, the following steps have been done:

-   Select a set of kafka python clients to be analyzed

-   Write a simple python program that measures performance of producer and consumer clients:

    -   Execute some warm-up

    -   Write a number of messages to kafka

    -   Ensure that all messages have been written to kafka

    -   Measure the time

    -   Consume the same set of messages and measure the time again

-   All tests have been executed multiple times to avoid any outliers

-   The focus in testing was testing with kafka 1.1 server. As a reference, some tests have been executed as well with kafka 0.9.

-   Not all possible options of the different clients have been tested.  
    If available, the following options have been used:

    -   Synchronous &lt;-&gt; asynchronous

    -   Usage of C-lib &lt;-&gt; usage without C-library

API’s covered
-------------

The following clients have been selected for further testing:

-   Pykafka (version: 2.7)

    -   Synchronous/asynchronous:

        -   Supports synchronous mode on API-level

        -   Supports “async + wait”

    -   Native Python client, i.e. does not try to simulate Java client API

    -   Optional: use C-library (librdkafka)

-   Kafka-python (version: 1.4.2)

    -   As close as possible to Java client API

    -   Synchronous/asynchronous:

        -   Supports “async + wait”

-   Confluent (Version: 3.1 )

    -   Python wrapper around C-library librdkafka (same author)

    -   Synchronous/asynchronous:

        -   Supports “async + wait”

-   Reference: kafka-python simple-client (Version: 1.4.2, API is DEPRECATED)

Test Targets
------------

-   Execution of a defined test set for several Python kafka clients – producer and consumer

-   Execution on a single machine

-   Not covered (lack of time & resources):

    -   Scalability tests

    -   Reliability tests

    -   Fine tuning of the different clients (configuration). This was out-of-scope for this analysis.  
        Needs to be done in next steps.

Test environment
----------------

All tests have been executed on a local Linux machine:

-   Ubuntu 16.04

-   Hardware:

    -   8-core

    -   Intel i7

    -   32 GB RAM

    -   256 GB SSD

-   Python test program running on bare-metal

-   Kafka/zookeeper running in Docker container

Test Results
============

Since a kafka migration will be done anyway, tests have been executed with kafka 1.1.

As a reference, some tests have been executed with kafka 0.9.

In this chapter, test results are shown for sync. and async. Clients:

-   The charts show the average of 5 series of measurements, done for producer and consumer of several kafka clients.

-   Default number of messages for producer and consumer if not mentioned differently: 100,000

Python clients overview
-----------------------

| Client                | Kafka 0.9 | Kafka 1.1 | remarks                                                                     |
|-----------------------|-----------|-----------|-----------------------------------------------------------------------------|
| Kafka-python - simple | C/P       | C/P       | Some issues with consumer on kafka 1.1; pls. refer to [Consumer](#consumer) |
| Kafka-python          | C/P       | C/P       |                                                                             |
| Pykafka               | C/P       | C/P       |                                                                             |
| Confluent             | %         | C/P       | Officially supported for kafka 0.9, but didn’t work properly                |

C=Consumer, P=Producer

<span id="_Test_results_for_1" class="anchor"></span>

Test results for kafka 1.1
--------------------------

### Producer sync.

<img src=".//media/image1.png" style="width:7.17708in;height:3.01389in" />

**Observations and judgement**:

-   **Kafka-python-simple**:  
    Throughput pretty good

<!-- -->

-   **Kafka-python**:  
    Pretty poor throughput, app. 5-times worse than kafka-python-simple!

-   **Pykafka**:  
    In all tests with synchronized mode, pykafka shows extremely low throughput, factor &gt;=40 worse than kafka-python-simple, factor &gt;=10 worse than kafka-python

-   **Confluent**:  
    Again, confluent provides by far the best throughput, even factor 2 better than kafka-python-simple

### Producer async.

<img src=".//media/image2.png" style="width:7.25in;height:3.00694in" />

**Observations & Judgement**:

-   **Kafka-python**:  
    Reasonable throughput

-   **Pykafka**:  
    With usage of C-library, the throughput is good

-   **Confluent**:  
    Very good throughput, at least 3.5 times better than any other client analyzed

### Consumer

<img src=".//media/image3.png" style="width:7.30208in;height:3.01389in" />

**Observations & Judgement**:

-   **Kafka-python – simple consumer:  
    **Low throughput – less than all other consumers  
    Note: Simple consumer didn’t work properly immediately. This was caused most likely by a protocol change in kafka (from 0.10). A setting that the old protocol shall be used resolved the problem:  
    kafka, server.properties:  
    log.message.format.version=0.9  
    Restart kafka.  
    For details, pls. refer to <https://kafka.apache.org/10/documentation.html#upgrade_10_performance_impact>

-   **Kafka-python**:  
    Reasonable throughput

-   **Pykafka**:.  
    Usage of C-library has huge impact on throughput:  
    o Without C-library: Reasonable throughput  
    o Usage of C-library: Good throughput  
    *Note:  
    pykafka has been tested with one more additional option: simple &lt;-&gt; balanced:  
    In balanced mode, balancing of consumers is done. However, this functionality is covered by kafka, from version 0.9.  
    *

-   **Confluent**:  
    Very good throughput: At least twice as high as with other clients

Test results for kafka 0.9
--------------------------

<img src=".//media/image4.png" style="width:7.47917in;height:3.35407in" />

**Notes**:

-   **Confluence** is not covered.  
    According to official documentation, confluent python client supports kafka 0.9. However, no reliable test results could be achieved with kafka 0.9: Error reported

    -   It wasn’t possible to reliably consume the number of messages written before

    -   No further investigation has been done to analyze the problem

-   Test results for **pykafka** are pretty similar to those for kafka 1.1. Thus, the complete test execution hasn’t been done again. Pls. refer to [Test results for kafka 1.1](#_Test_results_for_1) for details

**Observations**:

-   Consumer:

    -   **Kafka-python-simple**:  
        The currently used client (kafka-python, simple) delivers relatively poor throughput rates, that are outperformed by many other clients.  
        Throuphput is 8-times lower than Consumer client on 1.1!

    -   **Kafka-python**:  
        Default IF of kafka-python delivers app. 9 times better throughput

-   Producer:

    -   **Kafka-python-simple**:  
        The currently used client (kafka-python, simple) delivers pretty good throughput. From all clients with synchronous mode, it provides the best throughput

    -   **Kafka-python**:  
        Similar throughput as with kafka 1.1.

        -   **Synchronous**:  
            Pretty poor throughput, appl. 5-times worse than kafka-python-simple!

        -   **Asynchronous**:  
            Throughput significantly better than kafka-python-simple (nearly 2.5 times better). However, not extraordinary for async,

Quick overview of results (kafka 1.1 only)
------------------------------------------

<table>
<thead>
<tr class="header">
<th><strong>Client</strong></th>
<th><strong>Throughput</strong></th>
<th><strong>Remarks</strong></th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td></td>
<td><strong>Producer (sync.)</strong></td>
<td><strong>Producer (async.)</strong></td>
<td><strong>consumer</strong></td>
<td></td>
</tr>
<tr class="even">
<td>Kafka-python simple</td>
<td>+</td>
<td>%</td>
<td>o</td>
<td>Some issues of consumer with kafka 1.1</td>
</tr>
<tr class="odd">
<td>Kafka-python</td>
<td>-</td>
<td>O</td>
<td>+</td>
<td></td>
</tr>
<tr class="even">
<td>Pykafka</td>
<td>--</td>
<td>C-lib: +<br />
no C-lib: O</td>
<td>C-lib: +<br />
no C-lib: O</td>
<td></td>
</tr>
<tr class="odd">
<td>Confluent</td>
<td>++</td>
<td>++</td>
<td>++</td>
<td>Requires C-library; didn’t work reliably iwith kafka 0.9</td>
</tr>
</tbody>
</table>

Comparison of kafka 0.9 &lt;-&gt; 1.1
-------------------------------------

Apart from some incompatibilities (Confluent &lt;-&gt; kafka 0.9, some issues with kafka-python-simple &lt;-&gt; kafka 1.1), measurements taken were pretty similar, no major differences regarding throughput – with one exception:

Kafka-python simple consumer has 8-times higher throughput with kafka 1.1 than with kafka 0.9!

Reference to other performance comparisons
------------------------------------------

There are quite a few benchmark tests for kafka Python clients available.

One of the most comprehensive ones is:

<http://activisiongamescience.github.io/2016/06/15/Kafka-Client-Benchmarking/>

This benchmark compares the same Python kafka clients as those covered in this document.

2 major differences:

-   Testing has been done in 2016

-   Producers have only been tested in asynchronous mode

Comparison of results:

The measured throughput values can’t be compared: other versions, different hardware, …  
However, the relation between these values would be a better measure for a comparison:

-   **Consumers**:

| Test set   | Confluent | Pykafka (no C-lib) | Pykafka ( C-lib) | Kafka-python (used as base for rel. values) |          |          |          |          |
|------------|-----------|--------------------|------------------|---------------------------------------------|----------|----------|----------|----------|
|            | Absolute  | Relative           | Absolute         | Relative                                    | Absolute | Relative | Absolute | Relative |
| Activision | 261408    | 6.94               | 33977            | 0.90                                        | 164312   | 4.36     | 37668    | 1        |
| FEST       | 133986    | 3.0                | 30531            | 0.68                                        | 70537    | 1.58     | 44710    | 1        |

-   **Producers**:

| Test set   | Confluent | Pykafka (no C-lib) | Pykafka ( C-lib) | Kafka-python (used as base for rel. values) |          |          |          |          |
|------------|-----------|--------------------|------------------|---------------------------------------------|----------|----------|----------|----------|
|            | Absolute  | Relative           | Absolute         | Relative                                    | Absolute | Relative | Absolute | Relative |
| Activision | 183456    | 12.45              | 17446            | 1.18                                        | 63595    | 4.32     | 14737    | 1        |
| FEST       | 528313    | 49.55              | 10552            | 0.99                                        | 138292   | 12.97    | 10663    | 1        |

**Judgement**:  
In some cases, the factors differ quite a lot.  
There are many possible explanations (different HW, different parameters for clients, different versions, …). It has not been investigated further to find out the reason for the differences.

The important fact is:

Based on the numbers from Activision (async only for producers), the recommendations given for the relevant scenarios would be identical with the proposal given in chapter 3.

Proposal
========

The following assumptions are taken for the proposal:

Kafka-Version: &gt;= 1.0

The proposal heavily depends on the requirements.

Thus, 3 different scenarios are used for the proposal.

Assumptions for judgement:

-   Producer throughput shouldn’t be lower than current implementation (kafka-python-simple)

-   Consumer throughput should be significantly better than current implementation

<table>
<thead>
<tr class="header">
<th>Criteria</th>
<th>Scenario 1</th>
<th>Scenario 2</th>
<th>Scenario 3</th>
<th>Scenario 4</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>C-library OK</td>
<td>Yes</td>
<td>Yes</td>
<td>No</td>
<td>No</td>
</tr>
<tr class="even">
<td>Asynchronous producer</td>
<td>Yes</td>
<td>No</td>
<td>Yes</td>
<td>No</td>
</tr>
<tr class="odd">
<td>Client recommended</td>
<td>Confluent</td>
<td>Confluent</td>
<td>Pykafka or kafka-python</td>
<td>Kafka python, simple client provides reasonable throughput with kafka 1.0 and higher.<br />
None of the other clients fulfills all requirements</td>
</tr>
<tr class="even">
<td>Remarks</td>
<td>Provides clearly the best throughput. Async. Handling needs to be implemented</td>
<td>Provides clearly the best throughput</td>
<td>Further investigation necessary.<br />
Async. Handling needs to be implemented</td>
<td></td>
</tr>
</tbody>
</table>

**Summary**:

-   If the usage of a C-library is acceptable, Confluent client provides clearly the best throughput, no matter if producer is working synchronously or asynchronously.  
    This would allow as well a stepwise process:

    -   Migrate to confluent, use sync. Producer

    -   Move to async. producer

-   If no C-library shall be used, but asynchronous mode is OK, either pykafka or kafka-python can be used. Further investigation is required.

-   If no C-library shall be used and producer shall still operate synchronously, there is not a single client that fulfills all requirements.  
    As a temporary solution, kafka-python-simple can still be used as producer. As a consumer, kafka-python can be used. However, this would be only a temporary solution: From kafka 2.0 onwards, kafka-python-simple wouldn’t be supported any longer.

-   Pls. note that this recommendation is based only on the tests described in this document.  
    For a final decision, further actions like testing in an integrated monasca environment is necessary.  
    Pls. refer to [Next steps](#next-steps) for details.

Next steps
==========

As a next step, the selected kafka Python client should be tested, integrated in monasca.  
Then, several kind of tests can be executed:

-   Monasca tempest tests

-   Performance tests (e.g. Fujitsu System test)

-   Stress tests (e.g. Fujitsu system tests)

Besides, fine-tuning with configuration parameters can be done.

Appendix: Code Snippets
=======================

Example: Confluent client
-------------------------

-   **createProducer**:  
    def createProducer(self, kafkaSync):  
    self.config\["kafkaSync"\] = kafkaSync  
    config = {'bootstrap.servers': self.config\["kafkaHost"\], 'client.id': socket.gethostname(),  
    ‘default.topic.config': {'acks': 'all'}}  
    self.producer = confluent\_kafka.Producer(config)

-   **stopProducer**:  
    def stopProducer(self):  
    self.beforeFlushTimer(self.timeDict\['producer'\])  
    self.producer.flush(30)

-   **produce**:  
    def produce(self, num\_msg=20000):  
    self.msgCount = num\_msg  
    for x in range (self.msgCount):  
    if self.config\["kafkaSync"\] == False:  
    self.producer.produce(self.config\["topic"\], value=self.msg)  
    elif self.config\["kafkaSync"\] == True:  
    self.producer.produce(self.config\["topic"\], value=self.msg, callback=confluentAck)  
    \# **Note**: confluentAck checks if an error occurred  
    \# Wait up to 1 second for events. Callbacks will be invoked during  
    \# this method call if the message is acknowledged  
    self.producer.poll(1)  
    …

-   **createConsumer**:  
    self.consumerConfig = {'bootstrap.servers': self.config\["kafkaHost"\],  
    'client.id': socket.gethostname(),  
    'default.topic.config': {'acks': 'all'},  
    'enable.auto.commit': True,  
    'auto.commit.interval.ms':1000,  
    'group.id': "perftest",  
    'default.topic.config':  
    {  
    'auto.offset.reset': 'latest'  
    }  
    }  
    self.consumer = confluent\_kafka.Consumer(self.consumerConfig)  
    self.consumer.subscribe(\[self.config\["topic"\]\]

-   **consume**:  
    def consume(self, num\_msg):  
    count = 0  
    while True:  
    msg = self.consumer.poll(timeout=1.0)  
    if msg is None:  
    break  
    if msg.error():  
    if msg.error().code() != confluent\_kafka.KafkaError.\_PARTITION\_EOF:  
    print " Error: {}".format(msg.error())  
    break  
    else:  
    count += 1  
    if num\_msg &gt; 0:  
    if count != num\_msg:  
    print "ERROR: ConfluentClient.consume: \# of messages not as expected, read: {}, \\  
    expected: {}".format(count, num\_msg)  
    return count

-   **producerPerfTest**:  
    …  
    testObj = ConfluentClient(topic=topic,kafkaHost=kafkaHost, zookeeperHost=zookeeperHost)  
    testObj.createProducer(kafkaSync=kafkaSync)  
    testObj.createConsumer()  
    …  
    print " Start Test"  
    testObj.startProducer()  
    testObj.startTimer(testObj.timeDict\['producer'\])  
    testObj.produce(num\_msg=num\_msg)  
    testObj.stopProducer()  
    testObj.stopTimer(testObj.timeDict\['producer'\])  
    print " Producer Test completed"  
    time.sleep(30)  
    startTime = time.time()  
    testObj.startTimer(testObj.timeDict\['consumer'\])  
    testObj.consume(num\_msg)  
    testObj.stopTimer(testObj.timeDict\['consumer'\])  
    stopTime = time.time()  
    testObj.stopConsumer()  
    print " Consumer Test completed"

**  
**

-   **main**:  
    …  
    producerPerfTest(resultArr\[-1\], api="confluent", topic=topic, num\_msg=100000, kafkaSync=False,  
    kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen,  
    min\_queued\_messages=min\_queued\_messages)  
    …  
    producerPerfTest(resultArr\[-1\], api="confluent", topic=topic, num\_msg=100000, kafkaSync=True,  
    kafkaHost=kafkaHost, zookeeperHost=zookeeperHost, msgLen=msgLen,  
    min\_queued\_messages=min\_queued\_messages)
