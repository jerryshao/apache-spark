#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from py4j.java_collections import ListConverter, MapConverter, SetConverter
from py4j.java_gateway import java_import, Py4JError, Py4JJavaError

from pyspark.rdd import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.serializers import PairDeserializer, NoOpSerializer
from pyspark.streaming import DStream

__all__ = ['KafkaUtils', 'OffsetRange', 'utf8_decoder']


def utf8_decoder(s):
    """ Decode the unicode as UTF-8 """
    return s and s.decode('utf-8')


class KafkaUtils(object):

    @staticmethod
    def createStream(ssc, zkQuorum, groupId, topics, kafkaParams={},
                     storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2,
                     keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        Create an input stream that pulls messages from a Kafka Broker.

        :param ssc:  StreamingContext object
        :param zkQuorum:  Zookeeper quorum (hostname:port,hostname:port,..).
        :param groupId:  The group id for this consumer.
        :param topics:  Dict of (topic_name -> numPartitions) to consume.
                        Each partition is consumed in its own thread.
        :param kafkaParams: Additional params for Kafka
        :param storageLevel:  RDD storage level.
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A DStream object
        """
        kafkaParams.update({
            "zookeeper.connect": zkQuorum,
            "group.id": groupId,
            "zookeeper.connection.timeout.ms": "10000",
        })
        if not isinstance(topics, dict):
            raise TypeError("topics should be dict")
        jlevel = ssc._sc._getJavaStorageLevel(storageLevel)

        try:
            # Use KafkaUtilsPythonHelper to access Scala's KafkaUtils (see SPARK-6027)
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader()\
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createStream(ssc._jssc, kafkaParams, topics, jlevel)
        except Py4JJavaError as e:
            # TODO: use --jar once it also work on driver
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e
        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda k_v: (keyDecoder(k_v[0]), valueDecoder(k_v[1])))

    @staticmethod
    def createDirectStream(ssc, brokerList, topics, kafkaParams={},
                           keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        .. note:: Experimental

        Create an input stream that directly pulls messages from a Kafka Broker.

        This is not a receiver based Kafka input stream, it directly pulls the message from Kafka
        in each batch duration and processed without storing.

        This does not use Zookeeper to store offsets. The consumed offsets are tracked
        by the stream itself. For interoperability with Kafka monitoring tools that depend on
        Zookeeper, you have to update Kafka/Zookeeper yourself from the streaming application.
        You can access the offsets used in each batch from the generated RDDs (see

        To recover from driver failures, you have to enable checkpointing in the StreamingContext.
        The information on consumed offset can be recovered from the checkpoint.
        See the programming guide for details (constraints, etc.).

        :param ssc:  StreamingContext object
        :param brokerList: A String representing a list of seed Kafka brokers (hostname:port,...)
        :param topics:  list of topic_name to consume.
        :param kafkaParams: Additional params for Kafka
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A DStream object
        """
        java_import(ssc._jvm, "org.apache.spark.streaming.kafka.KafkaUtils")

        kafkaParams.update({"metadata.broker.list": brokerList})

        if not isinstance(topics, list):
            raise TypeError("topics should be list")
        jtopics = SetConverter().convert(topics, ssc.sparkContext._gateway._gateway_client)
        jparam = MapConverter().convert(kafkaParams, ssc.sparkContext._gateway._gateway_client)

        try:
            helperClass = ssc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            jstream = helper.createDirectStream(ssc._jssc, jparam, jtopics)
        except Py4JJavaError, e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(ssc.sparkContext)
            raise e

        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        stream = DStream(jstream, ssc, ser)
        return stream.map(lambda (k, v): (keyDecoder(k), valueDecoder(v)))

    @staticmethod
    def createRDD(sc, brokerList, offsetRanges, kafkaParams={},
                  keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):
        """
        .. note:: Experimental

        Create a RDD from Kafka using offset ranges for each topic and partition.
        :param sc:  SparkContext object
        :param brokerList: A String representing a list of seed Kafka brokers (hostname:port,...)
        :param offsetRanges:  list of offsetRange to specify topic:partition:[start, end) to consume
        :param kafkaParams: Additional params for Kafka
        :param keyDecoder:  A function used to decode key (default is utf8_decoder)
        :param valueDecoder:  A function used to decode value (default is utf8_decoder)
        :return: A RDD object
        """
        kafkaParams.update({"metadata.broker.list": brokerList})

        if not isinstance(offsetRanges, list):
            raise TypeError("offsetRanges should be list")
        jparam = MapConverter().convert(kafkaParams, sc._gateway._gateway_client)

        try:
            helperClass = sc._jvm.java.lang.Thread.currentThread().getContextClassLoader() \
                .loadClass("org.apache.spark.streaming.kafka.KafkaUtilsPythonHelper")
            helper = helperClass.newInstance()
            joffsetRanges = ListConverter().convert([o._joffsetRange(helper) for o in offsetRanges],
                                                    sc._gateway._gateway_client)
            jrdd = helper.createRDD(sc._jsc, jparam, joffsetRanges)
        except Py4JJavaError, e:
            if 'ClassNotFoundException' in str(e.java_exception):
                KafkaUtils._printErrorMsg(sc)
            raise e

        ser = PairDeserializer(NoOpSerializer(), NoOpSerializer())
        rdd = RDD(jrdd, sc, ser)
        return rdd.map(lambda (k, v): (keyDecoder(k), valueDecoder(v)))

    @staticmethod
    def _printErrorMsg(sc):
        print """
________________________________________________________________________________________________

  Spark Streaming's Kafka libraries not found in class path. Try one of the following.

  1. Include the Kafka library and its dependencies with in the
     spark-submit command as

     $ bin/spark-submit --packages org.apache.spark:spark-streaming-kafka:%s ...

  2. Download the JAR of the artifact from Maven Central http://search.maven.org/,
     Group Id = org.apache.spark, Artifact Id = spark-streaming-kafka-assembly, Version = %s.
     Then, include the jar in the spark-submit command as

     $ bin/spark-submit --jars <spark-streaming-kafka-assembly.jar> ...

________________________________________________________________________________________________

        """ % (sc.version, sc.version)


class OffsetRange(object):
    """
    Represents a range of offsets from a single Kafka TopicAndPartition.
    """

    def __init__(self, topic, partition, fromOffset, untilOffset):
        """
        Create a OffsetRange to represent  range of offsets
        :param topic: Kafka topic name.
        :param partition: Kafka partition id.
        :param fromOffset: Inclusive starting offset.
        :param untilOffset: Exclusive ending offset.
        """
        self._topic = topic
        self._partition = partition
        self._fromOffset = fromOffset
        self._untilOffset = untilOffset

    def _joffsetRange(self, helper):
        return helper.createOffsetRange(self._topic, self._partition, self._fromOffset,
                                        self._untilOffset)
