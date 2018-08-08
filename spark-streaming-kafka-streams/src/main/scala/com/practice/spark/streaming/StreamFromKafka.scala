package com.practice.spark.streaming

import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._

import org.apache.spark.storage.StorageLevel

/*
 * Creating Dstreams using kafka 0.8 API using two different methods:
 * Receiver based and Direct Streaming
 * Word Count using kafka
 */

object StreamFromKafka { 
  
  //suppress logs
  
  def main(args: Array[String]) {
    
    val sparkSession = SparkSession.builder()
                                   .appName("Simple Kafka Connector")
                                   .master("local[2]")
                                   .getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(10))
    ssc.checkpoint("C:///Users/mtarigopula/Desktop/data/checkpoint")
    
    val zkQuorum = "zookeperaddress"
    val groupId = "grp"
    
    //(topicName, No.of.paritions)
    val topicMap = Map("topic1" -> 5, "topic2"->3)
        
    //receiver based streaming, one receiver per each executor
    val kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    val kafkaStream2 = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap, StorageLevel.DISK_ONLY_2) //storage level is optional parameter, see API/source code for more info

    val kafkaParams = Map(
        "metadata.broker.list" -> "localhost:9092",
        "zookeeper.connect" -> "localhost:2181",
        "group.id" -> "kafka-spark-streaming-example",
        "zookeeper.connection.timeout.ms" -> "1000")
        
    val topicSet = Set("topic1", "topic2")   
    //direct streaming
    val directStream = KafkaUtils.createDirectStream(ssc, kafkaParams, topicSet)
    //createDirectStream is overloaded method which takes different sets of params
    //(ssc, kafkaParams, Map("topic1" -> 5, "topic2"->3), StorageLevel.MEMORY_ONLY_SER)
    
    //Convert ReceivedInputDStream(k,v) into Dstream(value[String])
    val lines = kafkaStream.map(_._2)

    
    val words = lines.flatMap(_.split(" "))
    val wordcounts = words.map((_, 1L))
    val wordCount = wordcounts.countByWindow(Seconds(30), Seconds(30)) //when window duration and slide duration are same windows don't overlap
    
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
    
    
  }
  
}