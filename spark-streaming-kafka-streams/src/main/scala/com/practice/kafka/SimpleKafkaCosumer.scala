package com.practice.kafka

import java.util.Properties
import org.apache.kafka.clients.consumer.{KafkaConsumer, ConsumerRecords, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import scala.collection.JavaConverters._

import java.util

object SimpleKafkaCosumer extends App {

  val TOPIC = "Test"
  val topicPartition2 = new TopicPartition(TOPIC, 3)
  
  val props:Properties = new Properties()
  
  val properties: Properties = props.put("key", "value").asInstanceOf[Properties]
  
  val consumer = new KafkaConsumer(properties)
  
  //subscribe to a topic
  consumer.subscribe(TOPIC)
  val x = util.Collections.singletonList(TOPIC)
  //read from a particular partition
  //consumer.subscribe(topicPartition2)
  
  while(true) {
    //: java.util.Map[String, ConsumerRecords[Any, Any]] to scala.collection.Map[String, ConsumerRecords[String, String]]
    val messages = consumer.poll(1000)
    messages.forEach{ crs => 
        val records = crs.records()
        for(record <- records.asScala){
          println(record)
        }    
    }
      
  }

}
    
