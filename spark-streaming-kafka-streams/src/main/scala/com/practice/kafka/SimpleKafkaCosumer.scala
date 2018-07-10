package com.practice.kafka

import java.util.Properties
import org.apache.kafka.clients.consumer.KafkaConsumer
import scala.collection.JavaConverters._

object SimpleKafkaCosumer extends App{

  val topic = "Test"
  
  val props:Properties = new Properties()
  
  val x = props.put("key", "value")
  
  
}