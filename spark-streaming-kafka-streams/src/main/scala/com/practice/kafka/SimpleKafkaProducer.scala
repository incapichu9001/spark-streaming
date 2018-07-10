package com.practice.kafka

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import java.util.concurrent.Future


object SimpleKafkaProducer extends App{
  
  val TOPIC = "KafkaTopic1"
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
 
  val producer = new KafkaProducer[String, String](props)
  try{
    for(i<- 1 to 100) {
      
      val record = new ProducerRecord(TOPIC, s"key-$i", s"hello $i")
      val result: Future[RecordMetadata] = producer.send(record)
      
    }
  }catch {
    case e:Exception => e.printStackTrace()
    producer.close()
  }
  finally {
    producer.close()
  }
  
  
}