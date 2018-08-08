package com.practice.spark.streaming

import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession

object StreamingAPI extends App{
  
  val spark= SparkSession.builder().appName("TableDecisionDemo").master("local[2]").getOrCreate()
  val sc = spark.sparkContext
  
  val ssc = new StreamingContext(sc, Seconds(1))
  
  val lines = ssc.textFileStream("C:///Users/mtarigopula/Desktop/data/input")
  
  val words = lines.flatMap(_.split(" "))
  val wordcounts = words.map((_, 1L))
                       //count the tuples //window, slide
  val wordCount = wordcounts.countByWindow(Seconds(30), Seconds(30))
  
  wordCount.print()
  
  /**Save the Dstream to HDFS as CSV files**/
  import spark.implicits._
  wordCount.foreachRDD(rdd=> rdd.toDF.write.csv("hdfs path"))
    
  wordCount.countByValueAndWindow(Seconds(30), Seconds(20),5).print()
  
  ssc.start()
  
  ssc.awaitTermination()
  
}