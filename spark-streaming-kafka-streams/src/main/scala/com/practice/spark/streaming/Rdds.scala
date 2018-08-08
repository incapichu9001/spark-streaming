package com.practice.spark.streaming

import org.apache.spark.sql.SparkSession
/*
 * Actions and Transformations
 * Rdd functions, pairRdd functions
 */
object Rdds extends App{
  
  val spark= SparkSession.builder().appName("TableDecisionDemo").master("local[2]").getOrCreate()
  val sc = spark.sparkContext
  
  val lines = sc.textFile("somepath", 5)
  //Action, gets result to driver
  val c = lines.count
  
  /**Pair RDD functions**/
  //reduceByKey - transformation - takes a function - gives an rdd
  val res = lines.map((_, 1L)).reduceByKey(_+_)
  
  //CountByKey - action - gives a Scala Map (collection)
  val counts = lines.map((_, 1L)).countByKey()
  
  //foldByKey - transformation - takes a zero value, function - gives an rdd (functionality similar to fold function in scala)
  val foldsum = lines.map((_, 1L)).foldByKey(0)(_+_) //for addition function can use '0' which is additive identity
  
  val foldprod = lines.map((_, 1L)).foldByKey(1)(_*_) //for product function can use 1 which is multiplicative identity
  
  
}