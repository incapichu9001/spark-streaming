package com.practice.spark.streaming

import org.apache.spark.streaming._
import org.apache.spark.sql.SparkSession

object SocketStreaming extends App{
  
val spark= SparkSession.builder().appName("TableDecisionDemo").master("local[2]").getOrCreate()
val sc = spark.sparkContext
  
//val conf = new SparkConf().setMaster("local[2]").setAppName("TestApp")
val ssc = new StreamingContext(sc, Seconds(1))

val lines = ssc.socketTextStream("crlnxt049.us.aegon.com", 63333)

//val rdd1 = 

val tableDF = spark.sql("""select sourcesystemname from enterprisedatalakedev.gdqdatastoragetest""") 
val tableList = tableDF.collect()

lines.foreachRDD { rdd=>
val spark1 = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
import spark.implicits._
val tableDF = spark1.sql("""select sourcesystemname from enterprisedatalakedev.gdqdatastoragetest""")
val tableList = tableDF.map(x=> x.getString(0)).collect()
val matched = rdd.map(x => (tableList.contains(x), x)).collect()
matched.foreach{ x => 
if(x._1) println(s"${x._2} is present in list")
else println(s"${x._2} is NOTTT present in list")
}
}

ssc.start()
ssc.awaitTermination()
}