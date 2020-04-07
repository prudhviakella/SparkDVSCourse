package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.SalesRecordParser


object Counters {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    val dataRDD = sc.textFile(args(1),2)
    val malformedRecords = sc.accumulator(0)
    //println("Partitions: "  + dataRDD.partitions.length)
    // foreach is an action but runs at executor side

    dataRDD.foreach(record => {
      val parseResult = SalesRecordParser.parse(record)
      if(parseResult.isLeft){
        malformedRecords += 1
      }
    })

    println("No of malformed records is =  " + malformedRecords.value)

   /* val test = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
      if(parseResult.isLeft){
        malformedRecords += 1
      }
    })
    println(test.collect.toList)

    //dataRDD.foreach(println(_))
    //print the counter
    println("No of malformed records is =  " + malformedRecords.value)

    //Thread.sleep(50000)*/

  }

}
