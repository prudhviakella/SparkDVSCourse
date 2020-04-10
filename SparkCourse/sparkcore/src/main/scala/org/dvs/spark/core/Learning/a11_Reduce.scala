package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.SalesRecordParser

object Reduce {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("apiexample").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/sales2.csv",
      1)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    dataRDD.foreach(println)
    //give me salesRecord which has max value
    //val dummySalesRecord = new SalesRecord(null,null,null,Double.MinValue)

    val maxSalesRecord = salesRecordRDD.reduce((acc, salesRecord) => {
      println("Acc:" + acc)
      println("salesRecord:" + salesRecord)
      if (acc.itemValue < salesRecord.itemValue) salesRecord else acc
    })

    println("max sale record is " + maxSalesRecord)
  }

}
