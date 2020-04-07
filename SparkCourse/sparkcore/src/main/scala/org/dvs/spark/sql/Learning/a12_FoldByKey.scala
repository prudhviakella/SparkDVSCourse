package org.dvs.spark.sql.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.sql.Learning.serialization.SalesRecordParser

object FoldByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("apiexample")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/sales2.csv",
      1)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val byCustomer = salesRecordRDD.map(salesRecord =>
      (salesRecord.customerId, salesRecord.itemValue))
    byCustomer.foreach(x => println(x))

    val maxByCustomer =
      byCustomer.foldByKey(Double.MinValue)((acc, itemValue) => {
        println("Accumulator:" + acc)
        println("ItemValue:" + itemValue)
        //The value that you returning back will reflect the value
        //lets say
        if (itemValue > acc) itemValue else acc
      })
    maxByCustomer.foreach(x => println("bycustomer:" + x))
    maxByCustomer.sortBy(x => x._2, false).foreach(println)

    val sortedByValue = maxByCustomer.sortBy(-_._2)

    val sortvalue: Array[(String, Double)] =
      maxByCustomer.sortBy(x => x._2, false).take(1)
    println("Max value is:" + sortvalue(0)._2)

    println("Sorted Values:" + sortedByValue.collect().toList)

  }

}
