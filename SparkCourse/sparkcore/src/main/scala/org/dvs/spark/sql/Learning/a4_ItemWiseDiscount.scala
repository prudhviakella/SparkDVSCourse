package org.dvs.spark.sql.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.sql.Learning.serialization.SalesRecordParser

object ItemWiseDiscount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("apiexamples")

    //following 2 lines are required for spark history server. All DAGs are shown in UI
    conf.set("spark.eventLog.enabled", "true")
    conf.set(
      "spark.eventLog.dir",
      "/home/cloudera/projects/spark-core/src/main/resources/sparkhistoryserverlogs")
    val sc = new SparkContext(conf)
    //val dataRDD = sc.textFile("/home/cloudera/projects/spark-core/src/main/resources/sales.csv")
    val dataRDD = sc.textFile(
      "hdfs://quickstart.cloudera:8020/user/cloudera/csvdata/sales.csv")
    val salesRecordRDD = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
      parseResult.right.get
    })
    salesRecordRDD.sortBy(x => x.itemValue)
    //salesRecordRDD.sortBy(- _.itemValue)

    //apply discount for each book from a customer

    val itemDiscountRDD = salesRecordRDD.map(sales => {
      val itemValue = sales.itemValue
      val newItemValue = itemValue * 0.95 //itemValue - (itemValue * 5) / 100.0
      (sales.customerId, newItemValue)
    })

    val totalAmountByCustomer = itemDiscountRDD.reduceByKey(_ + _) //.sortBy(_._2,false)
    //val totalAmountByCustomer = itemDiscountRDD.map(row => (row.customerId,row.itemValue)).reduceByKey(_+_)
    totalAmountByCustomer.collect().foreach(println)

  }
}
