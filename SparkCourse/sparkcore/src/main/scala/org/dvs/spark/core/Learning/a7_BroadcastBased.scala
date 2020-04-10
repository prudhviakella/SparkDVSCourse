package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.SalesRecordParser

import scala.collection.mutable

object BroadcastBased {

  def creatCustomerMap(customerDataPath: String) = {

    val customerMap = mutable.Map[String, String]()

    val lines = scala.io.Source.fromFile(customerDataPath).getLines()
    while (lines.hasNext) {
      val values = lines.next().split(",")
      //for(line <- lines) {
      //val values = line.split(",")
      customerMap.put(values(0), values(1))
    }
    customerMap
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    val sc = new SparkContext(conf)
    val salesRDD = sc.textFile(args(1))
    val customerDataPath = args(2)
    //val customerMap = creatCustomerMap(customerDataPath)

    val customerMap = scala.io.Source
      .fromFile(customerDataPath)
      .getLines()
      .map(x => x.split(","))
      .map(x => (x(0), x(1)))
      .toMap
    //broadcast data

    val customerBroadCast = sc.broadcast(customerMap)

    val joinRDD = salesRDD.map(rec => {
      val salesRecord = SalesRecordParser.parse(rec).right.get
      val customerId = salesRecord.customerId
      val custMap = customerBroadCast.value
//      val customerName = custMap.get(customerId) match {
//        case None => "Unknonw User"
//        case Some(custName) => custName
//      }
      val customerName = custMap.getOrElse(customerId, "Unknown User")
      (customerName, salesRecord)
    })

    joinRDD.foreach(println)

    //joinRDD.saveAsTextFile("src/main/resources/join_output")

    //joinRDD.saveAsTextFile("src/main/resources/join_output1")

  }

}
