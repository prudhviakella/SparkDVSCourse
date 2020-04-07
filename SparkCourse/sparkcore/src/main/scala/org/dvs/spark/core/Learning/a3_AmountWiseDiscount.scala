package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.SalesRecordParser


object AmountWiseDiscount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("apiexamples")//.setMaster("local")
    conf.set("spark.executor.memory","")
    val sc = new SparkContext(conf)
    //Transformation1
    val dataRDD = sc.textFile(args(0))

    //Transformation2
    val salesRecordRDD = dataRDD.map(row => {
      //Input           => Output
      //111,1,333,400.0 => SalesRecord(111,1,333,400.0) => new Function1[String,SalesRecord]{apply(v1:String)=>{}}
      //112,2,222,505.0 => SalesRecord(112,2,222,505.0)
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    //Transformation3
    //Input
    //SalesRecord(111,1,333,400.0)
    //SalesRecord(112,2,222,505.0)
    val totalAmountByCustomer = salesRecordRDD
      //Input
      //SalesRecord(111,1,333,400.0)
      //SalesRecord(112,2,222,505.0)
      .map(record => (record.customerId, record.itemValue))
      //(1,400.0)
      //        =>PairedRDD
      //(2,500.0)
      .reduceByKey(_ + _)

    //(Customerid,totalamountspend)

   // (custId, TotalAmount)
    val discountAmountByCustomer = totalAmountByCustomer.map(record => {
            val custId = record._1
            val totalAmt = record._2
          if(totalAmt > 1600) {
            val afterDiscount = totalAmt - (totalAmt * 10) / 100.0
            (custId, afterDiscount)
          }
          else
            (custId, totalAmt)
   })
//   val discountAmountByCustomer = totalAmountByCustomer.map {
//      case (customerId, totalAmount) => {
//        if (totalAmount > 1600) {
//          val afterDiscount = totalAmount - (totalAmount * 10) / 100.0
//          (customerId, afterDiscount)
//        }
//        else (customerId, totalAmount)
//      }
//    }

    totalAmountByCustomer.foreach(println)

    discountAmountByCustomer.collect().foreach(println)

    Thread.sleep(600000)

  }

}
