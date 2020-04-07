package org.dvs.spark.sql.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.sql.Learning.serialization.{
  SalesRecord,
  SalesRecordParser
}

object a5_SalesRecordKryoSerilization {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster(args(0)).setAppName("apiexamples")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[SalesRecord]))
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(args(1))
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })
    println(salesRecordRDD.collect().toList)
  }
}
