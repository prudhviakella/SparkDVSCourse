package org.dvs.spark.core.Learning

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.{
  SalesRecord,
  SalesRecordParser
}

object HandleMalformedRecords {

  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local").setAppName("apiexamples")
    //conf.set("spark.memory.storageFraction","0.7")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/sales-error.csv")

    val parsedRdd = dataRDD.map(record => {
      val parseResult = SalesRecordParser.parse(record)
      if (parseResult.isRight) {
        (true, parseResult.right.get) //(true,SalesRecord(111,1,222,400.0)
      } else (false, record) //(false,"121,3,777")
    })

    parsedRdd.persist(StorageLevel.MEMORY_ONLY_SER)

    /*If you clearly observe */
    val malformedRecords = parsedRdd.filter(x => x._1 == false).map(_._2) //.cache

    val normalRecords = parsedRdd
      .filter(x => x._1 == true)
      .map(x =>
        x._2 match {
          case y: SalesRecord => y
      })

    normalRecords.map(x => x.itemValue).foreach(println)

    //val normalRecords1 = parsedRdd.map(_._2).subtract(malformedRecords)
    //val salesRecordRDD = normalRecords.map(row => SalesRecordParser.parse(row).right.get)

//    println(malformedRecords.collect().toList)
//    println(malformedRecords.count())
//    println(normalRecords.collect().toList)

    //parsedRdd.unpersist()

    normalRecords.foreach(println)
    malformedRecords.foreach(println)

    Thread.sleep(500000)

  }

}
