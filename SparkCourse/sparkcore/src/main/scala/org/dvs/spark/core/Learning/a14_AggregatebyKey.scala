package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.core.Learning.serialization.{
  SalesRecord,
  SalesRecordParser
}

object Aggregate {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("apiexample")
    conf.setMaster("local[*]")
    val sc = new SparkContext(conf)

    val dataRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/sales2.csv",
      2)
    val salesRecordRDD = dataRDD.map(row => {
      val parseResult = SalesRecordParser.parse(row)
      parseResult.right.get
    })

    val zeroValue = (Double.MaxValue, Double.MinValue)

    val seqOp = (minMax: (Double, Double), record: SalesRecord) => {
      val currentMin = minMax._1
      val currentMax = minMax._2
      val min =
        if (currentMin > record.itemValue) record.itemValue else currentMin
      val max =
        if (currentMax < record.itemValue) record.itemValue else currentMax
      (min, max)
    }

    val combineOp =
      (firstMinMax: (Double, Double), secondMinMax: (Double, Double)) => {
        ((firstMinMax._1 min secondMinMax._1),
         (firstMinMax._2 max secondMinMax._2))
      }

    val minmax = salesRecordRDD.aggregate(zeroValue)(seqOp, combineOp)

    /*val minmax = salesRecordRDD.aggregate((Double.MaxValue,Double.MinValue))( //initial value
       (acc, itr) =>{println(acc, itr);(acc._1 min itr.itemValue, acc._2 max itr.itemValue)}, //Seqop
       (a, b) => {println(a,b); (a._1 min b._1, a._2 max b._2)} // combiner
       )*/

    println("mi = " + minmax._1 + " and max = " + minmax._2)

  }

}
