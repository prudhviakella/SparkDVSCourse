package org.dvs.spark.core.Learning

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomerPartitioner(np: Int) extends Partitioner {

  def numPartitions: Int = np

  def getPartition(key: Any): Int = {
    println(s"customer partitioner is called for key:$key")
    key match {
      case null => 0
      case _ => {
        val keyValue = key.toString.toInt
        if (keyValue >= 3) 1 else 0
      }
    }
  }
}

object CustomPartition {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("custom partitioning example")
    conf.setMaster(args(0))
    val sc = new SparkContext(conf)

    val salesData = sc.textFile(args(1))

    val salesByCustomer = salesData.map(value => {
      val colValues = value.split(",")
      (colValues(1), colValues(2))
    })

    val groupedData = salesByCustomer.groupByKey(new CustomerPartitioner(2))

    //groupedData.collect().foreach(println)

    //printing partition specific data

    val groupedDataWithPartitionData = groupedData.mapPartitionsWithIndex {
      case (partitionNo, iterator) => {
        println("partition NO:" + partitionNo)
        for (record <- iterator)
          println(record)
        List((partitionNo, iterator.toList.length)).iterator
      }
    }
    val defaultPartRDD = salesByCustomer.groupByKey(4).mapPartitionsWithIndex {
      case (partitionNo, iterator) => {
        println("partition NO:" + partitionNo)
        for (record <- iterator)
          println(record)
        List((partitionNo, iterator.toList.length)).iterator
      }
    }
    println(groupedDataWithPartitionData.collect().toList)
    println(defaultPartRDD.collect().toList)
  }
}
