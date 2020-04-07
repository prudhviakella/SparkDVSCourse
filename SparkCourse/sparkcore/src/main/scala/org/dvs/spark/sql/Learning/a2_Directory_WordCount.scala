package org.dvs.spark.sql.Learning

import org.apache.spark.{SparkConf, SparkContext}

object Directory_WordCount {

  def main(args: Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Directory wordcount")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)
    //Default Partitions
    println("Default Partitions:" + sc.defaultParallelism)
    val inputRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/directories.txt",
      1)
    println("text file transformation partitioner:" + inputRDD.partitioner)
    println("partition length:" + inputRDD.partitions.size)
    //Get number of partitions for RDD
    println("text file transformation partitions:" + inputRDD.getNumPartitions)
    //Transformation1
    val wordRDD = inputRDD.map(rec => rec.split("/"))
    println("First map transformation partitioner:" + wordRDD.partitioner)
    //Transformation2
    val wordPairRDD = wordRDD.map(word => (word(1), 1))
    println("Second map transformation partitioner:" + wordPairRDD.partitioner)
    //Transformation3
    val wordCountRDD = wordPairRDD.reduceByKey(_ + _)
    wordCountRDD.cache()
    /*
      hash paritioner => hashcode(key) % (number of paritions) => 1234555 % 3 => 2
     */
    println("Reduce by key paritioner" + wordCountRDD.partitioner)
    //Action
    wordCountRDD.collect().foreach(println)
  }
}
