package org.dvs.spark.core.Learning

import org.apache.spark.{SparkConf, SparkContext}

/*
* Dataset path : /home/cloudera/projects/SparkCourse/sparkcore/src/main/resources/Datasets/Learning/a1_WordCount.txt
* arg[0] -> Master -> local
* arg[1] -> Dataset Path
*
*/

object a1_WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordcount").setMaster(args(0)) //local[2]

    val sc = new SparkContext(conf)

    val inputRDD = sc.textFile(args(1), 2) // 2 tasks 10partitions  10/2 = 5 mins
    /*
      Hello World\n
      Something Junk\n
      Hello Whats\n
      Happening Hello\n
     */
    //Transformation1
    val wordRDD = inputRDD.flatMap(rec => rec.split(" "))
    //map + flatten
    /*
      [Hello,World]
      [Something,Junk]
      [Hello, Whats]
      [Happening, Hello]

      =>
      Hello
      World
      Something
      Junk
      Hello
      Whats
      Happening
      Hello
     */
    //Transformation2
    val wordPairRDD = wordRDD.map(word => (word, 1))
    /*
    (Hello,1)
    (World,1)
    (Junk,1)
    (Hello,1)
    (Whats,1)
    (Happening,1)
    (Hello,1)
     */
    //Transformation3
    //val groupbyKeyDD = wordPairRDD.groupByKey().reduce((x,y))
    //val ReduceRdd = groupbyKeyDD.reduce((x,y)=>x+y)
    val wordCountRDD = wordPairRDD.reduceByKey((x, y) => x + y)
    //groupbykey + foldleftbyvalues (0)
    /*
    (Hello,(1,1,1))
    (World,(1))
    (Junk,(1))
    (Whats,(1,1))
    (Happening,(1))
    =>
    Hello:
    x=0,y=1 =>1
    x = 1,y = 1 => 2
    x = 2, y =1 => 3
    (Hello,3)
    (World,1)
    (Junk,1)
    (Whats,2)
    (Happening,1)
     */
    //Transformation
    val swapRDD = wordCountRDD.map(x => (x._2, x._1))
    /*
    (3,Hello)
    (1,World)
    (1,Junk)
    (2,Whats)
    (1,Happening)
     */
    //limit or top 5 sorted records
    val SortRdd = swapRDD.sortByKey().top(5)
    //Transformations
    val reswapRdd = SortRdd.map(x => (x._2, x._1))
    //Total Number of words in Document
    val totalWordcounttotal = reswapRdd.map(x => x._2).reduce(_ + _)
    //Action
    wordCountRDD.foreach(println)
  }
}
