package org.dvs.spark.sql.Learning

import org.apache.spark.{SparkConf, SparkContext}
import org.dvs.spark.sql.Learning.serialization.SalesRecordParser

object ShuffleBased {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local").setAppName("apiexamples")
    val sc = new SparkContext(conf)

    //job1&job2:Stage0
    val salesRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/sales.csv",
      3)
    val salesPair = salesRDD.map(row => {
      val salesRecord = SalesRecordParser.parse(row).right.get
      (salesRecord.customerId, salesRecord) //=>(1,SalesRecord(111,1,333,400.0))
    })

    //job0&job2:Stage1
    val customerRDD = sc.textFile(
      "/home/cloudera/projects/spark-core/src/main/resources/customers.csv",
      2)
    val customerPair = customerRDD.map(row => {
      val columnValues = row.split(",")
      (columnValues(0), columnValues(1)) //=>(1,"John")
    })

    //job0:Stage2
    val joinRDD = customerPair.join(salesPair) //=>(1,(("John",SalesRecord(111,1,333,400.0))
    val action1 = joinRDD.map {
      case (customerId, (customerName, salesRecord)) => {
        (customerName, salesRecord.itemId, salesRecord.itemValue)
      }
    }
    //Action1-> This action will create job 0
    action1.foreach(record => println("Action1 Results:" + record))

    //job1:Stage2
    val leftJoinRDD = customerPair.leftOuterJoin(salesPair) //(1,("John",SalesRecord())
    val action2 = leftJoinRDD.map(x => {
      val customerId = x._1
      val customerName = x._2._1
      val salesRec = x._2._2.getOrElse("NA")
      (customerName, salesRec)
    })
    //Action2-> This Action will create job1
    action2.foreach(record => println("Action2 Results:" + record))

    /*-------------------------------------------------------Optimizations-------------------------------------------------------------------*/

    //Optimization1: Using CO-Group Instead of Performing Two joins separately use the Co-group its acts like Full-Outer join so those results
    //you can  get Inner join,Right outer join and Left outer join so shuffling of data will happen only once
    //Co-group--Its acts like Fullouter join
    //The different between Co-group and join is join gives Flatten results but Co-group give Results as Iterables like below
    //(3,(CompactBuffer(SalesRecord(113,3,444,510.0), SalesRecord(119,3,333,3300.0), SalesRecord(122,3,444,4500.0),
    // SalesRecord(124,3,222,5100.0)),CompactBuffer(Micheal)))
    //Each CompactBuffer is a Iterable mean you can perform map,flatmap,foreach operations on that
    val CogroupRDD = salesPair.cogroup(customerPair)
    val Innerjoin = CogroupRDD.filter(element =>
      element._2._1.size != 0 && element._2._2.size != 0)
    val leftOuterjoin = CogroupRDD.filter(element => element._2._2 == 0)
    val rightOuterjoin = CogroupRDD.filter(element => element._2._1 == 0)

    val action3 = Innerjoin.flatMap(element => {
      val CustomerId = element._1
      val CustomerName = element._2._2.head // Left will have only element in CompactBuffer(Micheal) so getting head
      val SalesRecordRDD = element._2._1 //Getting Left CompactBuffer(SalesRecord(113,3,444,510.0), SalesRecord(119,3,333,3300.0),....)
      SalesRecordRDD.map(salesRecord =>
        (CustomerName, salesRecord.itemId, salesRecord.itemValue)) // This returns the List of results so flattening it
      //with flatmap. if you want to see the results as List instead of flatmap use map or use the below commented map code and print results
    })
    //Action3 - job2
    action3.foreach(record => println("Action3 Results:" + record))

//    val mapRDD = Innerjoin.map(element => {
//      val CustomerId = element._1
//      val CustomerName = element._2._2.head// Left will have only element in CompactBuffer(Micheal) so getting head
//      val SalesRecordRDD = element._2._1 //Getting Left CompactBuffer(SalesRecord(113,3,444,510.0), SalesRecord(119,3,333,3300.0),....)
//      SalesRecordRDD.map(salesRecord=>(CustomerName,salesRecord.itemId,salesRecord.itemValue)) // This returns the array of results of flattening it
//      //with flatmap if you want to see the results with Array instead of flatmap use map and print results
//    })
//    //Action3
//    mapRDD.foreach(record=>println("Action3 Results:"+record))

    //val InnerJoinRDD = leftJoinRDD.filter(x=>x._2._2 != null)

    //Optimization2: Perform LeftOuterjoin and get Innerjoin results from that so that shuffle will happen only Once
    val leftJoinRDDoptimized = customerPair.leftOuterJoin(salesPair) //(1,("John",SalesRecord())
    val action4 = leftJoinRDDoptimized.map(x => {
      val customerId = x._1
      val customerName = x._2._1
      val salesRec = x._2._2.getOrElse("NA")
      (customerName, salesRec)
    })
    //Action4 - job3
    action4.foreach(x => println("Action4 Results:" + x))

    val InnerjoinOptimized = leftJoinRDDoptimized.filter(x => x._2._2 != null)

    //Action5 - job4(compare results of job4 and job0 In job0 you will see shuffling will happen but in job4 those tasks will be skipped
    InnerjoinOptimized.foreach(record => println("Action5 Results:" + record))
    //This sleep is used wait the Application for 10 mins so that we can able to see DAG from UI
    Thread.sleep(300000)

  }

}
