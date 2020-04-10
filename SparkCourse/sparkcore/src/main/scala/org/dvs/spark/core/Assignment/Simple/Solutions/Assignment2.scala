package org.dvs.spark.core.Assignment.Simple.Solutions

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark.{SparkConf, SparkContext}

/*
 * User information (user-id, email, language, location)
 * datasource: /home/cloudera/projects/SparkCourse/sparkcore/src/main/resources/Datasets/Assignment/Assignment2/Customer.txt
 * Transaction information (transaction-id, product-id, user-id, purchase-amount, item-description)
 * datasource: /home/cloudera/projects/SparkCourse/sparkcore/src/main/resources/Datasets/Assignment/Assignment2/CustomerTransaction.txt
 *
 * find the number of unique locations in which each product has been sold.
 *
 * */

object Assignment2 extends App {

  case class User(userid: Int,
                  email: String,
                  language: String,
                  location: String)

  case class Transaction(transactionid: Int,
                         productid: Int,
                         userid: Int,
                         purchaseamount: Double,
                         productdescription: String)

  val conf = new SparkConf().setAppName("Assignment2").setMaster("local")
  val context = new SparkContext(conf)

  /*Reading the files and converting them to RDD*/
  val UserRDD = context.textFile(
    "/home/cloudera/projects/SparkCourse/sparkcore/src/main/resources/Datasets/Assignment/Assignment2/Customer.txt",
    2)

  val TransactionRDD = context.textFile(
    "/home/cloudera/projects/SparkCourse/sparkcore/src/main/resources/Datasets/Assignment/Assignment2/CustomerTransaction.txt",
    3)

  /*Convert Each Line into respective case class objects*/
  val UserObjectRDD = UserRDD.map(line => {
    val userarray = line.split(" ")
    User(userarray(0).toInt, userarray(1), userarray(2), userarray(3))
  })

  val TransObjectRDD = TransactionRDD.map(line => {
    val transarray = line.split(" ")
    Transaction(transarray(0).toInt,
                transarray(1).toInt,
                transarray(2).toInt,
                transarray(3).toDouble,
                transarray(4))
  })

  /*Join the Both the RDD common field b/w two RDD is userid.Join needs a paired RDD and it has to look like below
   * (userid,location)
   * (userid,product_id,productdescription)
   * As we are joining to join using userid make sure first element in both the paired RDD has to userid*/
  val UserPairRDD = UserObjectRDD.map(user => {
    (user.userid, user)
  })
  val TranPairRDD = TransObjectRDD.map(tran => {
    (tran.userid, tran)
  })
  /*Join*/
  val CombineRDD = UserPairRDD.join(TranPairRDD, 1)

  CombineRDD
    .map {
      case (customerid, (user, tranaction)) => {
        (tranaction.productid, user.location)
      }
    }
    .groupByKey() // returns (key,CompactBuffer(values)) for every key
    .map {
    //Performing map with pattern match key=productid value CompactBuffer(locations)
    //Example:(1,CompactBuffer(US, FR, GB, GB)) as compactBuffer is iterable you can convert them to List or Set
    //Approach 1: Write you own Tail Recursive function(Removeduplicates) to remove duplicates
    case (key, value) => {
        val cleanlist = Removeduplicates(value.toList, "", List())
        (key,cleanlist,cleanlist.size)
      }
        //Approch2 : Convert CompactBuffer to Set so that duplicate values will be removed
      case (key, value) => {
        val cleanlist = value.toSet
        (key,cleanlist,cleanlist.size)
      }
    }
    .foreach(println)


  /*Tail Recursive Function to remove duplicates*/
  def Removeduplicates(list: List[String],
                       currentvalue: String,
                       currentlist: List[String]): List[String] = {
    list match {
      case Nil => currentlist
      case x :: xs => {
        if (x == currentvalue) Removeduplicates(xs, currentvalue, currentlist)
        else Removeduplicates(xs, x, x :: currentlist)
      }
    }
  }

}
