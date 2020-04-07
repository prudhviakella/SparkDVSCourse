package org.dvs.spark.sql.Learning.serialization

case class SalesRecord(transactionId: String,
                       customerId: String,
                       itemId: String,
                       itemValue: Double) {

  /*
  override def toString: String = {
    transactionId+","+customerId+","+itemId+","+itemValue
  } */
}
