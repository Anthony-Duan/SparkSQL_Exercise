package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @ Description:
  * @ Date: Created in 22:14 2018/3/29
  * @ Author: Anthony_Duan
  */
object DataSetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataSetApp").master("local[2]").getOrCreate()
    //注意导入隐式转换
    import spark.implicits._

    val path = "file:///Users/duanjiaxing/data/sales.csv"
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show()
    val ds = df.as[Sales]
    ds.map(line => line.itemId).show
    ds.map(line => line.amountPaid).show()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}
