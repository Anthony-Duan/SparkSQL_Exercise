package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @Description:
  * @Date: Created in 08:59 2018/3/28
  * @Author: Anthony_Duan
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSessionApp")
      .master("local[2]").getOrCreate()

    val fileDF = spark.read.json("file:///Users/duanjiaxing/data/test.json")
    fileDF.show()

    spark.stop()
  }

}
