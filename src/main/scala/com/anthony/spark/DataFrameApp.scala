package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @ Description:DataFrme练习
  * @ Date: Created in 18:02 2018/3/28
  * @ Author: Anthony_Duan
  */
object DataFrameApp {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("DataFrameApp").getOrCreate()

    val testDF = spark.read.format("json").load("file:///Users/duanjiaxing/data/test.json")
//    打印scheme信息
    testDF.printSchema()
//    打印前20条信息
    testDF.show()

    testDF.select("name").show()


    //查询某列所有的数据： select name from table
    testDF.select("name").show()

    // 查询某几列所有的数据，并对列进行计算： select name, age+10 as age2 from table
    testDF.select(testDF.col("name"), (testDF.col("age") + 10).as("age2")).show()

    //根据某一列的值进行过滤： select * from table where age>19
    testDF.filter(testDF.col("age") > 19).show()

    //根据某一列进行分组，然后再进行聚合操作： select age,count(1) from table group by age
    testDF.groupBy("age").count().show()

    spark.stop()

  }
}
