package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @ Description: parquet文件的操作
  * @ Date: Created in 08:44 2018/3/30
  * @ Author: Anthony_Duan
  */
object parquetApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("parquetApp").master("local[2]").getOrCreate()

    val path = "file:///Users/duanjiaxing/data/users.parquet"
    val userDF = spark.read.format("parquet").load(path)
    userDF.printSchema()
    userDF.show()
    userDF.select("name", "favorite_color").show()
    userDF.select("name", "favorite_color").write.format("json").save("file:///Users/duanjiaxing/data/jsonout")


    //spark中默认读取文件的格式为parquet，如果没有指定文件类型默认为parquet文件类型
    //option可以传入参数 path可以将在option传入
    spark.read.format("parquet").option("path", path).load().show

    spark.stop()
  }

}
