package com.anthony.log

import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * @ Description:
  * @ Date: Created in 14:04 2018/3/31
  * @ Author: Anthony_Duan
  */
object SparkStatCleanJobYARN {

  def main(args: Array[String]): Unit = {

    if (args!=2){
      println("Usage: SparkStatCleanJobYARN <inputPath><outPath>")
    }
    val Array(inputPath,outPath)=args
    val spark = SparkSession.builder().getOrCreate()

    val accessRDD = spark.sparkContext.textFile(inputPath)
//    accessRDD.take(10).foreach(println)

//    RDD=>DF
    val accessDF = spark.createDataFrame(accessRDD.map(x => AccessConvertUtil.parseLog(x)),
      AccessConvertUtil.struct)
//    accessDF.show(30)

    //coalesce 指定分区数  这是sparkSQL的一个调优点  可以根据数据大小来调整实际的分区数
    accessDF.coalesce(1)
      .write.format("parquet")//设置输出格式
      .mode(SaveMode.Overwrite)//设置保存模式  覆盖文件
      .partitionBy("day")//设置分区字段  这里按照day字段进行分区
      .save(outPath)

    spark.stop()
  }

}
