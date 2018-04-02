package com.anthony.log

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * @ Description:
  * @ Date: Created in 12:39 2018/4/2
  * @ Author: Anthony_Duan
  */
object TopNstatJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").appName("TopNstatJob")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled",false)
      .getOrCreate()

    val accessDF = spark.read.parquet("file:///Users/duanjiaxing/data/clean")
//    accessDF.printSchema()
//    accessDF.show(30,false)

    videoAccessTopNStat(spark,accessDF)
  }

  /**
    * TopN统计
    * @param spark
    * @param accessDF
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame): Unit ={

    import spark.implicits._

    //使用agg聚合函数需要手动导入 import org.apache.spark.sql.functions._
    val videoAccessTopNDFAPI = accessDF.filter($"day" ==="20170511" && $"cmsType" ==="video")
      .groupBy("day","cmsId").agg(count("cmsId") as("times"))
      .orderBy($"times".desc)

    videoAccessTopNDFAPI.show(false)

    accessDF.createOrReplaceTempView("access_log")


    val videoAccessTopNDFSQL = spark.sql("select day,cmsId,count(1) as times from access_log " +
      "where day='20170511' and cmsType='video' group by day,cmsId order by times desc")
    videoAccessTopNDFSQL.show(false)
  }

}
