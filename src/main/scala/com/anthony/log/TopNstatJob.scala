package com.anthony.log

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer
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

    val day = "20170511"
//    删除某天的数据
    StatDAO.deleteData(day)

    videoAccessTopNStat(spark,accessDF)
    cityAccessTopNStat(spark,accessDF,day)
    videoTrafficsTopNStat(spark,accessDF,day)
    spark.stop()
  }


  /**
    * 按照流量进行统计
    */

  def videoTrafficsTopNStat(spark:SparkSession,accessDF:DataFrame,day:String)={
    import spark.implicits._

    val cityAccessTopNDF = accessDF.filter($"day"===day && $"cmsType" ==="video")
      .groupBy("day","cmsId").agg(sum("traffic").as("traffics"))
      .orderBy($"traffics".desc)

    try {
      cityAccessTopNDF.foreachPartition(partitionOfRecords=>{

        val list = new  ListBuffer[DayVideoTrafficsStat]
        partitionOfRecords.foreach(info=>{
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val traffics = info.getAs[Long]("traffics")

          list.append(DayVideoTrafficsStat(day,cmsId,traffics))
        })
        StatDAO.insertDayVideoTrafficsAccessTopN(list)

      })
    }

  }






  /**
    * 按照城市进行统计
    */
  def cityAccessTopNStat(spark:SparkSession,accessDF:DataFrame,day:String): Unit ={
    import spark.implicits._
    val cityAccessTopNDF = accessDF.filter($"day"===day&&$"cmsType"==="video")
      .groupBy("day","city","cmsId").agg(count("cmsId").as("times"))

//    cityAccessTopNDF.show(false)

    //Window函数在sparkSQL中的使用
    val top3DF = cityAccessTopNDF.select(
      cityAccessTopNDF("day"),
      cityAccessTopNDF("city"),
      cityAccessTopNDF("cmsId"),
      cityAccessTopNDF("times"),
      row_number().over(Window.partitionBy(cityAccessTopNDF("city"))
        .orderBy(cityAccessTopNDF("times").desc)
      ).as("times_rank")
    ).filter("times_rank <= 3")   //Top3
    try{
      top3DF.foreachPartition(partitionOfRecords =>{
        val list = new ListBuffer[DayCityVideoAccessStat]
        partitionOfRecords.foreach(info=>{
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val city = info.getAs[String]("city")
          val times = info.getAs[Long]("times")
          val times_rank = info.getAs[Int]("times_rank")
          list.append(DayCityVideoAccessStat(day,cmsId,city,times,times_rank))
        })
        StatDAO.insertDayCityVideoAccessTopN(list)
      })
    }catch {
      case e:Exception=>e.printStackTrace()
    }
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

//    videoAccessTopNDFAPI.show(false)

    accessDF.createOrReplaceTempView("access_log")


    val videoAccessTopNDFSQL = spark.sql("select day,cmsId,count(1) as times from access_log " +
      "where day='20170511' and cmsType='video' group by day,cmsId order by times desc")
//    videoAccessTopNDFSQL.show(false)

    try {
      videoAccessTopNDFAPI.foreachPartition(partitionOfRecords=>{
        val list = new ListBuffer[DayVideoAccessStat]
        partitionOfRecords.foreach(info=>{
          val day = info.getAs[String]("day")//这里的字段day cmsId等字段是DF中的字段
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")

          //不建议此处进行数据库插入
          list.append(DayVideoAccessStat(day,cmsId,times))

        })
        StatDAO.inertDayVideoAccessTopN(list)

      })
    }catch {
      case e => e.printStackTrace()
    }
  }

}
