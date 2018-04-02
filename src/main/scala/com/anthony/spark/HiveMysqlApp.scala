package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @ Description: Hive和Mysql联合查询
  * @ Date: Created in 09:36 2018/3/30
  * @ Author: Anthony_Duan
  */
object HiveMysqlApp {

  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("HiveMysqlApp").master("local[2]").getOrCreate()

//    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
    val hiveDF = spark.table("emp")

    hiveDF.show()


    //      .config("spark.sql.warehouse.dir", warehouseLocation)


    //加载hive表数据

//    val mysqlDF = spark.read.format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306")
//      .option("dbtable","spark.DEPT")
//      .option("user","root")
//      .option("password","xiaoduan")
//      .option("diriver","com.mysql.jdbc.Driver")
//      .load()

//    mysqlDF.show()

  }


}
