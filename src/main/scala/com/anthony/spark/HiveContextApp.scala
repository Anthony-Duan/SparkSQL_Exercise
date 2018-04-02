package com.anthony.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * @ Description:
  * @ Date: Created in 07:58 2018/3/28
  * @ Author: Anthony_Duan
  */
object HiveContextApp {

  def main(args: Array[String]): Unit = {

    //1）创建相应的Context
    val sparkConf = new SparkConf()

    //    在生产上通过脚本的方式指定
        sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)

    //2)相关处理：hiveTable
    hiveContext.table("emp").show()


    //3)关闭资源
    sc.stop()
  }


}
