package com.anthony.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Description: SQLContext的使用
  * @ Date: Created in 07:09 2018/3/28
  * @ Author: Anthony_Duan
  */
object SQLContextApp {

  def main(args: Array[String]): Unit = {


    val path = args(0)

    //1）创建相应的Context
    val sparkConf = new SparkConf()

//    在生产上通过脚本的方式指定
//    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    //2)相关处理：json
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    //3)关闭资源
    sc.stop()

  }


}
