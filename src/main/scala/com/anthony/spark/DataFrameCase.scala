package com.anthony.spark

import org.apache.spark.sql.SparkSession

/**
  * @ Description:
  * @ Date: Created in 20:44 2018/3/29
  * @ Author: Anthony_Duan
  */
object DataFrameCase {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameCase").master("local[2]").getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///Users/duanjiaxing/data/student.data")

    //注意：需要导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    //    studentDF.show()
    //    show 函数默认显示20条，并且长度超过一定限度默认隐藏
    studentDF.show(30, truncate = false)
    //    Returns the first `n` rows in the Dataset.
    studentDF.take(2)

    //    Returns the first `n` rows.
    studentDF.head(3)
    //    Returns the first row. Alias for head().
    studentDF.first()

    studentDF.select("name").show()

    studentDF.filter("name='' OR name='NULL'").show()
    studentDF.filter(studentDF.col("id") > 15).show()
    studentDF.filter("SUBSTR(name,0,1)='M'").show(false)

    studentDF.sort("name", "id").show()
    studentDF.sort(studentDF.col("id").desc, studentDF.col("name").desc).show()
    studentDF.select(studentDF.col("name") as "theName").show()

    val studentDF2 = rdd.map(_.split("\\|")).map(line => Student(line(0).toInt, line(1), line(2), line(3))).toDF()

    studentDF.join(studentDF2, studentDF.col("id") === studentDF2.col("id")).show()

    spark.stop()

  }

  case class Student(id: Int, name: String, phone: String, email: String)

}
