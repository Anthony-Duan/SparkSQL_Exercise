package com.anthony.spark

import java.sql.DriverManager


/**
  * @Description: 使用JDBC的连接sparkSQL
  * @Date: Created in 16:29 2018/3/28
  * @Author: Anthony_Duan
  */
object SparkSQLThriftServerApp {

  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val conn = DriverManager.getConnection("jdbc:hive2://localhost:14000","duanjiaxing","")
    val pstmt = conn.prepareStatement("select empno, ename, sal from emp")
    val rs = pstmt.executeQuery()

    while (rs.next()) {
      println("empno:" + rs.getInt("empno") +
        " , ename:" + rs.getString("ename") +
        " , sal:" + rs.getDouble("sal"))
    }

    rs.close()
    pstmt.close()
    conn.close()

  }
}
