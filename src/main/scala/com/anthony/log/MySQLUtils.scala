package com.anthony.log

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * @ Description:
  * @ Date: Created in 14:06 2018/4/2
  * @ Author: Anthony_Duan
  */
object MySQLUtils {

  def getConnection() ={
    //这两种方式效果相同  使用前需要在pom.xml中添加木MySQL的依赖
    //    DriverManager.getConnection("jdbc:mysql://192.localhost:3306/imooc_project","root","xiaoduan")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc_project?user=root&password=xiaoduan")
  }

  def release(connection:Connection,pstmt:PreparedStatement)={

    try {
      if (pstmt!=null){
        connection.close()
      }
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      if (connection!=null){
        connection.close()
      }
    }

  }

  def main(args: Array[String]): Unit = {
    println(getConnection())
  }
}
