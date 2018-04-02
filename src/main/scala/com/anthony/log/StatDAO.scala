package com.anthony.log

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ListBuffer

/**
  * @ Description: 各各维度统计的DAO操作
  * @ Date: Created in 16:09 2018/4/2
  * @ Author: Anthony_Duan
  */
object StatDAO {

  def inertDayVideoAccessTopN(list:ListBuffer[DayVideoAccessStat])={

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false)

      val sql = "insert into day_video_access_topn_stat(day,cms_id,times) values(?,?,?)"
      pstmt = connection.prepareStatement(sql)

      //每一个els就是一个实体
      for (ele<-list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.times)
        pstmt.addBatch()//添加到批次中
      }

      pstmt.executeBatch()//执行批量处理

      connection.commit()//手工提交
    }catch {
      case e:Exception =>e.printStackTrace()
    }finally {
      MySQLUtils.release(connection,pstmt)
    }

  }


  /**
    * 批量保存DayCityVideoAccessTopN到数据库
    */
  def insertDayCityVideoAccessTopN(list:ListBuffer[DayCityVideoAccessStat]): Unit ={
    var connection:Connection = null
    var pstmt:PreparedStatement = null
    try{
      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false)

      //这里需要用 ignore  否则会出现主键重复的错误
      val sql = "insert ignore into day_video_city_access_topn_stat (day,cms_id,city,times,times_rank) values (?,?,?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele <- list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setString(3,ele.city)
        pstmt.setLong(4,ele.times)
        pstmt.setInt(5,ele.times_rank)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()
    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MySQLUtils.release(connection,pstmt)
    }
  }



  def insertDayVideoTrafficsAccessTopN(list:ListBuffer[DayVideoTrafficsStat])={

    var connection:Connection =null
    var pstmt:PreparedStatement = null
    try{

      connection = MySQLUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into day_video_traffics_topn_stat(day,cms_id,traffics) values (?,?,?)"
      pstmt = connection.prepareStatement(sql)
      for (ele<-list){
        pstmt.setString(1,ele.day)
        pstmt.setLong(2,ele.cmsId)
        pstmt.setLong(3,ele.traffics)
        pstmt.addBatch()
      }

      pstmt.executeBatch()
      connection.commit()

    }catch {
      case e:Exception=>e.printStackTrace()
    }finally {
      MySQLUtils.release(connection,pstmt)
    }

  }

  def deleteData(day:String)={
    val tables = Array("day_video_access_topn_stat",
      "day_video_city_access_topn_stat","day_video_traffics_topn_stat")

    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{

      connection = MySQLUtils.getConnection()
      for(table<-tables){
//        这里是删除某一天的数据
        val deleteSQL = s"delete from $table where day =?"
        pstmt = connection.prepareStatement(deleteSQL)
        pstmt.setString(1,day)
        pstmt.executeUpdate()
      }

    }catch {
      case e:Exception => e.printStackTrace()
    }finally {
      MySQLUtils.release(connection,pstmt)
    }
  }


}
