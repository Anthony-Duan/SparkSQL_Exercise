package com.anthony.log

import com.ggstar.util.ip.IpHelper
/**
  * @ Description:
  * @ Date: Created in 14:55 2018/3/31
  * @ Author: Anthony_Duan
  */
object IpUtils {


  def getCity(ip:String) = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]) {
    println(getCity("218.75.35.226"))

  }

}

