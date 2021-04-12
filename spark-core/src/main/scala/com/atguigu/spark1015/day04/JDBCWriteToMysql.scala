package com.atguigu.spark1015.day04

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/23 下午11:35  
 * @Version 1.0
 */
object JDBCWriteToMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ha")
    val sc = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://master:3306/mydb666"
     val usr = "root"
    val passWsd = "123456"
    //写入数据到mysql中
    val rdd1 = sc.parallelize(Array((110, "liBai"), (120, "daQiao"), (212, "HouZi")), 2)

    //用foreachPartition
    rdd1.foreachPartition(it =>{
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, usr, passWsd)
      val sqlDemo = "insert into hpf1 values(?,?) "
      it.foreach( x =>{
        val statement = connection.prepareStatement(sqlDemo)
        statement.setInt(1,x._1)
        statement.setString(2,x._2)
        statement.executeUpdate()
      }
      )
    })


  }
}
