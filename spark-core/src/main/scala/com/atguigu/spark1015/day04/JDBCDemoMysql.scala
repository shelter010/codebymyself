package com.atguigu.spark1015.day04

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/23 下午11:24  
 * @Version 1.0
 */
object JDBCDemoMysql {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCMysql").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //一些基础的准备
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://master:3306/mydb666"
    val usrName = "root"
    val passWd ="123456"

    //new 一个 jdbcRdd
    val rdd2 = new JdbcRDD(
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, usrName, passWd)
      },
      "select id,name from hpf1 where id>=? and id<=?",
      1,
      20,
      2,
      result => (result.getInt(1), result.getString(2))
    )
    rdd2.collect.foreach(println)
  }
}
