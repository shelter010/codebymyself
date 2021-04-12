package com.atguigu.spark.sql.day02

import java.util.Properties

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/27 上午11:23  
 * @Version 1.0
 */
object JDBCRead {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName( "JDBCRead")
      .getOrCreate()

    //通用的写法
    val url = "jdbc:mysql://master:3306/mydb666"
    val user = "root"
    val pw = "123456"
//    val df = spark.read
//      .option("url", url)
//      .option("user", user)
//      .option("password", pw)
//      .option("dbtable", "hpf1")
//      .format("jdbc")
//      .load()

    //专用的写法
    val pros = new Properties()
    pros.put("user",user)
    pros.put("password",pw)
    val df = spark.read
      .jdbc(url, "hpf1", pros)

    df.show( )
    spark.close()

  }
}
