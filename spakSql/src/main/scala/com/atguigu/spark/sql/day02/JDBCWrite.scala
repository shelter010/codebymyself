package com.atguigu.spark.sql.day02

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author hpf
 * @create 2021/2/27 上午11:37  
 * @Version 1.0
 */
object JDBCWrite {
  val url = "jdbc:mysql://master:3306/mydb666"
  val user = "root"
  val pw = "123456"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("JDBCWrite")
      .getOrCreate()

    import spark.implicits._
    //写到 jdbc
    val df = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据" +
      "/user.json")
//    df.write
//      .format("jdbc")
//      .option("url", url)
//      .option("user", user)
//      .option("password", pw)
//      //表 如 没有的话 可以自动创建
//      .option("dbtable", "user1015")
//      //.mode("append") 和下面等价
//      .mode(SaveMode.Overwrite)
//      .save()
val pros = new Properties()
    pros.put("user",user)
    pros.put("password",pw)
    df.write
        .jdbc(url,"user1016",pros)

    spark.close()


  }
}
