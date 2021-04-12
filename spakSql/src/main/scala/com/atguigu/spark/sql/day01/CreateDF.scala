package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession
//import scala.util.control.Breaks._
/**
 * @author hpf
 * @create 2021/2/25 下午11:11  
 * @Version 1.0
 */
object CreateDF {
  def main(args: Array[String]): Unit = {
    //
    val spark = SparkSession.builder().appName("CreateDF").master("local[2]")
      .getOrCreate()
    //创建df
    val df = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据/user.json")
    //  对df做操作 创建临时表
    df.createOrReplaceTempView("user")
    //查询临时表
   // val df2 = spark.sql("select * from user")
   //或者 这样写
     spark.sql(
       """
         |select
         |name,
         |age
         |from user
         |""".stripMargin).show()

    //关闭SparkSession
    spark.close()
  }
}
