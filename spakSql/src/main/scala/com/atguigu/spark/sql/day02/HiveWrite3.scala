package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/27 下午3:51  
 * @Version 1.0
 */
object HiveWrite3 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("HiveWrite3")
      .enableHiveSupport()
      .getOrCreate()

    //
    val df1 = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据/user.json")

    df1.createOrReplaceTempView("a")

    spark.sql("use spark1015")
    val df2 = spark.sql("select * from a")
    val df3 = spark.sql("select sum(age) sum_age from a group by name")

    df2.write.mode("overwrite").saveAsTable("a1")
   // df3.write.saveAsTable("a2")   这时候存储 是有200个分区数 解决方式是下面的代码
    df3.coalesce(1).write.saveAsTable("a2")
    spark.close()

  }
}
