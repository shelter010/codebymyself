package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/27 下午2:14  
 * @Version 1.0
 */
object HiveWrite {
  def main(args: Array[String]): Unit = {
//需要加上这行 更换一下owner
    System.setProperty("HADOOP_USER_NAME","root")

    val spark = SparkSession
      .builder()
      .appName("HiveWrite")
      .master("local[*]")
      .enableHiveSupport()
      //定义数仓的位置   必须添加
      .config("spark.sql.warehouse.dir","hdfs://master:9000/user/hive/warehouse")
      .getOrCreate()

    import spark.implicits._
    //换一个数据库
//    spark.sql("create database  spark1015").show()
//    spark.sql("use spark1015").show()
//   spark.sql("create table user13579(id int,name string)").show()
//   spark.sql("insert into user13579 values(10,'lisi')").show()

    //将其他数据写入到hive中
    val df = spark.read.json("/Users/mac/Downloads/download from mac/download from baiduwangpan/15-spark/spark-sql数据" +
      "/user.json")
    spark.sql("use spark1015")
    //直接把数据写入到hive中 表可以存在也可以不存在
   // df.write.mode("append").saveAsTable("user222")
   //
    df.write.insertInto("user222")
    spark.close()
  }
}
