package com.atguigu.spark.sql.day02

import org.apache.spark.sql.SparkSession

/**ouse
 * @author hpf
 * @create 2021/2/27 下午2:49  
 * @Version 1.0
 */
object HIveWrite2 {
  //需要设置
  System.setProperty("HADOOP_USER_NAME","root")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HiveWrite2")
      .master("local[*]")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "hdfs://master:9000/user/hive/warehouse")
      .getOrCreate()

    //创建部分
    spark.sql("create database spark1017").show()
    spark.sql("use spark1017").show()
    spark.sql("create table person123(name string,age int)").show()
    spark.sql("insert into person123 values('dabai',13)").show()

    spark.close()
  }
}
