package com.hpf.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/4/18 下午11:53  
 * @Version 1.0
 */
object SparkHiveT {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("aa")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    System.setProperty("HADOOP_USER_NAME","root")
    //查询hive表的形式 形成数据源 --这是sparkSession的第三种方式
//    spark.sql("select * from user").show()
    spark.sql("show tables").show()
    //以下操作必须设置hdfs操作的用户名
    spark.sql("create table user3(id int,name String)")
    spark.sql("insert into user3 values(2,'lisi')")
    spark.sql("select * from user3").show()
    spark.close()
  }
}
