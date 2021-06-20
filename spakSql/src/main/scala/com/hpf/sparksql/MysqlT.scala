package com.hpf.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/4/17 下午3:50  
 * @Version 1.0
 */
object MysqlT {
  def main(args: Array[String]): Unit = {
    //读取mysql数据
    val con = new SparkConf().setMaster("local[*]").setAppName("ada")
    val spark = SparkSession.builder().config(con).getOrCreate()
     //从数据源读取
     val df = spark.read.format("jdbc")
       .option("url", "jdbc:mysql://master:3306/gmall")
       .option("driver", "com.mysql.jdbc.Driver")
       .option("user", "root")
       .option("password", "123456")
       .option("dbtable", "coupon_info")
       .load()

    //创建视图
    df.createOrReplaceTempView("user")
    spark.sql("select id,coupon_name from user where id = 1").show()


    //写数据
    
    spark.close()
  }
}
