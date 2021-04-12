package com.atguigu.spark1015

import java.sql.{DriverManager, ResultSet}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/19 上午6:59  
 * @Version 1.0
 */
object DbcRead {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DbcDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val url = "jdbc:mysql://master:3306/mydb666"
    val user = "root"
    val passwd = "123456"
    //从mysql获取连接并读取
    val rdd1 = new JdbcRDD[String](
      sc,
      ()=>{
        //获取mysql驱动
        // Class.forName("com.mysql.jdbc.Driver")
        Class.forName("com.mysql.jdbc.Driver")
        //建立连接
        DriverManager.getConnection(url,user,passwd)
      },
      "select * from hpf1 where id>=? and id <=?",
      1,
      10,
      2,
      (resultSet:ResultSet) => resultSet.getString(2)
    )
    println(rdd1.count())
    rdd1.collect().foreach(println)
    sc.stop()
  }
}
