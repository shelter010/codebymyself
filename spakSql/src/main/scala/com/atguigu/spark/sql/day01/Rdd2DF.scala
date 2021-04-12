package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/26 上午6:59  
 * @Version 1.0
 */
object Rdd2DF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("Rdd2DF").getOrCreate()
     import spark.implicits._
    val sc = spark.sparkContext
//    val rdd = sc.parallelize(1 to 10)
//    rdd.toDF("nums").show()
val rdd1 = sc.parallelize(Array(User("lisi", 11), User("zhangsan", 22)))
  rdd1.toDF().show()

    spark.stop()
  }
}

case class User(name: String, age: Int)