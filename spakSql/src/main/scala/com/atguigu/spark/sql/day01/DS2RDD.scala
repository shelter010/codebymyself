package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/26 上午8:00  
 * @Version 1.0
 */
object DS2RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("qa").master("local[2]").getOrCreate()
    import spark.implicits._

    val ds = List(User("dabai", 11), User("rabbit", 15), User("monkey", 13)).toDS()
    //ds转rdd
    val rdd = ds.rdd
    rdd.collect.foreach(println)
    spark.close()
  }
}
