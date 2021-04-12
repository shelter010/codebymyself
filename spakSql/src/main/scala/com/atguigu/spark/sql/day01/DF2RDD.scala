package com.atguigu.spark.sql.day01

import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/2/26 上午7:12  
 * @Version 1.0
 */
object DF2RDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("DF2RDD").master("local[2]").getOrCreate()

    import spark.implicits._
    //直接从1个scala集合到ddf
//    val df = (1 to 10).toDF("number")
//    //转rdd rdd中一定存储的是row
//    val rdd = df.rdd
//
//    val rdd1 = rdd.map(
//      row => row.getInt(0)
//    )
//    rdd1.collect.foreach(println)


    spark.close()
  }
}
