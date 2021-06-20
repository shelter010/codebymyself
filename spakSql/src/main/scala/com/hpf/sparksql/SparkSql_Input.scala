package com.hpf.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * @author hpf
 * @create 2021/4/17 上午11:48  
 * @Version 1.0
 */
object SparkSql_Input {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("spark_sql_t").setMaster("local[*]")
    //创建sparkSession对象
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //读取数据
    val df = spark.read.json("/Users/mac/Users/hpf/sparkStudy/spark1015/spakSql/input/user.json")
    //可视化
    df.show()
    //释放资源
    spark.stop()

  }
}
