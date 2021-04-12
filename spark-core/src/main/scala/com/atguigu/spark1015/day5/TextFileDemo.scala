package com.atguigu.spark1015.day5

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/18 下午10:35  
 * @Version 1.0
 */
object TextFileDemo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val conf = new SparkConf().setMaster("local[2]").setAppName("textFileDemo")
      .set("fs.defaultFS","hdfs://master:9000")

    val sc = new SparkContext(conf)
    sc.parallelize("hello"::"word"::"hello word"::Nil)
      .flatMap(_.split("\\W"))
      .map((_,1))
      .reduceByKey(_+_)
      .saveAsTextFile("/word1016")
  }
}
