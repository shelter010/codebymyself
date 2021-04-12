package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/3 下午10:29  
 * @Version 1.0
 */
object CreateRddDemo {
  def main(args: Array[String]): Unit = {
    /**
     * 得到RDD
     * 1 从数据源  a 外部数据源 文件数据库 hive
     * 2 从scala集合得到
     */
    //得到sparkcontext
    val conf = new SparkConf().setMaster("local[2]").setAppName("createRdd")
    val sc = new SparkContext(conf)
    //创建RDD 从scala集合中得到RDD
    val list = Array(30, 50, 70, 60, 10, 20)
    //val rdd = sc.parallelize(list)
    //或者这种方法
    val rdd = sc.makeRDD(list)
    //转换

    //行动suanzi
    val result = rdd.collect()
    result.foreach(println)

    //关闭SparkContext
    sc.stop()


  }
}
