package com.atguigu.spark1015.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 下午2:55  
 * @Version 1.0
 */
object Test222 {
  def main(args: Array[String]): Unit = {
    //测试reduceBykey求平均值
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
    //实现求平均值
    //3，1先map 在reduceByKey 再map
    rdd
      .map(x => (x._1, (x._2, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .map(x => (x._1, x._2._1 / x._2._2))
      .collect().foreach(println)

    val rdd2 = rdd.groupByKey()
    //求和
        .map(t =>(t._1,t._2.sum))
    rdd2.collect().foreach(println)
    //求平均数
    println("dadafafafafa")
    rdd.groupByKey()
        .map(data =>(data._1,(data._2.sum,data._2.size)))
        .map(data => (data._1,data._2._1 / data._2._2))
        .collect().foreach(println)



    //4.关闭连接
    sc.stop()

  }
}
