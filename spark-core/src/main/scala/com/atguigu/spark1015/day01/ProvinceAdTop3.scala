package com.atguigu.spark1015.day01

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 下午6:11
 * @Version 1.0
 */
object ProvinceAdTop3 {
  def main(args: Array[String]): Unit = {
    //System.setProperty("HAROOP_USER_NAME","root")
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("Spardaddad").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //求省份广告的top3 拿到数据是 时间戳 省份 城市 user 浏览的广告分割
    //读取数据
    val dataRdd = sc.textFile("/Users/mac/Downloads/personal/agent.log")
    val transRdd = dataRdd.map(data => {
      val arr = data.split(" ")
      (arr(1) + "=" + arr(4), 1)
    })

    val resRdd = transRdd.reduceByKey(_ + _)
      .map(data => {
        val splits = data._1.split("=")
        (splits(0), (splits(1), data._2))
      })
      .groupByKey()
      .mapValues(data => {
        data.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      })

    //.take(3)

    resRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()

  }
}
