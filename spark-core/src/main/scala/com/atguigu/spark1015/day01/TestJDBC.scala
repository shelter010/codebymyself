package com.atguigu.spark1015.day01

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/4/16 下午3:48  
 * @Version 1.0
 */
object TestJDBC {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    //需求：自定义累加器，统计RDD中首字母为“H”的单词以及出现的次数
    val rdd = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))
    //定义累加器
    val acc = new MyAcc3()
    sc.register(acc, "wordcount")

    rdd.foreach(word => {
      acc.add(word)
    })

    println(acc.value)
    //4.关闭连接
    sc.stop()

  }
}

class MyAcc3() extends AccumulatorV2[String, mutable.Map[String, Int]] {
  //定义输出数据的集合
  var map = mutable.Map[String, Int]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAcc3()

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    //业务逻辑
    if (v.startsWith("H")) {
      map(v) = map.getOrElse(v, 0) + 1
    }
    //    if (v.startsWith("S")){
    //      map(v) = map.getOrElse(v, 0) + 1
    //    }
  }

  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    var map1 = map
    var map2 = other.value
    map = map1.foldLeft(map2)(
      (map, kv) => {
        map(kv._1) = map.getOrElse(kv._1, 0) + kv._2
        map
      }
    )
  }

  override def value: mutable.Map[String, Int] = map
}
