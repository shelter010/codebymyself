package com.atguigu.spark1015.day5

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

/**
 * @author hpf
 * @create 2021/2/20 下午11:18  
 * @Version 1.0
 */

object MyAcc2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Myacc2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List(30, 50, 20, 10, 80)

    val rdd1 = sc.parallelize(list,2)
    val acc = new MyAcc2
    sc.register(acc)

    rdd1.foreach(x => acc.add(x))
    println(acc.value)

    sc.stop()
  }
}

//将来的累加器同时包含 sum count avg
class MyAcc2 extends AccumulatorV2[Double,Map[String,Any]]{
  private var map: Map[String, Any] = Map[String, Any]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[Double, Map[String, Any]] = {
    val acc = new MyAcc2
    acc.map = map
    acc
  }

  //不可变1集合 直接赋值一个空的集合
  override def reset(): Unit = map = Map[String,Any]()

  override def add(v: Double): Unit = {
    //对sum和count进行累加 avg在最后的value里面进行计算
    map += "sum" -> (map.getOrElse("sum",0D).asInstanceOf[Double] + v)
    map += "count" -> (map.getOrElse("count",0L).asInstanceOf[Long] + 1L)
  }

  override def merge(other: AccumulatorV2[Double, Map[String, Any]]): Unit = {
    //合并2个map
    other match {
      case o:MyAcc2 =>
        map += "sum" ->(map.getOrElse("sum",0D).asInstanceOf[Double] + o.map.getOrElse("sum",0D).asInstanceOf[Double])
        map += "count" ->(map.getOrElse("count",0L).asInstanceOf[Long] + o.map.getOrElse("count",0L).asInstanceOf[Long])
      case _ => throw new UnsupportedOperationException
    }
  }

  override def value: Map[String, Any] = {
    map += "avg" -> (map.getOrElse("sum",0D).asInstanceOf[Double] / map.getOrElse("count",0L).asInstanceOf[Long])
    map
  }
}
