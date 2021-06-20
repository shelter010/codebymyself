package com.atguigu.spark1015.day01

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/16 下午5:51  
 * @Version 1.0
 */
object top3T {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)


    val rdd = sc.makeRDD(Array("a", "b", "c", "d","sd","sf"), 2)
    rdd.glom().foreach(x => println(x.toList.mkString))

    rdd.map(data => {
      val splits = data.split(",")
      (splits(0), 1)
    })
      // .reduceByKey(_+_)
      //        .aggregateByKey(0)(_+_,_+_)
      .foldByKey(0)(_ + _)

    val acc = new Myacc123()
    sc.register(acc)
    rdd.foreach(data => {
      acc.add(data)
    })

    println(acc.value)
    //4.关闭连接
    sc.stop()

  }
}

class Myacc123() extends AccumulatorV2[String, Int] {
  var sum = 0

  override def isZero: Boolean = true

  override def copy(): AccumulatorV2[String, Int] = new Myacc123

  override def reset(): Unit = 0

  override def add(v: String): Unit = {
    if (v != null) {
      sum += 1
    }
  }

  override def merge(other: AccumulatorV2[String, Int]): Unit = {
    sum = other.value+sum
  }

  override def value: Int = sum
}
