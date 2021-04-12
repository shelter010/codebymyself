package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/6 下午6:22  
 * @Version 1.0
 */
object zipDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("ZipDemo")
    val sc = new SparkContext(conf)
    val list1 = List(30, 40, 70, 20, 11, 33, 66, 88)
    val list2 = List(30, 4, 70, 2, 1, 3)
    val rdd1 = sc.parallelize(list1, 2)
    val rdd2 = sc.parallelize(list2, 2)

    //拉链1  对应的分区的元素的个数应该是一样 2 分区数也要一样
    //总结1 总的1元素个数相等 2 分区数相等
  //  val rdd3 = rdd1.zip(rdd2)

    //下面这种就是把能进行拉链的拉上
    val rdd3 = rdd1.zipPartitions(rdd2)((it1, it2) => {
      it1.zip(it2)
    })

    rdd3.collect().foreach(println)

    sc.stop()

  }
}
