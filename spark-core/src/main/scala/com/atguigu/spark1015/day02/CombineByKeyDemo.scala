package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/8 下午10:31  
 * @Version 1.0
 */
object CombineByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("B_Demo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    // 1  求解每个key的value的和  需要用到CombineByKey
    //    val rdd2 = rdd1.combineByKey(
    //      v => v,
    //      (c: Int, v: Int) => c + v,
    //      (c1: Int, c2: Int) => c1 + c2
    //    )
    //2 求解每个key在每个分区内的最大值 然后再求出最大值的和
//    val rdd2 = rdd1.combineByKey(
//      v => v,
//      (c: Int, v: Int) => c.max(v),
//      (c1: Int, c2: Int)   => c1 + c2
//    )
    //简化的版本是
    /**
     * rdd11.combineByKey(
     *    +_,
     *    (_:Int).max(_:Int)
     *    (_:Int) + (_:Int)
     * )
     */

    //求每个key平均值
//    rdd1.combineByKey()
//    rdd2.collect().foreach(println)
//    sc.stop()

  }
}
