package com.atguigu.spark1015.day04

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/18 下午9:07  
 * @Version 1.0
 */
object PartitionerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("partitionDemo")
    val sc = new SparkContext(conf)
    val list = List(30, 40, 50, 60, 70, 80, null, null,'a','b','c')
    val rdd1 = sc.parallelize(list, 4).map((_, 1))
    val rdd2 = rdd1.partitionBy(new MyPartitioner(2))
    rdd2.glom().map(_.toList).collect().foreach(println)
    sc.stop()
  }
}

//自定义的hash分区器
class MyPartitioner(num: Int) extends Partitioner {
  //if (num <= 0)  throw new Exception  或者可以直接使用下面1这个
  assert(num > 0)

  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode().abs % num
  }
}
