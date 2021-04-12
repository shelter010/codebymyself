package com.atguigu.spark1015.day5

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/23 下午9:54  
 * @Version 1.0
 */
object MyPartitionerDemo {
  //利用自定义的分区器进行排序
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("MyDemo")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((12, "a"), (23, "b"), (30, "c"), (40, "d"), (50, "e"), (60, "f")),
      3)

    //使用自定义的分区器进行分区处理
    val rdd2 = rdd1.partitionBy(new MyPartitioner(5))

    val rdd3 = rdd2.mapPartitionsWithIndex((index, it) => it.map(x => (index, x._1 + ":" + x._2)))

    println(rdd3.collect.mkString(","))
  }
}

class MyPartitioner(nums: Int) extends Partitioner {
  override def numPartitions: Int = nums

  override def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => key.hashCode().abs % nums
  }
}
