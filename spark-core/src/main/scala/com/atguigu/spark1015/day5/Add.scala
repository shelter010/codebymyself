package com.atguigu.spark1015.day5

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/20 下午9:58  
 * @Version 1.0
 */
object Add {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Add")
    val sc = new SparkContext(conf)
    val list = List(1,2,3,4,5,6,7)
    val rdd1 = sc.parallelize(list,2)

    //先注册自定义的累加器
    val acc = new MyAcc
    sc.register(acc,"first")

    //加入累加器
   // val acc = sc.longAccumulator   这个是自带的
    val rdd2 = rdd1.map(x => {
      acc.add(1)
      x
    })
    rdd2.collect()
    println(acc.value)
  }
}

class MyAcc extends AccumulatorV2[Int,Int] {

  private var sum = 0
  //判0 对缓冲区的值进行判0
  override def isZero: Boolean = sum==0

  //把当前的累加器复制为1一个1新的累加器
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyAcc
    acc.sum = sum
    acc
  }

  override def reset(): Unit = sum = 0
//分区内的累加
  override def add(v: Int): Unit = sum+=v

  //合并累加器的值
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case acc: MyAcc => this.sum += acc.sum
      case _ => this.sum += 0
    }
  }

  //返回最终的累加器的值
  override def value: Int = sum
}
