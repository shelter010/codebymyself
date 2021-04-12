package com.atguigu.spark.sql.day01.udf

import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author hpf
 * @create 2021/2/27 上午9:06  
 * @Version 1.0
 */
case class Dog(name:String,age:Int)
case class AvgBuffer(sum:Int,count:Int){
  def avg(): Double ={
    sum.toDouble / count

  }
}
object UDAFDemo2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDAFDemo2").master("local[2]")
      .getOrCreate()
    import spark.implicits._
    val ds = List(Dog("daHuang", 11), Dog("zhongHuang", 15),Dog("daBai",22)).toDS()
    //强类型使用方式 需要将行转换为列
    val avg = new MyAvg2().toColumn.name("avg")
    val res = ds.select(avg)
    res.show()

    spark.close()
  }
}

class MyAvg2 extends Aggregator[Dog,AvgBuffer,Double] {
  //对缓存区进行初始化
  override def zero: AvgBuffer = AvgBuffer(0,0)
//聚合 分区内1聚合
  override def reduce(b: AvgBuffer, a: Dog): AvgBuffer = a match{
      //这里使用的样例类 是直接进行封装的
      //如果是dog对象 则把年龄相加 个数加上1
    case Dog(name,age) => AvgBuffer(b.sum+a.age,b.count+1)
      //如果是null 则原封不动返回
    case _ => b
  }
//分区间聚合
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    AvgBuffer(b1.sum+b2.sum,b1.count+b2.count)
  }
//返回的最终的值
  override def finish(reduction: AvgBuffer): Double = reduction.avg
//对缓存区进行编码
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product //如果是样例类 直接返回这个编码器就可以
//对返回值进行编码
  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
