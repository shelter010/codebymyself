//package com.hpf.sparksql
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.expressions.Aggregator
//import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
//
///**
// * @author hpf
// * @create 2021/4/17 下午1:43
// * @Version 1.0
// */
//case class User(age: Long, name: String)
//
//case class Buff(var sum: Long, var cnt: Long)
//
//object UDFTest {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setMaster("local[*]").setAppName("aaa")
//    val spark = SparkSession.builder().config(conf).getOrCreate()
//    //通过数据源创建df
//    val df = spark.read.json("/Users/mac/Users/hpf/sparkStudy/spark1015/spakSql/input/user.json")
//    //注册额udf函数 功能 在数据前加上字符串"name"
//    //    spark.udf.register("addName",(x:String)=>"name:"+x)
//    //    df.createOrReplaceTempView("user")
//    //    spark.sql("select addName(name) name,age from user").show()
//
//    //自定义聚合函数 实现求平均值
//    spark.udf.register("MyAvg",new MyAvg())
//    df.createOrReplaceTempView("user")
//    spark.sql("select MyAvg(age) from user").show()
//    //调用自定义聚合函数 uda
//    spark.stop()
//
//  }
//}
//
//class MyAvg() extends Aggregator[User, Buff, Double] {
//  override def zero: Buff = {
//    Buff(0L, 0L)
//  }
//
//  override def reduce(b: Buff, a: User): Buff = {
//    b.sum = b.sum + a.age
//    b.cnt = b.cnt + 1L
//    b
//  }
//
//  override def merge(b1: Buff, b2: Buff): Buff = {
//    b1.sum = b1.sum+b2.sum
//    b1.cnt = b1.cnt+b2.cnt
//    b1
//  }
//
//  override def finish(reduction: Buff): Double = {
//     reduction.sum.toDouble / reduction.cnt
//  }
//
//  override def bufferEncoder: Encoder[Buff] = Encoders.product
//
//  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
//}
