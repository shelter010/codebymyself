package com.atguigu.spark1015.day04

import com.twitter.chill.Kryo
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author hpf
 * @create 2021/2/18 上午7:26  
 * @Version 1.0
 */
object SerDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      //更换序列化器
      .set("spark.serializer",classOf[Kryo].getName)
      //更换那些需要用kryo序列化的类
      .registerKryoClasses(Array(classOf[Searcher]))
      .setMaster("local[2]").setAppName("Action5")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array("hello word", "hello hpf", "atguigu", "haha"), 2)
    //在rdd中找出包含query子字符串的元素

    val searcher = new Searcher("hello")
    val res = searcher.getMatchRdd1(rdd1)
    res.collect.foreach(println)

    sc.stop()
  }
}


class Searcher(val query:String) extends Serializable {
  //判断s中是否包括字符串
  def isMatch(s:String) = {
    s.contains(query)
  }

  def getMatchRdd1(rdd:RDD[String]) ={
    rdd.filter(isMatch)
  }
}