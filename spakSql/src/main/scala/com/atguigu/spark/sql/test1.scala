package com.atguigu.spark.sql

/**
 * @author hpf
 * @create 2021/6/17 下午5:43  
 * @Version 1.0
 */
object test1 {
  //使用implicit关键字声明的函数叫做 隐式函数
  implicit def convert(arg: Int): MyRichInt = {
    new MyRichInt(arg)
  }

  //隐式参数
  implicit val str: String = "hello world"

  def main(args: Array[String]): Unit = {
    //测试隐式转换
    //  println(2.myMin(6))
    hello

  }

  def hello(implicit arg:String ="good monring"):Unit={
    println(arg)
  }
}

class MyRichInt(val value: Int) {
  def myMax(i: Int): Int = {
    if (value < i) i else value
  }

  def myMin(i: Int): Int = {
    if (value < i) value else i
  }
}
