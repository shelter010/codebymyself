package com.com.atguigu.test

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/6/17 下午3:47  
 * @Version 1.0
 */
object test {
  def main(args: Array[String]): Unit = {
    val map = mutable.Map(1 -> "aa", 2 -> "bb", 3 -> "cc")
    //    map+=(4->"dd")
    //    println(map)
    //    val myInt = map.put(5, "ee")
    ////    println(myInt.getOrElse(1))
    //    println(map)
    //    map-=(1)
    //    println(map)
    ////    val map1 = map.updated(5,"eeee")
    //    map.updated(5,"eeee")
    //    println(map)
    //    map.put(7,"77")
    //    map+=((8,"88"),(9,"99"))
    //    println(map)

    val list = List(11, 22, 33, List(66, 55, 44))
    list.map(x => x match {
      case x: Int => println(x)
      case x: List[Int] => x.foreach(println)
    })
    println("==============")

    list match {
      case first :: second :: third :: rest => println(s"$first + $second +$third +$rest")
      case _ => println("something else")
    }

    println("========")
    //对1个元组集合进行遍历
    for (tuple <- Array((0, 1), (1, 0), (1, 1), (1, 0, 2))) {
      val res = tuple match {
        case (0, _) => "0..." //第一个元素是0的元组
        case (y, 0) => "" + y + " 0" //匹配后一个与元素是0的对偶元组
        case (a, b) => "" + a + " " + b
        case _ => "something else" //默认
      }
      println(res)
    }


  }
}
