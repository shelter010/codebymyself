package com.atguigu.spark1015.day02

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/2/4 下午10:53  
 * @Version 1.0
 */
case class User(name:String,age:Int){
  override def hashCode(): Int = this.age

  override def equals(obj: Any): Boolean = obj match {
    case User(_,age) => this.age == age
    case _ => false
  }
}
object DistinctDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //val list = List(11, 2 2, 33, 44, 55, 66,55,67,767,78,7)
//    val list = List(11, 22, 33, 44, 11, 22, 45, 54, 22, 33, 45)
//    val rdd1 = sc.parallelize(list, 2)
//    //distinct 去重算子
//    val rdd2 = rdd1.distinct()  //去重后 顺序不一样
//    rdd2.collect().foreach(println)

    //不仅仅是针对的数组 针对对象的情况
    //复写 user的 hashcode he equal fang fa ; 然后年龄相同的 就被distinct了
    val rdd1 = sc.parallelize(List(User("lisi", 22), User("zhansan", 18), User("ab", 22)))
    implicit val ord:Ordering[User] = new Ordering[User]{
      override def compare(x: User, y: User): Int = x.age - y.age
    }
    val rdd2 = rdd1.distinct(2)(ord)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
