package com.atguigu.spark1015.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author hpf
 * @create 2021/4/14 下午5:17  
 * @Version 1.0
 */
object SortByKeyT {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //sortByKey 如果key为自定义类型 譬如case class 那么需要混入特质
    val stdList: List[(Student, Int)] = List(
      (new Student("jingjing", 18), 2),
      (new Student("bangzhang", 18), 2),
      (new Student("jingjing", 20), 2),
      (new Student("zhangfei", 18), 3),
      (new Student("jingjing", 19), 2),
      (new Student("banzhang", 15), 5)
    )

    val rdd1: RDD[(Student, Int)] = sc.makeRDD(stdList)
    rdd1.sortByKey().collect().foreach(println)






    //4.关闭连接
    sc.stop()

  }
}

class Student(val name: String, val age: Int) extends Ordered[Student] with Serializable{


  override def toString: String = name+"=="+age

  override def compare(that: Student): Int = {
    //先按照name排序 再按照年龄排序
    var res = this.name.compareTo(that.name)
    if (res!=0) res else this.age.compareTo(that.age)
  }
}


