package com.atguigu.spark.streaming.day01.kafka

import scala.collection.mutable.ArrayBuffer

/**
 * @author hpf
 * @create 2021/6/16 下午7:29  
 * @Version 1.0
 */
object Test111 {


  def definedFunc(arr1: Array[Int], arr2: Array[Int]): Unit = {

    val arr1 = Array(11, 2, 3, 34, 3)
    val arr2 = Array(34, 3)
    val temp_res = Array[Int]()

    val flag: Boolean = false
    val arr1Size: Int = arr1.size
    val arr2Size: Int = arr2.size
    //1 判断传入2个数组的大小
    val temp1 = Array[Int]()
    val temp2 = Array[Int]()
    //1-1 如果 arr1.size < arr2.size 将arr1和arr2调换【保证arr1 > arr2 】
    //这里temp1是大的数组 temp2是小的数组【 做一个标识】

    if(arr1.size<arr2.size){
      for(i <- arr1.indices){
        arr1(i)+:temp2
      }

      for(i <- arr2.indices){
        arr2(i)+:temp1
      }
    }


    //2 2层for循环 定义一个temp_res数组 表示取出来的arr2的值
    for (i <- temp1.indices){
      for (j <- temp2.indices){
        if (temp2(j)==temp1(i)){
          temp2(j)+:temp_res
        }

      

    }

    //3 比较temp2 是否和 arr2相等 如果相等 那么就是temp1 包含 temp2  定义一个flag表签
    var count:Int = 0
    for(i <- temp_res.indices){
      if (temp_res(i) == temp2(i)){
        count+=1
      }
    }

    //4 count数字和temp2.size对比
    if (count==temp2.size){
      println(temp1(1))
    }

  }

  /**
   * 2个数组 如果 a包含b的话 那么输出a数据的第一个元素
   */

}
