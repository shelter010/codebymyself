package com.com.atguigu.test

import com.com.atguigu.test.Test4.array

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * @author hpf
 * @create 2021/6/18 下午6:01  
 * @Version 1.0
 */
object Test4 {
  val array = Array(Array[String]("1", "2", "3", "4"), Array[String]("a", "b", "c"), Array[String]("A", "B", "C", "D", "E"))
  val maxSize123 = maxSize(array)

  def main(args: Array[String]): Unit = {
    //定义1个二维数组 长度不够的话 补0处理
    //1 求出maxsize 也就是内部的一维数组的最大长度

    //2 maxsize和其他数组长度做减法 diff
    //2-1 如果长度长度 不处理

    //2-2 如果不想等 diff-size 补0处理 string类型的
    //  println(maxSize(array)) 正确

    // println(sameSizeArr(array))

  }
  def arrRes(array: Array[Array[String]]) :ArrayBuffer[mutable.Buffer[String]] = {
    var arrBuff1 = arr2ArrBuff(array)
    var res = sameSizeArrBuff(arrBuff1)
    res
  }


  def maxSize(arr: Array[Array[String]]): Int = {
    // 遍历出每个1维数组 求出size
    // 和下一个数组长度比较 把较大的值付给变量 temp_value
    var temp_value = 0

    for (i <- 0 until arr.size) {
      if (temp_value < arr(i).size) {
        temp_value = arr(i).size
      } else temp_value
    }

    temp_value
  }

  def arr2ArrBuff(array: Array[Array[String]]): ArrayBuffer[mutable.Buffer[String]] = {
    // 首先将 array 转换为 arraybuffer
    var arrayBuffer = ArrayBuffer[mutable.Buffer[String]]()
    val size = array.size

    for (i <- 0 until size) {
      var buffer = array(i).toBuffer
      arrayBuffer.+=(buffer)
    }
    arrayBuffer
    //ran后调用 maxSize方法 做减法 求去diff

    //然后循环 每个补0

    // 最后输出
  }

  def sameSizeArrBuff(arrBuff: ArrayBuffer[mutable.Buffer[String]]): ArrayBuffer[mutable.Buffer[String]] = {

    var diff = 0

    for (arrBuf <- arrBuff) {
      if (arrBuf.size == maxSize123) {

      } else {
        val diff = maxSize123 - arrBuf.size
        for (i <- 0 until diff) {
          arrBuf.append("0")
        }
      }
    }
    arrBuff

  }

}
