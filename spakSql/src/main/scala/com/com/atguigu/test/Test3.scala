package com.com.atguigu.test

import scala.collection.mutable.ArrayBuffer
import com.com.atguigu.test.Test4._

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/6/17 下午6:45  
 * @Version 1.0
 */
object Test3 {
  def main(args: Array[String]): Unit = {
    //输入一个二维数组，求蛇形输出结果。
    // 例如[[1,2,3,4], [a,b,c], [A,B,C,D,E]]，
    // 蛇形输出即输出[1,a,A,B,b,2,3,c,C,D,4,E]
    //    /**
    //     * [1,2,3,4,0]       0 1 2 3
    //     * [a,b,c,0,0]         0 1 2
    //     * [A,B,C,D,E]     0 1 2 3 4
    //     */
    val arr = Array(Array[String]("1", "2", "3", "4"), Array[String]("a", "b", "c"), Array[String]("A", "B", "C", "D", "E"))

    def snake(arr: Array[Array[String]]): ArrayBuffer[String] = {
      //arr.foreach(x=>println(x.mkString(",")))
      val arrBuff = ArrayBuffer[String]()
      var temp = 0

      //调用 test4
      val res = arrRes(arr)

      val maxSize11 = maxSize(arr)

      //code部分
      // 定义一个arraybuffer
      var arBuf = ArrayBuffer[String]()

      //假如maxsize11是偶数长度 是下面代码
      if(maxSize11 % 2 == 0){
        for (i <- 0 until maxSize11 - 1 by 2) {
          for (buf <- res) {
            arBuf.append(buf(i))
          }
          for (buf1 <- res.reverse) {
            arBuf.append(buf1(i + 1))
          }
        }
      }else{
        //如果是奇数字 最后一组 没法跑出来 那么可以另外计算
        for (i <- 0 until maxSize11 - 1 by 2) {
          for (buf <- res) {
            arBuf.append(buf(i))
          }
          for (buf1 <- res.reverse) {
            arBuf.append(buf1(i + 1))
          }
        }

        for(bu123 <- res){
          arBuf.append(bu123(maxSize11-1))
        }
      }




     // println(arBuf)

      //          for (i <- 0 to maxSize11 - 1 by 2 if i <= maxSize11 - 1) {
      //
      //            for (arr123 <- res) {
      //              arrBuff.append(arr123(i))
      //            }
      //            for (arr321 <- res.reverse) {
      //              arrBuff.append(arr321(i + 1))
      //            }
      //          }

      //arrBuff
      //println(res) 测试打印 ok
      arBuf
    }

    var buffer = snake(arr)

    //再删除元素是0的部分
    val strings = buffer.filter(!_.eq("0"))

    println(strings)

  }

}

