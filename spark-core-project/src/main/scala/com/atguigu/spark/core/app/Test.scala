package com.atguigu.spark.core.app

import java.text.DecimalFormat

/**
 * @author hpf
 * @create 2021/2/25 下午9:29  
 * @Version 1.0
 */
object Test {
  def main(args: Array[String]): Unit = {
    val f = new DecimalFormat("00.00%")
    println(f.format(math.Pi))
  }
}
