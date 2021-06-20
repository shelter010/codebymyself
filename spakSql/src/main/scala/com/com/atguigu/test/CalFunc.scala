package com.com.atguigu.test

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/6/17 下午1:02  
 * @Version 1.0
 */

/**
 * array增加元素的方式
 * （4）增加元素（由于创建的是不可变数组，增加元素，其实是产生新的数组）
 * println(arr01)
 * val ints: Array[Int] = arr01 :+ 5
 * println(ints)
 *
 * 可变map增加元素方式
 * //（1）创建可变集合         val map = mutable.Map( "a"->1, "b"->2, "c"->3 )  
 * //（3）向集合增加数据         map.+=("d"->4)  
 * // 将数值4添加到集合，并把集合中原值1返回         val maybeInt: Option[Int] = map.put("a", 4) 
 * println(maybeInt.getOrElse(0))          //（4）删除数据         map.-=("b", "c")  
 * //（5）修改数据         map.update("d",5)
 * map("d") = 5          //（2）打印集合         map.foreach((kv)=>{println(kv)}) 
 */
object CalFunc {
  def main(args: Array[String]): Unit = {


    //1 定义1个不可变数组
    val arr = Array(11, 22, 33, 454, 546, 456, 2)
    val target = 24
    // twoSum(arr,target)

    val a = Array(11, 2, 3, 34, 3)
    val b = Array(34, 2,423)
    //如果为true 输出
    if (calCover(a, b)) {
      println(a(0))
    } else {
      println("b不是a的子集")
    }


  }

  //给定一个整数数组 nums 和一个整数目标值 target，请你在该数组中找出 和为目标值 target
  // 的那 两个 整数，并返回它们的数组下标。
  def twoSum(array: Array[Int], target: Int): Boolean = {
    //2  因为是2数和是目标值 所以可以用map 或者元组来做 ； 先定义1个可变map 然后k v分别是匹配的2个值 缓存用
    val map = mutable.Map[Int, Int]()
    var flag = false

    //3 判断map是否有相应的值 然后进行替换

    //3-1 如果没 那么将第一个元素加进去-作为key 然后获取key对应的值 如果没的话 将第二个值放进去 然后通过if判断==target 相等就返回

    //3-2 不相等 那么遍历接下来的元素 分别和第一个相加 看看结果 2轮for循环

    //3-3 找出来之后 返回 map里面的值
    for (i <- array.indices) {
      for (j <- i + 1 to array.length - 1) {
        val map_value = map.getOrElse(i, array(i))
        if (map_value + array(j) == target) {
          // map.put(array(j),map_value)
          map.put(i, j)
        }
      }
    }

    if (!map.isEmpty) {
      flag = true
    }
    for ((i, j) <- map) {
      println(i, j)
    }
    flag
  }

  //2 两个数组a,b,判断a是否包含b,如果包含，返回a数组的开始位置 解释：A包含B---则B为A的子集或等于A
  // val a = Array(11,2,3,34,3)
  //val b = Array(34,3)

  // 1 如果包含的话 返回true
  def calCover(a: Array[Int], b: Array[Int]): Boolean = {
    var flag1 = false
    val arr = mutable.ArrayBuffer[Int]()
    //情况1 当 a的长度 小于 b的长度 则肯定为false
    if (a.size < b.size) {
      flag1
    }
    //情况2 当 a的长度大于或等于b的长度 则 通过2层循环或者循环守卫来判断
   // for (i <- a.indices) {
      for (j <- b.indices) {
        //判断中包含的元素 是否在里面 可以放到一个list数组 因为是顺序的
        if (a.contains(b(j))) {
          arr.append(b(j))
        }
      }
    println(arr.mkString(","))
  //  }

    //判断长度就可以
    if (arr.size == b.size) {
      flag1 = true
    }

    flag1

  }

  //2 判断flag 然后如果为true 直接打印a的第一个值


}
