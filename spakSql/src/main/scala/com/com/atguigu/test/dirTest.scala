//package com.com.atguigu.test
//
//import java.io.File
//
///**
// * @author hpf
// * @create 2021/6/14 下午10:34
// * @Version 1.0
// */
//object dirTest {
//
//  def subdirs(dir: File):Iterator[File] = {
//    //采用递归的方式遍历目录
//    val children = dir.listFiles().filter(_.isDirectory())
//    children.toIterator ++ children.toIterator.flatMap(subdirs)
//
//  }
//  def subFiles(dir:File):Iterator[File] = {
//    val child = dir.listFiles().filter(_.isFile)
//    println(child.size)
//    for (f <- child){
//      println(f)
//    }
//    child.toIterator
//  }
//
//  //两数之和求下标
//  def twoSum(nums:Array[Int],target:Int):Array[Int] = {
//    val mapIndex = Map[Int,Int]()
//    for (i <- nums.indices){
//      if (mapIndex.contains(target-nums(i))){
//        return Array(mapIndex(target-nums(i)),i)
//      }
//      mapIndex += (nums(i) -> i)
//    }
//    Array(0,0)
//  }
//  def main(args: Array[String]): Unit = {
//    val path = new File("/Users/mac/Users/hpf/sparkStudy/spark1015/spakSql/input")
//    for (d <- subdirs(path))
//      println(d)
//  //调用打印文件的方法
//    subFiles(path)
//    //
//    val arr = Array(2, 7, 9, 342, 4353, 5)
//    twoSum(arr,9).foreach(println)
//  }
//
//
//}
