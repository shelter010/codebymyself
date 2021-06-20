//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//import scala.util.control.Breaks._
//
///**
// * @author hpf
// * @create 2021/6/9 下午7:04
// * @Version 1.0
// */
//object Test123 {
//  def main(args: Array[String]): Unit = {
//    val arr = Array[Int](231,343,111,2,34,7,453)
//    val a = twoSum(arr, 9)
////    println(a.mkString(","))
//    println(climbStairs(3))
//  }
//  def twoSum(nums:Array[Int],target:Int):Array[Int] = {
//    val res = new Array[Int](2)
//    breakable {
//      for (i <- 0 until nums.length) {
//        for (j <- i+1 until nums.length) {
//          if (nums(i)+nums(j)==target){
//            res(0) = i
//            res(1) = j
//            break()
//          }
//        }
//      }
//    }
//    res
//  }
//
//  def climbStairs(n:Int):Int={
//    n match {
//      case 0 | 1 | 2 => n
//      case _ => climbStairs(n-1)+climbStairs(n-2)
//    }
//  }
//
//  def invertTree(root:TreeNode):TreeNode = {
//    if (root==null) null
//    val left = invertTree(root.left)
//    root.left = invertTree(root.right)
//    root.right = left
//    root
//  }
//
//
//}
//class TreeNode{
//  var left:TreeNode = null
//  var right:TreeNode = null
//}
//
//def reverseList(head: ListNode): ListNode = {
//  if(head == null || head.next == null) return head
//  var pre:ListNode = null
//  var cur = head
//  while(cur!=null){
//    val next = cur.next
//    cur.next = pre
//    pre = cur
//    cur = next
//  }
//  pre
//}
//class ListNode{
//  var next:ListNode = null
//}