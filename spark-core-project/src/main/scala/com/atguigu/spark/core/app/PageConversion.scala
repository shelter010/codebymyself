package com.atguigu.spark.core.app

import java.text.DecimalFormat

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * @author hpf
 * @create 2021/2/25 下午8:43  
 * @Version 1.0
 */
object PageConversion {
  def statPageConversionRate(sc: SparkContext, userVisitActionRdd: RDD[UserVisitAction], str: String) = {
    //先做跳转流
    val pages = str.split(",")
    val prePages = pages.take(pages.length - 1)
    val postPages = pages.takeRight(pages.length - 1)
    //拉链拉出来的结果是元组
    val targetPageFlows = prePages.zip(postPages).map(
      {
        case (pre, post) => s"$pre->$post"
      }
      //把targetPages 做广播变量
    )
    val broadS = sc.broadcast(targetPageFlows)
    //println(targetPageFlows.toList)
    //先计算分母 计算需要页面的点击量
    val pageAndCount = userVisitActionRdd.filter(action => prePages.contains(action.page_id.toString))
      .map(action => (action.page_id, 1))
      .countByKey()
   // println(pageAndCount)

    //计算分子
    //3.1 按照sessionId分组 不能先对需要的页面过滤 否则会影响跳转逻辑
    val sessionIdGrouped = userVisitActionRdd.groupBy(_.session_id)
    val pageFlowRdd = sessionIdGrouped.flatMap {
      case (sid, actionIt) =>
        val actions = actionIt.toList.sortBy(_.action_time)
        val preActions = actions.take(actions.length - 1)
        val postActions = actions.takeRight(actions.length - 1)
        preActions.zip(postActions).map {
          case (preAction, postAction) => s"${preAction.page_id}->${postAction.page_id}"
        }
          .filter(flow => broadS.value.contains(flow)) //使用广播变量优化性能

    }
  //  pageFlowRdd.collect.foreach(println)

    //聚合操作
    val pageFlowsAndCount = pageFlowRdd.map((_, 1)).countByKey()

    //计算跳转率
    val res = pageFlowsAndCount.map {
      case (flow, count) =>
        (flow, count.toDouble / pageAndCount(flow.split("->")(0).toLong))
    }
    val f = new DecimalFormat(".000%")
    val res1 = res.map({
      case (flow, baiFenBi) => (flow, f.format(baiFenBi))
    })
//这是优化部分 用 DecimalFormat类 来进行格式化
    println(res1)
  }




}
