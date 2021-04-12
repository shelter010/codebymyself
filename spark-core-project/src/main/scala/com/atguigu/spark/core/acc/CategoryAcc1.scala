package com.atguigu.spark.core.acc

import com.atguigu.spark.core.project.bean.UserVisitAction
import org.apache.spark.util.AccumulatorV2

/**
 * @author hpf
 * @create 2021/2/24 下午8:19
 * @Version 1.0
 */
class CategoryAcc1() extends AccumulatorV2[UserVisitAction, Map[(String, String), Long]] {
  //作为一个输出的缓存器
  private var map = Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, Map[(String, String), Long]] = {
    //复制一个累加器
    val acc = new CategoryAcc1
    acc.map = map
    acc
  }

  override def reset(): Unit = map=Map[(String, String), Long]()

  //分区内进行计算
  override def add(v: UserVisitAction): Unit = {
    v match {
      //点击行为
      case action if action.click_category_id != -1 =>
        val key = (action.click_category_id.toString,"click")
        map += key -> (map.getOrElse(key,0L)+1)
      //下单行为
      case action if action.order_category_ids != "null" =>
        //切出来这次下单的多个品类
        val cidS = action.order_category_ids.split(",")
        //yong foreach 进行遍历
        cidS.foreach(cid => {
          val key =  (cid,"order")
          map += key -> (map.getOrElse(key,0L)+1L)
        })
      //支付行为
      case action if action.pay_category_ids != "null" =>
        val cidSS = action.pay_category_ids.split(",")
        cidSS.foreach(cid => {
          val key = (cid,"pay")
          map += key -> (map.getOrElse(key,0L)+1L)
        })

      case _ =>
      //  throw UnsupportedClassVersionError
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, Map[(String, String), Long]]): Unit = {
    this.map = other match {
      case o:CategoryAcc1 =>
        o.map.foreach({
          case ((cid,action),count) =>
            this.map += (cid,action) -> (this.map.getOrElse((cid,action),0L)+count)
        })
        map
    }
  }

  override def value: Map[(String, String), Long] = map
}
