package com.atguigu.spark.core.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/2/24 下午10:16  
 * @Version 1.0
 */
object CategorySessionTop10 {
  def statSessionTop10(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRdd: RDD[UserVisitAction]): Unit = {
    //过滤出来只包含top10品类的那些点击记录
    val cidS = categoryTop10.map(_.categoryId.toLong)
    val filterUserVisitActionRdd = userVisitActionRdd.filter(action => cidS.contains(action.click_category_id))
    // filterUserVisitActionRdd.collect.foreach(println)

    //每个品类 top10 session 计算
    val cidSidANdOneRdd = filterUserVisitActionRdd.map(action => ((action.click_category_id, action.session_id), 1))
    //做聚合做操作  得到rdd[(cid,sid),count]
    val cidSidAndCountRdd = cidSidANdOneRdd.reduceByKey(_ + _)
    //在通过map 得到 rdd[cid,(sid,count)]
    val cidAndSidCountRdd = cidSidAndCountRdd.map({
      case ((cid, sid), count) => (cid, (sid, count))
    })
    //按照category分组 排序取top10
    val cidAndSidCountIt = cidAndSidCountRdd.groupByKey()

    //对每个值排序 取top10
    val res = cidAndSidCountIt.mapValues(it => {
      //it.toList.sortBy(-_._2).take(10)
      it.toList.sortBy(-_._2).take(10)
    })
   // res.collect.foreach(println)
    res

  }

  def statSessionTop10_1(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRdd: RDD[UserVisitAction]): Unit = {
    //过滤出来只包含top10品类的那些点击记录
    val cidS = categoryTop10.map(_.categoryId.toLong)
    val filterUserVisitActionRdd = userVisitActionRdd.filter(action => cidS.contains(action.click_category_id))
    // filterUserVisitActionRdd.collect.foreach(println)

    //每个品类 top10 session 计算
    val cidSidANdOneRdd = filterUserVisitActionRdd.map(action => ((action.click_category_id, action.session_id), 1))
    //做聚合做操作  得到rdd[(cid,sid),count]
    val cidSidAndCountRdd = cidSidANdOneRdd.reduceByKey(_ + _)
    //在通过map 得到 rdd[cid,(sid,count)]
    val cidAndSidCountRdd = cidSidAndCountRdd.map({
      case ((cid, sid), count) => (cid, (sid, count))
    })
        //分组 排序去top10
        val cidAndSidCountIt = cidAndSidCountRdd.groupByKey()
    val res = cidAndSidCountIt.mapValues(it => {
      //不要把Iterable 直接转换沉成list 可以找一个1能够排序的集合 每次只要哦最大的10个
      var set = mutable.TreeSet[SessionInfo]()
      it.foreach {
        case (sid, count) =>
          val info = SessionInfo(sid, count)
          set += info
          if (set.size > 10) set = set.take(10)
      }
      set.toList
    })


  }
  }

/*
计算session的口径 ： 看1每个session的点击记录
1 过滤出来只包括top10品类的那些点击记录
   上面的记录就是过滤出来点击记录了
2 每个品类top10Session的计算
=>rdd[(cid,sid),1] reduceByKey
=> rdd[(cid,sid),count] map
=》Rdd[cid,(sid,count)] groupByKey
Rdd[cid,Iterator[(SessionId,count),(sessionId,count)]] map内部 对iterator排序 取前10
  上面的排序有可能内存溢出
 */