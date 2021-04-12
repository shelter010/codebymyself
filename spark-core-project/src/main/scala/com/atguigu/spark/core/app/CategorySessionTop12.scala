package com.atguigu.spark.core.app

import com.atguigu.spark.core.project.bean.{CategoryCountInfo, UserVisitAction}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
 * @author hpf
 * @create 2021/2/25 下午7:49  
 * @Version 1.0
 *         目的是求解top10品类的 前10个点击类比较多的
 */
object CategorySessionTop12 {
  def statSessionTop12(sc: SparkContext, categoryTop10: Array[CategoryCountInfo], userVisitActionRdd: RDD[UserVisitAction]): Unit = {
    ///去掉 groupByKey 用自定义的分区器 让每个cid 进入一个分区 reduceByKey(分区器，_+_)
    val cidS = categoryTop10.map(_.categoryId.toLong)
    val filterUserVisitActionRdd = userVisitActionRdd.filter(userVisitActionRdd => cidS.contains(userVisitActionRdd.click_category_id))
    val cidSidAndOne = filterUserVisitActionRdd.map(action => ((action.click_category_id, action.session_id), 1))
    val cidSidAndCount = cidSidAndOne.reduceByKey(new CategorySessionPartitioner(cidS), _ + _)
    //执行 mapPartitions
    val result = cidSidAndCount.mapPartitions(it => {
      var set = mutable.TreeSet[SessionInfo]()
      var categoryId = -1L
      it.foreach {
        case ((cid, sid), count) =>
          categoryId = cid
          val info = SessionInfo(sid, count)
          set += info
          if (set.size > 10) set = set.take(10)
      }
      //set.map((categoryId,_)).toIterator
      Iterator((categoryId,set.toList))
    })
    result.foreach(println)

    //上面这种是最完美的方法
  }

}

class CategorySessionPartitioner(cidS:Array[Long]) extends Partitioner() {
  //定义的cidIndexMap 相当于一个缓存器具在后面的方法 getNumPartition中可以使用
  private val cidIndexMap: Map[Long, Int] = cidS.zipWithIndex.toMap
  //分区和品类id数量一致 可以保证一个分区只有一个cid
  override def numPartitions: Int = cidS.length

  override def getPartition(key: Any): Int = key match{
    case (cid:Long,_) =>cidIndexMap(cid)
  }
}