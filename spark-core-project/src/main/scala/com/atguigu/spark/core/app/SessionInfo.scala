package com.atguigu.spark.core.app

/**
 * @author hpf
 * @create 2021/2/25 上午8:12  
 * @Version 1.0
 */
//按照降序排序
case class SessionInfo(sessionId:String,
                       count:Long ) extends Ordered[SessionInfo]{
  override def compare(that: SessionInfo): Int = {
  if (this.count>= that.count) -1
    else 1
  }
  }

