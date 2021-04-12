package com.atguigu.streaming.project.util

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

/**
 * @author hpf
 * @create 2021/3/2 下午10:01  
 * @Version 1.0
 */
object RedisUtil {
  private val conf = new JedisPoolConfig
  conf.setMaxTotal(100)
  conf.setMaxIdle(10)
  conf.setMinIdle(10)
  conf.setMaxWaitMillis(10000) //最大等待时间
  conf.setBlockWhenExhausted(true) //忙碌是否等待
  conf.setTestOnBorrow(true)
  conf.setTestOnReturn(true)
  val pool = new JedisPool(conf,"master",6379)

  def getClient=pool.getResource

}

/*
1 使用连接池创建客户端

2 直接创建客户端
 */