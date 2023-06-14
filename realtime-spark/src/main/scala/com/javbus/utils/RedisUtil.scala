package com.javbus.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import java.util

object RedisUtil {

  var pool: JedisPool = null

  def getClient(): Jedis = {

    if (pool == null) {
      val conf: JedisPoolConfig = new JedisPoolConfig
      conf.setMaxTotal(100) //最大连接数
      conf.setMaxIdle(20) //最大空闲
      conf.setMinIdle(20) //最小空闲
      conf.setBlockWhenExhausted(true) //忙碌时是否等待
      conf.setMaxWaitMillis(500) //忙碌时等待时长 毫秒
      conf.setTestOnBorrow(true) //每次获得连接的进行测试

      pool = new JedisPool(conf, PropUtil.getPropWithBundle(ConfigUtil.REDIS_HOST), PropUtil.getPropWithStream(ConfigUtil.REDIS_PORT).toInt)
    }

    pool.getResource
  }

  def saveOffset(topic: String, groupID: String, ranges: Array[OffsetRange]): Unit = {
    if (ranges != null && ranges.size > 0) {
      // 使用hash结构存储偏移量数据
      val hkey: String = s"offsets:$topic:$groupID"
      val offsetsMap: util.HashMap[String, String] = new util.HashMap[String, String]()
      for (elem <- ranges) {
        offsetsMap.put(elem.partition.toString, elem.untilOffset.toString)
      }
      val jedis: Jedis = RedisUtil.getClient()
      jedis.hset(hkey, offsetsMap)
      jedis.close()
    }
  }

  def getOffset(topic: String, groupID: String): Map[TopicPartition, Long] = {
    val hkey: String = s"offsets:$topic:$groupID"
    val jedis: Jedis = RedisUtil.getClient()
    val offsetsMap: util.Map[String, String] = jedis.hgetAll(hkey)
    jedis.close()

    import scala.collection.JavaConverters._
    val partitionOffset: Map[TopicPartition, Long] = offsetsMap.asScala.map { case (partition, offset) => (new TopicPartition(topic, partition.toInt), offset.toLong) }.toMap
    partitionOffset
  }
}
