package com.javbus.ods

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.javbus.utils.{DStreamUtil, KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

object DbApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("DbApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "TOPIC_DB"

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    // 从Redis中获取偏移量
    val offsets: Map[TopicPartition, Long] = RedisUtil.getOffset(topic, topic)
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic, offsets)
    } else {
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic)
    }

    // 提取本批次的偏移量
    var ranges: Array[OffsetRange] = null
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    //offsetDStream.map(record => JSON.parseObject(record.value())).print(100)

    val objDStream: DStream[JSONObject] = offsetDStream.transform(rdd => rdd.map(record => {
      val obj: JSONObject = JSON.parseObject(record.value())
      obj
    }))

    objDStream.foreachRDD { rdd => {
      // 每个批次开启一次redis 动态获取事务表和维度表的列表
      val jedis: Jedis = RedisUtil.getClient()
      val factTables: util.Set[String] = jedis.smembers("FACT:TABLE")
      val factTablesBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(factTables)
      jedis.close()

      rdd.foreachPartition {
        iter =>
          // 因为连接对象无法序列化 所以无法在Driver端开启 只能在Executor端开启
          // 一个分区开启一个redis客户端 将维度数据写入redis
          val dimJedis: Jedis = RedisUtil.getClient()
          iter.foreach {
            obj =>
              val opType: String = obj.getString("type")
              val op: String = opType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => "O"
              }

              if (!op.equals("O")) {
                val tableName: String = obj.getString("table")
                val data: JSONObject = obj.getJSONObject("data")
                // 事务表进入kafka
                if (factTablesBC.value.contains(tableName)) {
                  val topicName = s"DWD_${tableName.toUpperCase()}_$op"
                  KafkaUtil.sendData(topicName, JSON.toJSONString(data, new SerializeConfig(true)))

                  // 制造延迟 验证双流join
//                  if (tableName.equals("order_detail")) {
//                    Thread.sleep(1000)
//                  }
                } else {
                  // 维度表进入redis
                  val id: String = data.getString("id")
                  val redisKey: String = s"DIM_${tableName.toUpperCase()}_${id}"
                  dimJedis.set(redisKey, JSON.toJSONString(data, new SerializeConfig(true)))
                }
              }
          }
          // 每个rdd分区进行一次kafka缓存区数据刷写磁盘操作
          KafkaUtil.flush()
          //
          dimJedis.close()
      }

      // 每个批次提交一次偏移量
      RedisUtil.saveOffset(topic, topic, ranges)
    }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
