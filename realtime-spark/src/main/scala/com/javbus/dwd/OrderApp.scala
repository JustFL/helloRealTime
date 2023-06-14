package com.javbus.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.javbus.bean.{OrderDetail, OrderInfo, OrderWide}
import com.javbus.utils.{DStreamUtil, EsUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("OrderApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 从redis中读取order_info的偏移量
    val infoTopic: String = "DWD_ORDER_INFO_I"
    val infoOffsets: Map[TopicPartition, Long] = RedisUtil.getOffset(infoTopic, infoTopic)

    // 根据偏移量得到dStream
    var infoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (infoOffsets != null && infoOffsets.nonEmpty) {
      infoKafkaDStream = DStreamUtil.getKafkaDStream(ssc, infoTopic, infoTopic, infoOffsets)
    } else {
      infoKafkaDStream = DStreamUtil.getKafkaDStream(ssc, infoTopic, infoTopic)
    }

    // 提取偏移量
    var infoRanges: Array[OffsetRange] = Array.empty
    val infoOffsetsDStream: DStream[ConsumerRecord[String, String]] = infoKafkaDStream.transform(rdd => {
      infoRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    // 补充字段
    val infoObjDStream: DStream[OrderInfo] = infoOffsetsDStream.mapPartitions { iter => {
      val infoList: ListBuffer[OrderInfo] = new ListBuffer[OrderInfo]
      for (record <- iter) {
        val info: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        infoList.append(info)
      }
      infoList.toIterator
    }
    }

    val infoWithDimDStream: DStream[OrderInfo] = infoObjDStream.mapPartitions { iter => {
      val jedis: Jedis = RedisUtil.getClient()
      val infolist: List[OrderInfo] = iter.toList
      for (obj <- infolist) {
        // 补充创建订单日期和小时
        val create_date: String = obj.create_time.split(" ")(0)
        val create_hour: String = obj.create_time.split(" ")(1).split(":")(0)
        obj.create_date = create_date
        obj.create_hour = create_hour

        // 补充地域维度
        val provinceKey: String = s"DIM_BASE_PROVINCE_${obj.province_id}"
        val provinceObj: JSONObject = JSON.parseObject(jedis.get(provinceKey))
        obj.province_name = provinceObj.getString("name")
        obj.province_area_code = provinceObj.getString("area_code")
        obj.province_3166_2_code = provinceObj.getString("iso_3166_2")
        obj.province_iso_code = provinceObj.getString("iso_code")

        // 补充用户维度
        val userKey: String = s"DIM_USER_INFO_${obj.user_id}"
        val userObj: JSONObject = JSON.parseObject(jedis.get(userKey))
        obj.user_gender = userObj.getString("gender")
        val birthday: LocalDate = LocalDate.parse(userObj.getString("birthday"))
        val now: LocalDate = LocalDate.now()
        obj.user_age = Period.between(birthday, now).getYears
      }
      jedis.close()
      infolist.iterator
    }
    }

    // 从redis中读取order_detail的偏移量
    val detailTopic = s"DWD_ORDER_DETAIL_I"
    val detailOffsets: Map[TopicPartition, Long] = RedisUtil.getOffset(detailTopic, detailTopic)

    // 根据偏移量得到dStream
    var detailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (detailOffsets != null && detailOffsets.nonEmpty) {
      detailKafkaDStream = DStreamUtil.getKafkaDStream(ssc, detailTopic, detailTopic, detailOffsets)
    } else {
      detailKafkaDStream = DStreamUtil.getKafkaDStream(ssc, detailTopic, detailTopic)
    }

    // 提取偏移量
    var detailRanges: Array[OffsetRange] = null
    val detailOffsetDStream: DStream[ConsumerRecord[String, String]] = detailKafkaDStream.transform(rdd => {
      detailRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val detailObjDStream: DStream[OrderDetail] = detailOffsetDStream.map(record => JSON.parseObject(record.value(), classOf[OrderDetail]))

    // 流转化格式
    val infoWithIdStream: DStream[(Long, OrderInfo)] = infoWithDimDStream.map(info => (info.id, info))
    val detailWithIdStream: DStream[(Long, OrderDetail)] = detailObjDStream.map(detail => (detail.order_id, detail))

    // 双流join
    val joinStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = infoWithIdStream.fullOuterJoin(detailWithIdStream)

    // 处理info和detail数据不在同一批次问题
    val finalStream: DStream[OrderWide] = joinStream.mapPartitions {
      iter => {
        val jedis: Jedis = RedisUtil.getClient()
        val wideList: ListBuffer[OrderWide] = new ListBuffer[OrderWide]
        for ((id, (infoOp, detailOp)) <- iter) {

          // info在
          if (infoOp.isDefined) {
            // info发送到redis 等待其他detail
            val infoKey: String = s"ORDER_JOIN:ORDER_INFO:${id}"
            jedis.set(infoKey, JSON.toJSONString(infoOp.get, new SerializeConfig(true)))
            // 设置过期时间
            jedis.expire(infoKey, 24 * 3600)

            // 查询redis是否有detail
            val details: util.Set[String] = jedis.smembers(s"ORDER_JOIN:ORDER_DETAIL:${id}")
            // redis中有detail 直接发送
            if (details != null && details.size() > 0) {
              import scala.collection.JavaConverters._
              val scalaDetails: mutable.Set[String] = details.asScala
              for (elem <- scalaDetails) {
                val detail: OrderDetail = JSON.parseObject(elem, classOf[OrderDetail])
                val wide: OrderWide = new OrderWide(infoOp.get, detail)
                wideList.append(wide)
              }
            }

            // detail在
            if (detailOp.isDefined) {
              // 直接发送
              val wide: OrderWide = new OrderWide(infoOp.get, detailOp.get)
              wideList.append(wide)
            }
          } else {
            // info不在 detail在
            // 查询redis中info是否存在
            val infoStr: String = jedis.get(s"ORDER_JOIN:ORDER_INFO:${id}")
            if (infoStr != null && infoStr.size > 0) {
              // redis中存在 直接发送
              val info: OrderInfo = JSON.parseObject(infoStr, classOf[OrderInfo])
              val wide: OrderWide = new OrderWide(info, detailOp.get)
              wideList.append(wide)
            } else {
              // redis中info不在 发送到redis等待info
              val detailKey: String = s"ORDER_JOIN:ORDER_DETAIL:${id}"
              jedis.sadd(detailKey, JSON.toJSONString(detailOp.get, new SerializeConfig(true)))
              jedis.expire(detailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        wideList.iterator
      }
    }

    //finalStream.print(100)

    // 写入es
    finalStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val tuples: List[(String, OrderWide)] = iter.toList.map(wide => (wide.detail_id.toString, wide))
        if (tuples != null && tuples.length > 0) {
          val now: LocalDate = LocalDate.now()
          EsUtil.batch(s"mall_order_$now", tuples)
        }
      })

      // 每个RDD提交一次偏移量
      RedisUtil.saveOffset(infoTopic, infoTopic, infoRanges)
      RedisUtil.saveOffset(detailTopic, detailTopic, detailRanges)
    })


    ssc.start()
    ssc.awaitTermination()

  }
}
