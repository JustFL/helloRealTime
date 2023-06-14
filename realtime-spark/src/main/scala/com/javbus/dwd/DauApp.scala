package com.javbus.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.javbus.bean.{DauInfoBean, PageLogBean}
import com.javbus.utils.{BeanUtil, DStreamUtil, EsUtil, KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.lang
import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer


/*
统计日活 操作包括去重 关联维度
 */
object DauApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "DWD_PAGE_LOG"
    val offsets: Map[TopicPartition, Long] = RedisUtil.getOffset(topic, topic)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic, offsets)
    } else {
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic)
    }

    var ranges: Array[OffsetRange] = Array.empty
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    val objDStream: DStream[PageLogBean] = offsetDStream.map(record => {
      val pageLogObj: PageLogBean = JSON.parseObject(record.value(), classOf[PageLogBean])
      pageLogObj
    })

    // 凡是有last_page_id 说明不是本次会话的第一个页面 直接过滤掉
    val filterDStream: DStream[PageLogBean] = objDStream.filter(obj => obj.last_page_id == null)

    // 利用redis对不同会话的mid进行去重
    val redisFilterDStream: DStream[PageLogBean] = filterDStream.transform {
      rdd =>
        rdd.mapPartitions {
          iter => {
            val pages: ListBuffer[PageLogBean] = new ListBuffer[PageLogBean]
            val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
            // 每个分区开启一次redis连接
            val jedis: Jedis = RedisUtil.getClient()
            for (elem <- iter) {
              // 取得设备号
              val mid: String = elem.mid
              // 根据时间戳获取日期
              val date: String = df.format(new Date(elem.ts))
              val flag: lang.Long = jedis.sadd(s"DAU:$date", mid)
              // 返回1表示没有当前的mid 将当前数据加入List返回
              if (flag == 1L) {
                pages.append(elem)
              }
            }
            jedis.close()
            pages.toIterator
          }
        }
    }

    // 维度关联
    val finalDStream: DStream[DauInfoBean] = redisFilterDStream.mapPartitions {
      iter => {
        val jedis: Jedis = RedisUtil.getClient()
        val beans: ListBuffer[DauInfoBean] = new ListBuffer[DauInfoBean]
        val df: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH")
        for (obj <- iter) {

          val bean: DauInfoBean = new DauInfoBean()
          // copy属性到新对象中
          BeanUtil.copyProperties(obj, bean)

          // 获取性别 年龄
          val user_id: String = obj.user_id
          val userKey: String = s"DIM_USER_INFO_$user_id"
          val userObj: JSONObject = JSON.parseObject(jedis.get(userKey))
          val gender: String = userObj.getString("gender")
          val birthday: String = userObj.getString("birthday")
          val age: Int = Period.between(LocalDate.parse(birthday), LocalDate.now()).getYears
          bean.user_gender = gender
          bean.user_age = age.toString

          // 获取地区信息
          val province_id: String = obj.province_id
          val provinceKey = s"DIM_BASE_PROVINCE_$province_id"
          val provinceObj: JSONObject = JSON.parseObject(jedis.get(provinceKey))
          bean.province_name = provinceObj.getString("name")
          bean.province_iso_code = provinceObj.getString("iso_code")
          bean.province_3166_2 = provinceObj.getString("iso_3166_2")
          bean.province_area_code = provinceObj.getString("area_code")

          // 获取日期和小时
          val dateAndHour: String = df.format(new Date(bean.ts))
          val date: String = dateAndHour.split(" ")(0)
          val hour: String = dateAndHour.split(" ")(1)
          bean.dt = date
          bean.hr = hour

          beans.append(bean)
        }
        jedis.close()
        beans.toIterator
      }
    }

    //finalDStream.print(100)
    finalDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        // 一个分区的数据保存到ES
        val tuples: List[(String, DauInfoBean)] = iter.toList.map(dauinfo => (dauinfo.mid, dauinfo))
        if (tuples != null && tuples.length > 0) {
          val now: LocalDate = LocalDate.now()
          EsUtil.batch(s"mall_dau_$now", tuples)
        }
      })
      // 一个RDD提交一次偏移量
      RedisUtil.saveOffset(topic, topic, ranges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
