package com.javbus.ods

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.javbus.bean.{ActionLogBean, DisplayLogBean, PageLogBean, StartLogBean}
import com.javbus.utils.{DStreamUtil, KafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.lang

/*
精确一次消费的两种做法
1 将数据处理和偏移量提交整合成为一个事务 要么全部成功 要么全部失败
2 先数据处理 后提交偏移量 保证数据不会丢 只会造成重复消费 再利用下游的幂等性去除重复数据
 */

object LogApp {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("LogApp")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic = "ODS_BASE_LOG"

    // 从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = RedisUtil.getOffset(topic, topic)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 若redis中有偏移量 从偏移量的位置开始消费数据
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic, offsets)
    } else {
      // 若redis中没有偏移量 从kafka自动维护的位置开始消费数据
      kafkaDStream = DStreamUtil.getKafkaDStream(ssc, topic, topic)
    }

    // 提取当前批次数据的偏移量
    var ranges: Array[OffsetRange] = Array.empty[OffsetRange]
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(rdd => {
      /*
      transform和map的区别
      map是针对rdd中的每一个元素的
      transform是针对一串rdd中的每一个rdd的 针对的是一个批次的数据 是周期性执行的
       */
      ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //这句执行在dirver中
      rdd
    })

    //kafkaDStream.transform(rdd => rdd.map(record => record.value())).print(100)

    offsetDStream.foreachRDD { rdd => {
      // 为保证kafka的数据能真正的从缓存中刷写到磁盘中 使用foreachPartition算子 对每个分区的数据处理完成后 进行一次刷写
      rdd.foreachPartition { iter => {
        iter.foreach {
          record => {
            // 日志数据分流处理
            val baseObj: JSONObject = JSON.parseObject(record.value())

            // 获取时间
            val ts: Long = baseObj.getLong("ts")

            // 如果是错误日志 直接发送
            if (baseObj.containsKey("err")) {
              val errObj: JSONObject = baseObj.getJSONObject("err")
              KafkaUtil.sendData("DWD_ERROR_LOG", errObj.toJSONString)
            } else {

              // 获取公共字段
              val commonObj: JSONObject = baseObj.getJSONObject("common")
              val ar: String = commonObj.getString("ar")
              val ba: String = commonObj.getString("ba")
              val ch: String = commonObj.getString("ch")
              val is_new: String = commonObj.getString("is_new")
              val md: String = commonObj.getString("md")
              val mid: String = commonObj.getString("mid")
              val os: String = commonObj.getString("os")
              val uid: String = commonObj.getString("uid")
              val vc: String = commonObj.getString("vc")


              // 启动日志发送
              if (baseObj.containsKey("start")) {

                val startObj: JSONObject = baseObj.getJSONObject("start")
                val entry: String = startObj.getString("entry")
                val loading_time: Long = startObj.getLong("loading_time")
                val open_ad_id: String = startObj.getString("open_ad_id")
                val open_ad_ms: Long = startObj.getLong("open_ad_ms")
                val open_ad_skip_ms: Long = startObj.getLong("open_ad_skip_ms")

                val startLogBean: StartLogBean = StartLogBean(ar, ba, ch, is_new, md, mid, os, uid, vc,
                  entry, loading_time, open_ad_id, open_ad_ms, open_ad_skip_ms, ts)

                KafkaUtil.sendData("DWD_START_LOG", JSON.toJSONString(startLogBean, new SerializeConfig(true)))
              }

              // 页面日志发送
              if (baseObj.containsKey("page")) {
                val pageObj: JSONObject = baseObj.getJSONObject("page")
                val during_time: lang.Long = pageObj.getLong("during_time")
                val page_item: String = pageObj.getString("item")
                val page_item_type: String = pageObj.getString("item_type")
                val last_page_id: String = pageObj.getString("last_page_id")
                val page_id: String = pageObj.getString("page_id")
                val source_type: String = pageObj.getString("source_type")

                val pageLogBean: PageLogBean = PageLogBean(ar, ba, ch, is_new, md, mid, os, uid, vc,
                  during_time, page_item, page_item_type, last_page_id, page_id, source_type, ts)

                KafkaUtil.sendData("DWD_PAGE_LOG", JSON.toJSONString(pageLogBean, new SerializeConfig(true)))

                // 事件日志发送
                if (baseObj.containsKey("actions")) {
                  val actions: JSONArray = baseObj.getJSONArray("actions")
                  if (actions != null && actions.size() > 0) {
                    actions.forEach(action => {
                      val actionObj: JSONObject = action.asInstanceOf[JSONObject]
                      val action_id: String = actionObj.getString("action_id")
                      val action_item: String = actionObj.getString("item")
                      val action_item_type: String = actionObj.getString("item_type")
                      //println(s"action_id=$action_id, action_item=$action_item, action_item_type=$action_item_type")

                      val actionLogBean: ActionLogBean = ActionLogBean(ar, ba, ch, is_new, md, mid, os, uid, vc,
                        during_time, page_item, page_item_type, last_page_id, page_id, source_type,
                        action_id, action_item, action_item_type, ts)
                      KafkaUtil.sendData("DWD_PAGE_ACTION_LOG", JSON.toJSONString(actionLogBean, new SerializeConfig(true)))
                    })
                  }
                }

                // 曝光日志发送
                if (baseObj.containsKey("displays")) {
                  val displays: JSONArray = baseObj.getJSONArray("displays")
                  if (displays != null && displays.size() > 0) {
                    for (i <- 0 until displays.size()) {
                      val obj: JSONObject = displays.getJSONObject(i)
                      val display_type: String = obj.getString("display_type")
                      val display_item: String = obj.getString("item")
                      val display_item_type: String = obj.getString("item_type")
                      val display_order: String = obj.getString("order")
                      val display_pos_id: String = obj.getString("pos_id")
                      //                    printf("%s %s %s %s display_pos_id=%s \n",
                      //                      display_type, display_item, display_item_type, display_order, display_pos_id)

                      val displayLogBean: DisplayLogBean = DisplayLogBean(ar, ba, ch, is_new, md, mid, os, uid, vc,
                        during_time, page_item, page_item_type, last_page_id, page_id, source_type,
                        display_type, display_item, display_item_type, display_order, display_pos_id, ts)
                      KafkaUtil.sendData("DWD_PAGE_DISPLAY_LOG", JSON.toJSONString(displayLogBean, new SerializeConfig(true)))
                    }
                  }
                }
              }
            }
          }
        }

        // 当前分区数据处理完成 刷写一次数据到磁盘
        KafkaUtil.flush()
      }
      }
      // 当前批次数据处理完毕 提交偏移量
      RedisUtil.saveOffset(topic, topic, ranges)
    }

    }

    ssc.start()
    ssc.awaitTermination()
  }
}
