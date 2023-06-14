package com.javbus.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

object DStreamUtil {

  private val params: mutable.Map[String, String] = mutable.Map(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropUtil.getPropWithBundle(ConfigUtil.KAFKA_BOOTSTRAP_SERVER),
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true")

  def getKafkaDStream(ssc: StreamingContext, topic: String, groupID: String): InputDStream[ConsumerRecord[String, String]] = {
    params.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    val topics: Array[String] = Array(topic)

    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, params))
  }


  def getKafkaDStream(ssc: StreamingContext, topic: String, groupID: String, offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    params.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)
    val topics: Array[String] = Array(topic)

    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, params, offsets))
  }

}
