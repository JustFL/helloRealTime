package com.javbus.utils

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}

import java.util.Properties

object KafkaUtil {

  private val props: Properties = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropUtil.getPropWithStream(ConfigUtil.KAFKA_BOOTSTRAP_SERVER))
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  // 幂等性
  props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

  private val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

  def sendData(topic: String, msg: String) = {
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, msg)
    producer.send(record)
  }

  def flush(): Unit = {
    producer.flush()
  }
}
