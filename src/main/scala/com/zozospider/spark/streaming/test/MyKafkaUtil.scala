package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties

object MyKafkaUtil {

  // 消费
  def getDirectStream(topic: String, groupId: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    // Kafka 配置
    val map: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "vm017:9092,vm06:9092,vm03:9092",
      // 当前客户端的消费者组名称
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    // 读取 Kafka 数据创建 DStream
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc = ssc,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics = Set(topic), kafkaParams = map)
    )
    inputDStream
  }

  // 生产
  def getKafkaProducer: KafkaProducer[String, String] = {
    // Kafka 配置
    val properties: Properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "vm017:9092,vm06:9092,vm03:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    // KafkaProducer
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    kafkaProducer
  }

  // 生产
  def sendDataToKafka(kafkaProducer: KafkaProducer[String, String], topic: String, value: String): Unit = {
    val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("topic-kafka", value)
    kafkaProducer.send(record)
  }

}
