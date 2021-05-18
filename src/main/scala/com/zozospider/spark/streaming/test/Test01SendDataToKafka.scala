package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

// 生产数据
object Test01SendDataToKafka {

  // Application => Kafka => SparkStreaming => Analysis
  def main(args: Array[String]): Unit = {

    // Kafka 配置
    val properties: Properties = new Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "vm017:9092,vm06:9092,vm03:9092")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // KafkaProducer
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)

    // 不断发送字符串到 Kafka
    while (true) {

      // 每次获取 20 条随机生成的字符串: 某个时间点 某个地区 某个城市 某个用户 某个广告
      val lines: Array[String] = getData(20)

      // 遍历发送每一行
      lines.foreach((s: String) => {
        println(s)

        // 发送本行字符串记录到 Kafka
        val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("topic-kafka", s)
        kafkaProducer.send(record)
      })

      println("------")
      Thread.sleep(2000)
    }
  }

  // 生成 n 条随机生成的字符串
  // 某个时间点 某个地区 某个城市 某个用户 某个广告
  def getData(n: Int): Array[String] = {

    val getter: RandomGetter[City] = RandomGetter(
      RandomValue(City(1, "北京", "华北"), 30),
      RandomValue(City(2, "上海", "华东"), 30),
      RandomValue(City(3, "广州", "华南"), 10),
      RandomValue(City(4, "深圳", "华南"), 20),
      RandomValue(City(5, "天津", "华北"), 10)
    )

    val arrayBuffer: ArrayBuffer[String] = ArrayBuffer[String]()

    for (_ <- 0 to n) {

      // 某个时间点 某个地区 某个城市 某个用户 某个广告
      val timestamp: Long = System.currentTimeMillis()
      val city: City = getter.randomValue
      // val cityId: Long = city.id
      val cityArea: String = city.area
      val cityName: String = city.name
      val userId: Int = new Random().nextInt(6) + 1
      val adId: Int = new Random().nextInt(6) + 1

      arrayBuffer += timestamp + " " + cityArea + " " + cityName + " " + userId + " " + adId
    }

    arrayBuffer.toArray
  }

}
