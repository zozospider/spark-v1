package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

// 生产数据
object SendDataToKafka {

  // Application => Kafka => SparkStreaming => Analysis
  def main(args: Array[String]): Unit = {

    // 获取 KafkaProducer
    val kafkaProducer: KafkaProducer[String, String] = MyKafkaUtil.getKafkaProducer

    // 不断发送字符串到 Kafka
    while (true) {

      // 每次获取 20 条随机生成的字符串: 某个时间点 某个地区 某个城市 某个用户 某个广告
      val lines: Array[String] = getData(10)

      // 遍历发送每一行
      lines.foreach((s: String) => {
        println(s)

        // 发送本行字符串记录到 Kafka
        MyKafkaUtil.sendDataToKafka(kafkaProducer = kafkaProducer, topic = "topic-kafka", value = s)
      })

      println("------")
      Thread.sleep(3000)
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
      val cityArea: String = city.cityArea
      val cityName: String = city.cityName
      val userId: Int = new Random().nextInt(6) + 1
      val adId: Int = new Random().nextInt(6) + 1

      arrayBuffer += timestamp + " " + cityArea + " " + cityName + " " + userId + " " + adId
    }

    arrayBuffer.toArray
  }

}
