package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// 需求一: 广告黑名单
// 实现实时的动态黑名单机制: 将每天对某个广告点击超过 100 次的用户拉黑
// 注: 黑名单保存到 MySQL 中
object Test01A {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // 获取 Kafka 的 InputDStream
    val inputDStream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getDirectStream(topic = "topic-kafka", groupId = "consumer-group-kafka", ssc = streamingContext)
    // 将每条消息的 value 取出
    val dStream2: DStream[String] = inputDStream.map((kv: ConsumerRecord[String, String]) => kv.value())

    // 转换成 AdLog
    val dStream3: DStream[AdLog] = dStream2.map((s: String) => {
      val fields: Array[String] = s.split(" ")
      AdLog(fields(0).toLong, fields(1), fields(2), fields(3), fields(4))
    })



    streamingContext.start
    streamingContext.awaitTermination
  }

}
