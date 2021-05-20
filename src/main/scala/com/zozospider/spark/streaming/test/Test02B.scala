package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

// 需求二: 实时统计每天各地区各城市各广告的点击总流量, 并将其存入 MySQL
object Test02B {

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
