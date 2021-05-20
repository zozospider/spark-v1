package com.zozospider.spark.streaming.test

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

// 需求三: 最近一小时广告点击量
// 结果展示:
// 1: List [15:50->10, 15:51->25, 15:52->30]
// 2: List [15:50->10, 15:51->25, 15:52->30]
// 3: List [15:50->10, 15:51->25, 15:52->30]
object Test03C {

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

    val handler: TestHandler = new TestHandler


    val dStream4: DStream[(AdHM, Long)] = handler.toAdHMCount(dStream3)

    val dStream5: DStream[(String, List[(String, Long)])] = handler.groupAd(dStream4)
    dStream5.print()

    streamingContext.start
    streamingContext.awaitTermination
  }

}
