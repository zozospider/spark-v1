package com.zozospider.spark.streaming.dstream.create

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 创建 - Kafka 数据源
object DStreamCreate03Kafka {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // Kafka 参数
    val map: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "vm017:9092,vm06:9092,vm03:9092",
      // 当前客户端的消费者组名称
      ConsumerConfig.GROUP_ID_CONFIG -> "consumer-group-kafka",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 读取 Kafka 数据创建 DStream
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      ssc = streamingContext,
      locationStrategy = LocationStrategies.PreferConsistent,
      consumerStrategy = ConsumerStrategies.Subscribe[String, String](topics = Set("topic-kafka"), kafkaParams = map)
    )

    // 将每条消息的 K V 取出
    val dStream2: DStream[String] = inputDStream.map((kv: ConsumerRecord[String, String]) => kv.value())

    // WordCount
    dStream2
      .flatMap((s: String) => s.split(" "))
      .map((s: String) => (s, 1))
      .reduceByKey((i1: Int, i2: Int) => i1 + i2)
      .print()

    streamingContext.start
    streamingContext.awaitTermination
  }

}
