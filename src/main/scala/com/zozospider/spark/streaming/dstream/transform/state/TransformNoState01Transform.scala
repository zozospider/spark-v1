package com.zozospider.spark.streaming.dstream.transform.state

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

// DStream 转换 - 无状态转化操作 - transform()
object TransformNoState01Transform {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Streaming").setMaster("local[*]")
    val streamingContext: StreamingContext = new StreamingContext(conf = conf, batchDuration = Seconds(3))

    // Transform 允许 DStream 上执行任意的 RDD-to-RDD 函数
    // 即使这些函数并没有在 DStream 的 API 中暴露出来, 通过该函数可以方便的扩展 Spark API
    // 该函数每一批次调度一次, 其实也就是对 DStream 中的 RDD 应用转换

    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream(hostname = "localhost", port = 9999)

    // transform() 方法可以将底层 rdd 获取到后进行操作
    // 1. DStream 功能不完善
    // 2. 需要代码周期性执行

    // code: Driver 端执行
    inputDStream.transform((rdd: RDD[String]) => {
      // code: Driver 端执行 (周期性执行)
      rdd.map((s: String) => s)
    })

    // code: Driver 端执行
    inputDStream.map((s: String) => s)

    streamingContext.start
    streamingContext.awaitTermination
  }

}
