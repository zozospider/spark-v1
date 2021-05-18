package com.zozospider.spark.streaming.dstream.transform.nostate

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
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
    // transform() 使用场景:
    // 1. DStream 功能不完善时
    // 2. 需要代码周期性执行

    // code: Driver 端执行
    val dStream: DStream[(String, Int)] = inputDStream.transform(

      // code: Driver 端执行 (周期性执行, 一个采集周期执行一次)
      (rdd: RDD[String]) => {

        val rdd2: RDD[String] = rdd.flatMap(
          // code: Executor 端执行
          (s: String) => s.split(" ")
        )
        val rdd3: RDD[(String, Int)] = rdd2.map(
          // code: Executor 端执行
          (s: String) => (s, 1)
        )
        val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(
          // code: Executor 端执行
          (i1: Int, i2: Int) => i1 + i2
        )
        rdd4
      }
    )
    dStream.print()

    print("------")

    // code: Driver 端执行
    val dStream2: DStream[String] = inputDStream.flatMap(
      // code: Executor 端执行
      (s: String) => s.split(" ")
    )
    // code: Driver 端执行
    val dStream3: DStream[(String, Int)] = dStream2.map(
      // code: Executor 端执行
      (s: String) => (s, 1)
    )
    // code: Driver 端执行
    val dStream4: DStream[(String, Int)] = dStream3.reduceByKey(
      // code: Executor 端执行
      (i1: Int, i2: Int) => i1 + i2
    )
    dStream4.print()

    streamingContext.start
    streamingContext.awaitTermination
  }

}
